//! Tools for working with JSON strings and Variants

use crate::variant_buffer_manager::VariantBufferManager;
use crate::variant_utils;
use indexmap::IndexMap;
use rust_decimal::prelude::*;
use serde_json::Value;
use std::error::Error;

const DEFAULT_SIZE_LIMIT: usize = 16 * 1024 * 1024;

struct VariantBuilder<'a, T: VariantBufferManager> {
    value_size: usize,
    metadata_size: usize,
    size_limit: usize,
    // We use index map to preserve the order of insertion since the order of insertion determines
    // key ID
    dictionary: IndexMap<String, usize>,
    variant_buffer_manager: &'a mut T,
}

struct FieldEntry<'a> {
    key: &'a str,
    id: usize,
    offset: usize,
}

impl<'a, T: VariantBufferManager> VariantBuilder<'a, T> {
    fn build(&mut self, json: &Value) -> Result<(), Box<dyn Error>> {
        self.build_value(json)?;
        self.build_metadata()?;
        Ok(())
    }

    fn build_value(&mut self, json: &Value) -> Result<(), Box<dyn Error>> {
        match json {
            Value::Null => self.append_null(),
            Value::Bool(b) => self.append_boolean(*b),
            Value::Number(n) => {
                // With the arbitrary_precision feature, numbers are internally stored as strings
                if n.is_i64() {
                    self.append_int(n.as_i64().unwrap())?;
                } else {
                    // Check if decimal
                    match Decimal::from_str_exact(n.as_str()) {
                        // TODO: Replace with custom decimal parsing to support decimal unscaled
                        // value greater than 2^96 - 1
                        Ok(dec) => self.append_decimal(dec)?,
                        Err(_) => {
                            // Try float
                            match n.as_f64() {
                                Some(f) => self.append_double(f),
                                None => {
                                    Err(format!("Failed to parse {} as number", n.as_str()).into())
                                }
                            }?
                        }
                    };
                }
                Ok(())
            }
            Value::String(s) => {
                self.append_string(s)?;
                Ok(())
            }
            Value::Array(arr) => {
                let start = self.value_size;
                let mut offsets = Vec::<usize>::new();
                for v in arr {
                    offsets.push(self.value_size - start);
                    self.build(v)?;
                }
                self.finish_writing_array(start, &mut offsets)?;
                Ok(())
            }
            Value::Object(mp) => {
                let mut fields = Vec::<FieldEntry>::new();
                let start = self.value_size;
                for (k, v) in mp.iter() {
                    let id = self.add_key(k);
                    fields.push(FieldEntry {
                        key: k,
                        id,
                        offset: self.value_size - start,
                    });
                    self.build(v)?;
                }
                self.finish_writing_object(start, &mut fields)?;
                Ok(())
            }
        }?;
        Ok(())
    }

    fn build_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        let num_keys = self.dictionary.len();
        // In rust, key.len() returns the number of bytes not characters
        let dictionary_string_size: usize =
            self.dictionary.keys().map(|key| key.len()).sum();
        let max_size = std::cmp::max(num_keys, dictionary_string_size);
        if max_size > self.size_limit {
            return Err("Variant metadata exceeds size limit".into());
        }
        let offset_size = Self::get_integer_size(max_size);
        let offset_start = 1 + offset_size;
        let string_start = offset_start + (num_keys + 1) * offset_size;
        let metadata_size = string_start + dictionary_string_size;
        if metadata_size > self.size_limit {
            return Err("Variant metadata exceeds size limit".into());
        }
        self.metadata_size = metadata_size;
        self.variant_buffer_manager
            .ensure_metadata_buffer_size(metadata_size)?;
        let metadata_buffer = self.variant_buffer_manager.borrow_metadata_buffer();
        let header_byte: u8 = variant_utils::VERSION | ((offset_size as u8 - 1) << 6);
        metadata_buffer[0] = header_byte;
        metadata_buffer[1..1 + offset_size]
            .copy_from_slice(&num_keys.to_le_bytes()[0..offset_size]);
        let mut offset_itr = offset_start;
        let mut string_itr = string_start;
        let mut current_offset: usize = 0;
        // In rust, key.len() returns the number of bytes not characters.
        for key in self.dictionary.keys() {
            let key_len = key.len();
            metadata_buffer[offset_itr..offset_itr + offset_size]
                .copy_from_slice(&current_offset.to_le_bytes()[..offset_size]);
            metadata_buffer[string_itr..string_itr + key_len].copy_from_slice(key.as_bytes());
            offset_itr += offset_size;
            current_offset += key_len;
            string_itr += key_len;
        }
        metadata_buffer[offset_itr..offset_itr + offset_size]
            .copy_from_slice(&current_offset.to_le_bytes()[..offset_size]);
        Ok(())
    }

    fn check_capacity(&mut self, additional: usize) -> Result<(), Box<dyn Error>> {
        let required = self.value_size + additional;
        if required > self.size_limit {
            // TODO: Formalize this error.
            return Err("Variant size limit exceeded.".into());
        }
        let cur_len = self.variant_buffer_manager.borrow_value_buffer().len();
        if required > cur_len {
            // Need to get new buffer
            let new_size = required.next_power_of_two();
            self.variant_buffer_manager
                .ensure_value_buffer_size(new_size)?;
        }
        Ok(())
    }

    fn append_null(&mut self) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1)?;
        self.write_primitive_header(variant_utils::NULL)?;
        Ok(())
    }

    fn append_boolean(&mut self, b: bool) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1)?;
        self.write_primitive_header(if b {
            variant_utils::TRUE
        } else {
            variant_utils::FALSE
        })?;
        Ok(())
    }

    fn append_int(&mut self, i: i64) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1 + variant_utils::U64_SIZE as usize)?;
        if i as i8 as i64 == i {
            self.write_primitive_header(variant_utils::INT1)?;
            self.write_value_bytes(&(i as i8).to_le_bytes())?;
        } else if i as i16 as i64 == i {
            self.write_primitive_header(variant_utils::INT2)?;
            self.write_value_bytes(&(i as i16).to_le_bytes())?;
        } else if i as i32 as i64 == i {
            self.write_primitive_header(variant_utils::INT4)?;
            self.write_value_bytes(&(i as i32).to_le_bytes())?;
        } else {
            self.write_primitive_header(variant_utils::INT8)?;
            self.write_value_bytes(&(i).to_le_bytes())?;
        }
        Ok(())
    }

    fn append_decimal(&mut self, dec: Decimal) -> Result<(), Box<dyn Error>> {
        self.check_capacity(2 + 16)?;
        let unscaled: i128 = dec.mantissa();
        let scale = dec.scale() as u8;
        if unscaled.abs() <= variant_utils::MAX_UNSCALED_DECIMAL_4 as i128
            && scale <= variant_utils::MAX_PRECISION_DECIMAL_4
        {
            self.write_primitive_header(variant_utils::DECIMAL4)?;
            self.write_value_bytes(&(scale).to_le_bytes())?;
            self.write_value_bytes(&(unscaled as i32).to_le_bytes())?;
        } else if unscaled.abs() <= variant_utils::MAX_UNSCALED_DECIMAL_8 as i128
            && scale <= variant_utils::MAX_PRECISION_DECIMAL_8
        {
            self.write_primitive_header(variant_utils::DECIMAL8)?;
            self.write_value_bytes(&(scale).to_le_bytes())?;
            self.write_value_bytes(&(unscaled as i64).to_le_bytes())?;
        } else {
            self.write_primitive_header(variant_utils::DECIMAL16)?;
            self.write_value_bytes(&(scale).to_le_bytes())?;
            self.write_value_bytes(&unscaled.to_le_bytes())?;
        }
        Ok(())
    }

    fn append_double(&mut self, f: f64) -> Result<(), Box<dyn Error>> {
        self.check_capacity(1 + 8)?;
        self.write_primitive_header(variant_utils::DOUBLE)?;
        self.write_value_bytes(&f.to_le_bytes())?;
        Ok(())
    }

    fn append_string(&mut self, s: &String) -> Result<(), Box<dyn Error>> {
        let bytes = s.as_bytes();
        let long_str = bytes.len() > variant_utils::MAX_SHORT_STR_SIZE.into();
        let additional = if long_str {
            1 + variant_utils::U32_SIZE as usize
        } else {
            1
        };
        self.check_capacity(additional + bytes.len())?;
        if long_str {
            self.write_primitive_header(variant_utils::LONG_STR)?;
            self.write_value_bytes(&(s.len() as u32).to_le_bytes())?;
        } else {
            self.write_short_string_header(bytes.len() as u8)?;
        }
        self.write_value_bytes(bytes)?;
        Ok(())
    }

    fn finish_writing_array(
        &mut self,
        start: usize,
        offsets: &mut Vec<usize>,
    ) -> Result<(), Box<dyn Error>> {
        let data_size = self.value_size - start;
        let num_offsets = offsets.len();
        let large_size = num_offsets > variant_utils::U8_MAX as usize;
        let size_bytes = if large_size {
            variant_utils::U32_SIZE as usize
        } else {
            variant_utils::U8_SIZE as usize
        };
        let offset_size = Self::get_integer_size(data_size);
        let header_size = 1 + size_bytes + (num_offsets + 1) * offset_size;
        self.check_capacity(header_size)?;
        self.shift_value_bytes(start + header_size, start, start + data_size)?;
        let offset_start = start + 1 + size_bytes;
        let value_buffer = self.variant_buffer_manager.borrow_value_buffer();
        value_buffer[start] = Self::array_header(large_size, offset_size as u8);
        value_buffer[start + 1..offset_start]
            .copy_from_slice(&num_offsets.to_le_bytes()[..size_bytes]);
        let mut offset_itr = offset_start;
        for offset in offsets {
            value_buffer[offset_itr..offset_itr + offset_size]
                .copy_from_slice(&offset.to_le_bytes()[..offset_size]);
            offset_itr += offset_size;
        }
        value_buffer[offset_itr..offset_itr + offset_size]
            .copy_from_slice(&data_size.to_le_bytes()[..offset_size]);
        Ok(())
    }

    fn add_key(&mut self, key: &str) -> usize {
        match self.dictionary.get(key) {
            Some(id) => *id,
            None => {
                let id = self.dictionary.len();
                self.dictionary.insert(key.to_string(), id);
                id
            }
        }
    }

    fn array_header(large_size: bool, offset_size: u8) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BITS + 2))
            | ((offset_size - 1) << variant_utils::BASIC_TYPE_BITS)
            | variant_utils::ARRAY
    }

    fn finish_writing_object(
        &mut self,
        start: usize,
        fields: &mut Vec<FieldEntry>,
    ) -> Result<(), Box<dyn Error>> {
        let num_fields = fields.len();
        fields.sort_by_key(|f: &FieldEntry<'_>| f.key);
        let mut max_id: usize = 0;
        for field in &*fields {
            if field.id > max_id {
                max_id = field.id;
            }
        }
        let data_size = self.value_size - start;
        let large_size = num_fields > variant_utils::U8_MAX as usize;
        let size_bytes: usize = if large_size {
            variant_utils::U32_SIZE as usize
        } else {
            variant_utils::U8_SIZE as usize
        };
        let id_size = Self::get_integer_size(max_id);
        let offset_size = Self::get_integer_size(data_size);
        let header_size = 1 + size_bytes + num_fields * id_size + (num_fields + 1) * offset_size;
        self.check_capacity(header_size)?;
        self.shift_value_bytes(start + header_size, start, start + data_size)?;
        let value_buffer = self.variant_buffer_manager.borrow_value_buffer();
        value_buffer[start] = Self::object_header(large_size, id_size as u8, offset_size as u8);
        let id_start = start + 1 + size_bytes;
        let offset_start = id_start + num_fields * id_size;
        if large_size {
            value_buffer[start + 1..id_start].copy_from_slice(&(num_fields as u32).to_le_bytes());
        } else {
            value_buffer[start + 1..id_start].copy_from_slice(&(num_fields as u8).to_le_bytes());
        }
        self.write_field_ids_and_offsets(
            id_start,
            id_size,
            offset_start,
            offset_size,
            data_size,
            fields.as_slice(),
        );
        Ok(())
    }

    fn object_header(large_size: bool, id_size: u8, offset_size: u8) -> u8 {
        ((large_size as u8) << (variant_utils::BASIC_TYPE_BITS + 4))
            | ((id_size - 1) << (variant_utils::BASIC_TYPE_BITS + 2))
            | ((offset_size - 1) << variant_utils::BASIC_TYPE_BITS)
            | variant_utils::OBJECT
    }

    fn get_integer_size(value: usize) -> usize {
        if value <= variant_utils::U8_MAX as usize {
            return variant_utils::U8_SIZE as usize;
        }
        if value <= variant_utils::U16_MAX as usize {
            return variant_utils::U16_SIZE as usize;
        }
        variant_utils::U24_SIZE as usize
    }

    fn write_field_ids_and_offsets(
        &mut self,
        id_start: usize,
        id_size: usize,
        offset_start: usize,
        offset_size: usize,
        data_size: usize,
        fields: &[FieldEntry],
    ) {
        let mut id_itr = id_start;
        let mut offset_itr = offset_start;
        let value_buffer = self.variant_buffer_manager.borrow_value_buffer();
        for field in fields {
            value_buffer[id_itr..id_itr + id_size]
                .copy_from_slice(&(field.id).to_le_bytes()[..id_size]);
            value_buffer[offset_itr..offset_itr + offset_size]
                .copy_from_slice(&(field.offset).to_le_bytes()[..offset_size]);
            id_itr += id_size;
            offset_itr += offset_size;
        }
        value_buffer[offset_itr..offset_itr + id_size]
            .copy_from_slice(&(data_size).to_le_bytes()[..offset_size]);
    }

    fn write_primitive_header(&mut self, typ: u8) -> Result<(), Box<dyn Error>> {
        self.write_value_bytes(&[(typ << 2) | variant_utils::PRIMITIVE])?;
        Ok(())
    }

    fn write_short_string_header(&mut self, size: u8) -> Result<(), Box<dyn Error>> {
        self.write_value_bytes(&[(size << 2) | variant_utils::SHORT_STR])?;
        Ok(())
    }

    fn write_value_bytes(&mut self, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let value_buffer = self.variant_buffer_manager.borrow_value_buffer();
        if self.value_size + bytes.len() > value_buffer.len() {
            // Formalize this error
            return Err(
                "Buffer size insufficient. There might be a bug in the memory allocator.".into(),
            );
        }
        value_buffer[self.value_size..self.value_size + bytes.len()].copy_from_slice(bytes);
        self.value_size += bytes.len();
        Ok(())
    }

    fn shift_value_bytes(
        &mut self,
        new_start: usize,
        start: usize,
        end: usize,
    ) -> Result<(), Box<dyn Error>> {
        let additional = new_start - start;
        let borrowed_value = self.variant_buffer_manager.borrow_value_buffer();
        if self.value_size + additional > borrowed_value.len() {
            return Err("Buffer size limit exceeded".into());
        }
        borrowed_value.copy_within(start..end, new_start);
        self.value_size += additional;
        Ok(())
    }
}

/// Constructs a variant representation from a json string `json` (assumed to be valid utf-8) and
/// writes the "value" and "metadata" fields of the variant into value and metadata buffers provided
/// by `variant_buffer_manager`
pub fn json_to_variant<T: VariantBufferManager>(
    json: &str,
    variant_buffer_manager: &mut T,
    value_size: &mut usize,
    metadata_size: &mut usize,
) -> Result<(), Box<dyn Error>> {
    let json: Value = serde_json::from_str(json)?;

    let mut vb = VariantBuilder {
        value_size: 0,
        metadata_size: 0,
        dictionary: IndexMap::new(),
        size_limit: DEFAULT_SIZE_LIMIT,
        variant_buffer_manager,
    };
    vb.build(&json)?;
    *value_size = vb.value_size;
    *metadata_size = vb.metadata_size;
    Ok(())
}
