use crate::arrow::array::{
    types::{GenericBinaryType, GenericStringType, Int32Type, Int64Type},
    Array, BooleanArray, GenericByteArray, GenericListArray, MapArray, OffsetSizeTrait,
    PrimitiveArray,
};

use crate::{
    engine_data::{GetData, ListItem, MapItem},
    DeltaResult,
};

// actual impls (todo: could macro these)

impl GetData<'_> for BooleanArray {
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_bool(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<bool>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl GetData<'_> for PrimitiveArray<Int32Type> {
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_int(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i32>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl GetData<'_> for PrimitiveArray<Int64Type> {
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_long(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i64>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericStringType<i32>> {
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericBinaryType<i32>> {
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_binary(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a, OffsetSize> GetData<'a> for GenericListArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_list(
        &'a self,
        row_index: usize,
        _field_name: &str,
    ) -> DeltaResult<Option<ListItem<'a>>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(ListItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for MapArray {
    fn is_valid(&self, row_index: usize) -> bool {
        Array::is_valid(self, row_index)
    }

    fn get_map(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<MapItem<'a>>> {
        if Array::is_valid(self, row_index) {
            Ok(Some(MapItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}
