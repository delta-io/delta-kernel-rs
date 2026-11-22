use delta_kernel::schema::{DataType, StructType};

use crate::containers::write_json;
use crate::{LimitedBytes, LimitedString, LimitedVec, MemoryBudget, MemoryError, MemoryResult};

const MAX_DEPTH: usize = 128;

/// An owned schema copy with fallible nested storage, projection, and JSON serialization.
///
/// Source schemas are already validated Kernel schemas. Every copied field name, metadata JSON
/// payload, child list, and node allocation participates in the supplied budget. A flat node arena
/// avoids allocator-aware `Box` APIs and makes dropping the tree iterative. Parsing the original
/// Kernel schema is outside this experiment's allocation boundary.
#[derive(Debug)]
pub struct LimitedSchema {
    nodes: LimitedVec<Node>,
    root: usize,
}

#[derive(Debug)]
enum Node {
    Struct(LimitedVec<Field>),
    Array {
        element: usize,
        contains_null: bool,
    },
    Map {
        key: usize,
        value: usize,
        value_contains_null: bool,
    },
    // Primitive and annotated types preserve Kernel's canonical wire representation.
    Serialized(LimitedBytes),
}

#[derive(Debug)]
struct Field {
    name: LimitedString,
    data_type: usize,
    nullable: bool,
    metadata: LimitedBytes,
}

impl LimitedSchema {
    /// Copy a validated schema, returning an error and freeing partial state on exhaustion.
    pub fn try_from_kernel(budget: &MemoryBudget, schema: &StructType) -> MemoryResult<Self> {
        let mut result = Self {
            nodes: LimitedVec::new(budget),
            root: 0,
        };
        result.root = result.copy_struct(schema, 0)?;
        Ok(result)
    }

    /// Return an independently owned top-level projection in the requested order.
    ///
    /// Source and destination coexist under the same budget. Invalid or repeated indices return
    /// `InvalidProjection`. Failure preserves the source schema and releases partial output.
    pub fn try_project(&self, indices: &[usize]) -> MemoryResult<Self> {
        let fields = self.fields();
        let mut seen = LimitedVec::new(self.nodes.budget());
        seen.try_reserve_exact(indices.len())?;
        for index in indices {
            if *index >= fields.len() {
                return Err(MemoryError::InvalidProjection);
            }
            seen.try_push(*index)?;
        }
        seen.as_mut_slice().sort_unstable();
        if seen.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(MemoryError::InvalidProjection);
        }
        drop(seen);

        let mut result = Self {
            nodes: LimitedVec::new(self.nodes.budget()),
            root: 0,
        };
        let mut selected = LimitedVec::new(self.nodes.budget());
        selected.try_reserve_exact(indices.len())?;
        for index in indices {
            selected.try_push(result.copy_field(self, &fields[*index])?)?;
        }
        result.root = result.push_node(Node::Struct(selected))?;
        Ok(result)
    }

    /// Return the number of top-level fields.
    pub fn field_count(&self) -> usize {
        self.fields().len()
    }

    /// Return a field name in schema order, or `None` if the index is out of bounds.
    pub fn field_name(&self, index: usize) -> Option<&str> {
        self.fields().get(index).map(|field| field.name.as_str())
    }

    /// Encode this schema into a separately charged buffer while retaining the schema itself.
    pub fn try_to_json(&self) -> MemoryResult<LimitedBytes> {
        let mut bytes = LimitedBytes::new(self.nodes.budget());
        self.write_node(self.root, &mut bytes)?;
        Ok(bytes)
    }

    fn fields(&self) -> &[Field] {
        match &self.nodes[self.root] {
            Node::Struct(fields) => fields,
            _ => &[],
        }
    }

    fn push_node(&mut self, node: Node) -> MemoryResult<usize> {
        let index = self.nodes.len();
        self.nodes.try_push(node)?;
        Ok(index)
    }

    fn copy_struct(&mut self, schema: &StructType, depth: usize) -> MemoryResult<usize> {
        if depth >= MAX_DEPTH {
            return Err(MemoryError::SchemaTooDeep);
        }
        let budget = self.nodes.budget().clone();
        let mut fields = LimitedVec::new(&budget);
        fields.try_reserve_exact(schema.fields().len())?;
        for field in schema.fields() {
            let data_type = self.copy_type(&field.data_type, depth + 1)?;
            fields.try_push(Field {
                name: LimitedString::try_copy(&budget, &field.name)?,
                data_type,
                nullable: field.nullable,
                metadata: crate::try_to_json(&budget, &field.metadata)?,
            })?;
        }
        self.push_node(Node::Struct(fields))
    }

    fn copy_type(&mut self, data_type: &DataType, depth: usize) -> MemoryResult<usize> {
        if depth >= MAX_DEPTH {
            return Err(MemoryError::SchemaTooDeep);
        }
        let node = match data_type {
            DataType::Struct(schema) => return self.copy_struct(schema, depth),
            DataType::Array(array) => Node::Array {
                element: self.copy_type(array.element_type(), depth + 1)?,
                contains_null: array.contains_null(),
            },
            DataType::Map(map) => Node::Map {
                key: self.copy_type(map.key_type(), depth + 1)?,
                value: self.copy_type(map.value_type(), depth + 1)?,
                value_contains_null: map.value_contains_null(),
            },
            other => Node::Serialized(crate::try_to_json(self.nodes.budget(), other)?),
        };
        self.push_node(node)
    }

    fn copy_field(&mut self, source: &Self, field: &Field) -> MemoryResult<Field> {
        let data_type = self.copy_node(source, field.data_type)?;
        Ok(Field {
            name: LimitedString::try_copy(self.nodes.budget(), field.name.as_str())?,
            data_type,
            nullable: field.nullable,
            metadata: LimitedBytes::try_copy(self.nodes.budget(), &field.metadata)?,
        })
    }

    fn copy_node(&mut self, source: &Self, index: usize) -> MemoryResult<usize> {
        let node = match &source.nodes[index] {
            Node::Struct(fields) => {
                let mut copied = LimitedVec::new(self.nodes.budget());
                copied.try_reserve_exact(fields.len())?;
                for field in fields.iter() {
                    copied.try_push(self.copy_field(source, field)?)?;
                }
                Node::Struct(copied)
            }
            Node::Array {
                element,
                contains_null,
            } => Node::Array {
                element: self.copy_node(source, *element)?,
                contains_null: *contains_null,
            },
            Node::Map {
                key,
                value,
                value_contains_null,
            } => Node::Map {
                key: self.copy_node(source, *key)?,
                value: self.copy_node(source, *value)?,
                value_contains_null: *value_contains_null,
            },
            Node::Serialized(bytes) => {
                Node::Serialized(LimitedBytes::try_copy(self.nodes.budget(), bytes)?)
            }
        };
        self.push_node(node)
    }

    fn write_node(&self, index: usize, bytes: &mut LimitedBytes) -> MemoryResult<()> {
        match &self.nodes[index] {
            Node::Struct(fields) => {
                bytes.try_extend_from_slice(br#"{"type":"struct","fields":["#)?;
                for (position, field) in fields.iter().enumerate() {
                    if position != 0 {
                        bytes.try_push(b',')?;
                    }
                    bytes.try_extend_from_slice(br#"{"name":"#)?;
                    write_json(bytes, field.name.as_str())?;
                    bytes.try_extend_from_slice(br#", "type":"#)?;
                    self.write_node(field.data_type, bytes)?;
                    bytes.try_extend_from_slice(br#", "nullable":"#)?;
                    write_json(bytes, &field.nullable)?;
                    bytes.try_extend_from_slice(br#", "metadata":"#)?;
                    bytes.try_extend_from_slice(&field.metadata)?;
                    bytes.try_push(b'}')?;
                }
                bytes.try_extend_from_slice(b"]}")
            }
            Node::Array {
                element,
                contains_null,
            } => {
                bytes.try_extend_from_slice(br#"{"type":"array","elementType":"#)?;
                self.write_node(*element, bytes)?;
                bytes.try_extend_from_slice(br#", "containsNull":"#)?;
                write_json(bytes, contains_null)?;
                bytes.try_push(b'}')
            }
            Node::Map {
                key,
                value,
                value_contains_null,
            } => {
                bytes.try_extend_from_slice(br#"{"type":"map","keyType":"#)?;
                self.write_node(*key, bytes)?;
                bytes.try_extend_from_slice(br#", "valueType":"#)?;
                self.write_node(*value, bytes)?;
                bytes.try_extend_from_slice(br#", "valueContainsNull":"#)?;
                write_json(bytes, value_contains_null)?;
                bytes.try_push(b'}')
            }
            Node::Serialized(serialized) => bytes.try_extend_from_slice(serialized),
        }
    }
}
