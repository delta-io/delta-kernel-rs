use std::collections::{HashMap, HashSet};

use crate::schema::{DataType, Schema, StructField, StructType};

fn is_schema_compatible(existing: &Schema, new: &Schema) -> bool {
    for field in later.fields() {}
    todo!()
}
fn is_nullability_compatible(existing_nullable: bool, new_nullable: bool) -> bool {
    // The case to avoid is when the new is non-nullable, but the existing one is.
    // Hence !(!new_nullable && existing_nullable)
    //  == new_nullable || !existing_nullable
    new_nullable || !existing_nullable
}
fn is_struct_read_compatible(existing: &StructType, newtype: &StructType) -> bool {
    // Delta tables do not allow fields that differ in name only by case
    let existing_fields: HashMap<&String, &StructField> = existing
        .fields()
        .map(|field| (field.name(), field))
        .collect();

    let existing_names: HashSet<String> = existing
        .fields()
        .map(|field| field.name().clone())
        .collect();
    let new_names: HashSet<String> = newtype.fields().map(|field| field.name().clone()).collect();
    if new_names.is_subset(&existing_names) {
        return false;
    }
    newtype.fields().all(|new_field| {
        existing_fields
            .get(new_field.name())
            .into_iter()
            .all(|existing_field| {
                existing_field.name() == new_field.name()
                    && is_nullability_compatible(existing_field.nullable, new_field.nullable)
                    && is_datatype_read_compatible(
                        existing_field.data_type(),
                        new_field.data_type(),
                    )
            })
    });
    //
    //    new_fields{ newField =>
    //      // new fields are fine, they just won't be returned
    //      existingFields.get(newField.name).forall { existingField =>
    //        // we know the name matches modulo case - now verify exact match
    //        (existingField.name == newField.name
    //          // if existing value is non-nullable, so should be the new value
    //          && isNullabilityCompatible(existingField.nullable, newField.nullable)
    //          // and the type of the field must be compatible, too
    //          && isDatatypeReadCompatible(existingField.dataType, newField.dataType))
    //      }
    //    }
    false
}
fn is_datatype_read_compatible(existing: &DataType, newtype: &DataType) -> bool {
    match (existing, newtype) {
        // TODO: Add support for type widening
        (DataType::Array(a), DataType::Array(b)) => {
            is_datatype_read_compatible(a.element_type(), b.element_type())
                && is_nullability_compatible(a.contains_null(), b.contains_null())
        }
        (DataType::Struct(a), DataType::Struct(b)) => is_struct_read_compatible(a, b),
        (DataType::Map(a), DataType::Map(b)) => {
            is_nullability_compatible(a.value_contains_null(), b.value_contains_null())
                && is_datatype_read_compatible(a.key_type(), b.key_type())
                && is_datatype_read_compatible(a.value_type(), b.value_type())
        }
        (a, b) => a == b,
    }
}

//def isReadCompatible(
//    existingSchema: StructType,
//    readSchema: StructType,
//    forbidTightenNullability: Boolean = false,
//    allowMissingColumns: Boolean = false,
//    allowTypeWidening: Boolean = false,
//    newPartitionColumns: Seq[String] = Seq.empty,
//    oldPartitionColumns: Seq[String] = Seq.empty): Boolean = {
//
//  def isNullabilityCompatible(existingNullable: Boolean, readNullable: Boolean): Boolean = {
//    if (forbidTightenNullability) {
//      readNullable || !existingNullable
//    } else {
//      existingNullable || !readNullable
//    }
//  }
//
//  def isDatatypeReadCompatible(existing: DataType, newtype: DataType): Boolean = {
//    (existing, newtype) match {
//      case (e: StructType, n: StructType) =>
//        isReadCompatible(e, n, forbidTightenNullability, allowTypeWidening = allowTypeWidening)
//      case (e: ArrayType, n: ArrayType) =>
//        // if existing elements are non-nullable, so should be the new element
//        isNullabilityCompatible(e.containsNull, n.containsNull) &&
//          isDatatypeReadCompatible(e.elementType, n.elementType)
//      case (e: MapType, n: MapType) =>
//        // if existing value is non-nullable, so should be the new value
//        isNullabilityCompatible(e.valueContainsNull, n.valueContainsNull) &&
//          isDatatypeReadCompatible(e.keyType, n.keyType) &&
//          isDatatypeReadCompatible(e.valueType, n.valueType)
//      case (e: AtomicType, n: AtomicType) if allowTypeWidening =>
//        TypeWidening.isTypeChangeSupportedForSchemaEvolution(e, n)
//      case (a, b) => a == b
//    }
//  }
//
//  def isStructReadCompatible(existing: StructType, newtype: StructType): Boolean = {
//    val existingFields = toFieldMap(existing)
//    // scalastyle:off caselocale
//    val existingFieldNames = existing.fieldNames.map(_.toLowerCase).toSet
//    assert(existingFieldNames.size == existing.length,
//      "Delta tables don't allow field names that only differ by case")
//    val newFields = newtype.fieldNames.map(_.toLowerCase).toSet
//    assert(newFields.size == newtype.length,
//      "Delta tables don't allow field names that only differ by case")
//    // scalastyle:on caselocale
//
//    if (!allowMissingColumns &&
//      !(existingFieldNames.subsetOf(newFields) &&
//        isPartitionCompatible(newPartitionColumns, oldPartitionColumns))) {
//      // Dropped a column that was present in the DataFrame schema
//      return false
//    }
//    newtype.forall { newField =>
//      // new fields are fine, they just won't be returned
//      existingFields.get(newField.name).forall { existingField =>
//        // we know the name matches modulo case - now verify exact match
//        (existingField.name == newField.name
//          // if existing value is non-nullable, so should be the new value
//          && isNullabilityCompatible(existingField.nullable, newField.nullable)
//          // and the type of the field must be compatible, too
//          && isDatatypeReadCompatible(existingField.dataType, newField.dataType))
//      }
//    }
//  }
//
//  isStructReadCompatible(existingSchema, readSchema)
//}
//
#[cfg(test)]
mod tests {}
