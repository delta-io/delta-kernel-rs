use crate::arrow::array::{
    types::{GenericStringType, Int32Type, Int64Type},
    Array, BooleanArray, GenericByteArray, GenericListArray, MapArray, OffsetSizeTrait,
    PrimitiveArray,
};

use delta_kernel::engine_data::{GetData, ListItem, MapItem};
use delta_kernel::DeltaResult;

// actual impls (todo: could macro these)

pub(crate) struct ArrowBooleanArray<'a>(pub(crate) &'a BooleanArray);
impl<'a> GetData<'a> for ArrowBooleanArray<'a> {
    fn get_bool(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<bool>> {
        if self.0.is_valid(row_index) {
            Ok(Some(self.0.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct ArrowPrimitiveArrayInt32<'a>(pub(crate) &'a PrimitiveArray<Int32Type>);
impl<'a> GetData<'a> for ArrowPrimitiveArrayInt32<'a> {
    fn get_int(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i32>> {
        if self.0.is_valid(row_index) {
            Ok(Some(self.0.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct ArrowPrimitiveArrayInt64<'a>(pub(crate) &'a PrimitiveArray<Int64Type>);
impl<'a> GetData<'a> for ArrowPrimitiveArrayInt64<'a> {
    fn get_long(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i64>> {
        if self.0.is_valid(row_index) {
            Ok(Some(self.0.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct ArrowStringArray<'a>(pub(crate) &'a GenericByteArray<GenericStringType<i32>>);
impl<'a> GetData<'a> for ArrowStringArray<'a> {
    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        if self.0.is_valid(row_index) {
            Ok(Some(self.0.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct ArrowListArray<'a, OffsetSize: OffsetSizeTrait>(
    pub(crate) &'a GenericListArray<OffsetSize>,
);
impl<'a, OffsetSize> GetData<'a> for ArrowListArray<'a, OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn get_list(
        &'a self,
        row_index: usize,
        _field_name: &str,
    ) -> DeltaResult<Option<ListItem<'a>>> {
        if self.0.is_valid(row_index) {
            Ok(Some(ListItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct ArrowMapArray<'a>(pub(crate) &'a MapArray);
impl<'a> GetData<'a> for ArrowMapArray<'a> {
    fn get_map(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<MapItem<'a>>> {
        if self.0.is_valid(row_index) {
            Ok(Some(MapItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}
