//! This module implements [`GetDataItem`] for the various arrow types we support

use arrow_array::{
    types::{GenericStringType, Int32Type, Int64Type},
    Array, ArrayRef, BooleanArray, GenericByteArray, GenericListArray, MapArray, PrimitiveArray,
};

use crate::engine_data::{DataItem, GetDataItem, ListItem, MapItem};

impl<'a> GetDataItem<'a> for BooleanArray {
    fn get(&self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            Some(DataItem::Bool(self.value(row_index)))
        } else {
            None
        }
    }
}

impl<'a> GetDataItem<'a> for GenericByteArray<GenericStringType<i32>> {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            Some(DataItem::Str(self.value(row_index)))
        } else {
            None
        }
    }
}

impl<'a> GetDataItem<'a> for PrimitiveArray<Int64Type> {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            Some(DataItem::I64(self.value(row_index)))
        } else {
            None
        }
    }
}

impl<'a> GetDataItem<'a> for PrimitiveArray<Int32Type> {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            Some(DataItem::I32(self.value(row_index)))
        } else {
            None
        }
    }
}

impl<'a> GetDataItem<'a> for GenericListArray<i32> {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            let list_item = ListItem::new(self, row_index);
            Some(DataItem::List(list_item))
        } else {
            None
        }
    }
}

impl<'a> GetDataItem<'a> for MapArray {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            let map_item = MapItem::new(self, row_index);
            Some(DataItem::Map(map_item))
        } else {
            None
        }
    }
}

// Used to represent a column of all-null values
impl<'a> GetDataItem<'a> for () {
    fn get(&self, _row_index: usize) -> Option<DataItem<'a>> {
        None
    }
}
