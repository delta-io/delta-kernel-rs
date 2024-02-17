//! This module implements [`GetDataItem`] for the various arrow types we support

use arrow_array::{
    types::{GenericStringType, Int32Type, Int64Type},
    Array, BooleanArray, GenericByteArray, GenericListArray, MapArray, PrimitiveArray,
};

use crate::engine_data::{DataItem, GetDataItem, ListItem, MapItem};

macro_rules! impl_get_data_item {
    (($typ: ty, $enum_variant: ident)) => {
        impl<'a> GetDataItem<'a> for $typ {
            fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
                if self.is_valid(row_index) {
                    Some(DataItem::$enum_variant(self.value(row_index)))
                } else {
                    None
                }
            }
        }
    };
    (($typ: ty, $enum_variant: ident), $(($typ_rest: ty, $enum_variant_rest: ident)),+) => {
        impl_get_data_item!(($typ, $enum_variant));
        impl_get_data_item!($(($typ_rest, $enum_variant_rest)),+);
    };
}

impl_get_data_item!(
    (BooleanArray, Bool),
    (PrimitiveArray<Int32Type>, I32),
    (PrimitiveArray<Int64Type>, I64),
    (GenericByteArray<GenericStringType<i32>>, Str)
);

// ListArray item needs to build a `ListItem`, so is special
impl<'a> GetDataItem<'a> for GenericListArray<i32> {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            Some(DataItem::List(ListItem::new(self, row_index)))
        } else {
            None
        }
    }
}

// MapArray item needs to build a `MapItem`, so is special
impl<'a> GetDataItem<'a> for MapArray {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>> {
        if self.is_valid(row_index) {
            Some(DataItem::Map(MapItem::new(self, row_index)))
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
