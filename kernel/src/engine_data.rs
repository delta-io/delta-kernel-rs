use crate::{DeltaResult, Error};

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

macro_rules! gen_casts {
    (($fnname: ident, $enum_ty: ident, $typ: ty)) => {
        pub fn $fnname(&self) -> Option<$typ> {
            if let DataItem::$enum_ty(x) = self {
                Some(*x)
            } else {
                None
            }
        }
    };
    (($fnname: ident, $enum_ty: ident, $typ: ty), $(($fnname_rest: ident, $enum_ty_rest: ident, $typ_rest: ty)),+) => {
        gen_casts!(($fnname, $enum_ty, $typ));
        gen_casts!($(($fnname_rest, $enum_ty_rest, $typ_rest)),+);
    };
}

// a list that can go inside a DataItem
pub trait DataItemList {
    fn len(&self, row_index: usize) -> usize;
    fn get(&self, row_index: usize, list_index: usize) -> String;
}

// Note that copy/clone is cheap here as it's just a pointer and an int
// TODO(nick): Could avoid copy probably with manual impl of ExtractInto<&ListItem>
#[derive(Clone, Copy)]
pub struct ListItem<'a> {
    list: &'a dyn DataItemList,
    row: usize,
}

impl<'a> ListItem<'a> {
    pub fn new(list: &'a dyn DataItemList, row: usize) -> ListItem<'a> {
        ListItem { list, row }
    }

    pub fn len(&self) -> usize {
        self.list.len(self.row)
    }

    pub fn get(&self, list_index: usize) -> String {
        self.list.get(self.row, list_index)
    }
}

// a map that can go inside a DataItem
pub trait DataItemMap {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str>;
    fn materialize(&self, row_index: usize) -> HashMap<String, Option<String>>;
}

// Note that copy/clone is cheap here as it's just a pointer and an int
#[derive(Clone, Copy)]
pub struct MapItem<'a> {
    map: &'a dyn DataItemMap,
    row: usize,
}

impl<'a> MapItem<'a> {
    pub fn new(map: &'a dyn DataItemMap, row: usize) -> MapItem<'a> {
        MapItem { map, row }
    }

    pub fn get(&self, key: &str) -> Option<&'a str> {
        self.map.get(self.row, key)
    }

    pub fn materialize(&self) -> HashMap<String, Option<String>> {
        self.map.materialize(self.row)
    }
}

pub enum DataItem<'a> {
    Bool(bool),
    F32(f32),
    F64(f64),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    Str(&'a str),
    List(ListItem<'a>),
    Map(MapItem<'a>),
}

impl<'a> DataItem<'a> {
    gen_casts!(
        (as_bool, Bool, bool),
        (as_f32, F32, f32),
        (as_f64, F64, f64),
        (as_i32, I32, i32),
        (as_i64, I64, i64),
        (as_u32, U32, u32),
        (as_u64, U64, u64),
        (as_str, Str, &str),
        (as_list, List, ListItem<'a>),
        (as_map, Map, MapItem<'a>)
    );

    pub fn as_string(&self) -> Option<String> {
        self.as_str().map(|s| s.to_string())
    }
}

/// A trait similar to TryInto, that allows extracting a [`DataItem`] into a particular type
pub trait ExtractInto<T>: Sized {
    /// Extract a required item into type `T` for the specified `field_name`
    /// This returns an error if the item is not present
    fn extract_into(self, field_name: &str) -> DeltaResult<T> {
        let result = self.extract_into_opt(field_name)?;
        result.ok_or(Error::Generic(format!(
            "Missing value for required field: {field_name}"
        )))
    }
    /// Extract an optional item into type `T` for the specified `field_name`
    /// Returns `None` if the item is not present, or `Some(T)` if it is
    fn extract_into_opt(self, field_name: &str) -> DeltaResult<Option<T>>;
}
macro_rules! impl_extract_into {
    (($target_type: ty, $enum_variant: ident)) => {
        #[doc = "Attempt to extract a DataItem into a `"]
        #[doc = stringify!($target_type)]
        #[doc = "`. This does _not_ perform type  coersion, it just returns "]
        #[doc = concat!("`Ok(Some(", stringify!($target_type), "))`")]
        #[doc = " if the DataItem is a "]
        #[doc = concat!("`DataItem::", stringify!($enum_variant), "`")]
        #[doc = " or returns an error if it is not. "]
        #[doc = " Returns `Ok(None)` if the data item was not present in the source data."]
        impl<'a, 'b> ExtractInto<$target_type> for &'a Option<DataItem<'b>> {
            fn extract_into_opt(self, field_name: &str) -> DeltaResult<Option<$target_type>> {
                self.as_ref().map(|item| match item {
                    &DataItem::$enum_variant(x) => Ok(x),
                    _ => Err(Error::Generic(format!("Could not extract {field_name} as {}", stringify!($target_type))))
                }).transpose()
            }
        }
    };
    (($target_type: ty, $enum_variant: ident), $(($target_type_rest: ty, $enum_variant_rest: ident)),+) => {
        impl_extract_into!(($target_type, $enum_variant));
        impl_extract_into!($(($target_type_rest, $enum_variant_rest)),+);
    }
}

impl_extract_into!(
    (bool, Bool),
    (f32, F32),
    (f64, F64),
    (i32, I32),
    (i64, I64),
    (u32, U32),
    (u64, U64),
    (&'b str, Str),
    (ListItem<'b>, List),
    (MapItem<'b>, Map)
);

impl<'a, 'b> ExtractInto<&'a MapItem<'b>> for &'a Option<DataItem<'b>> {
    fn extract_into_opt(self, field_name: &str) -> DeltaResult<Option<&'a MapItem<'b>>> {
        self.as_ref().map(|item| match item {
            DataItem::Map(ref x) => Ok(x),
            _ => panic!()
        }).transpose()
    }
}

/// The `String` implementation for ExtractInto simply extracts the item as a &str and then
/// allocates a new string. This is a convenience wrapper only.
impl<'a, 'b> ExtractInto<String> for &'a Option<DataItem<'b>> {
    fn extract_into_opt(self, field_name: &str) -> DeltaResult<Option<String>> {
        let val: Option<&str> = self.extract_into_opt(field_name)?;
        Ok(val.map(|s| s.to_string()))
    }
}

pub trait GetDataItem<'a> {
    fn get(&'a self, row_index: usize) -> Option<DataItem<'a>>;
}

/// A `DataVisitor` can be called back to visit extracted data. Aside from calling
/// [`DataVisitor::visit`] on the visitor passed to [`crate::DataExtractor::extract`], engines do
/// not need to worry about this trait.
pub trait DataVisitor {
    // // Receive some data from a call to `extract`. The data in [vals] should not be assumed to live
    // // beyond the call to this funtion (i.e. it should be copied if needed)
    // // The row_index parameter must be the index of the found row in the data batch being processed.
    // fn visit(&mut self, row_index: usize, vals: &[Option<DataItem<'_>>]);

    /// The visitor is passed a slice of `GetDataItem` values, and a row count.
    // TODO(nick) better comment
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetDataItem<'a>]);
}

/// A TypeTag identifies the class that an Engine is using to represent data read by its
/// json/parquet readers. We don't parameterize our client by this to avoid having to specify the
/// generic type _everywhere_, and to make the ffi story easier. TypeTags nevertheless allow us some
/// amount of runtime type-safety as an engine can check that it got called with a data type it
/// understands.
pub trait TypeTag: 'static {
    // Can't use `:Eq / :PartialEq` as that's generic, and we want to return this trait as an object
    // below. We require the 'static bound so we can be sure the TypeId will live long enough to
    // return. In practice this just means that the type must be fully defined and not a generated type.

    /// Return a [`std::any::TypeId`] for this tag.
    fn tag_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    /// Check if this tag is equivalent to another tag
    fn eq(&self, other: &dyn TypeTag) -> bool {
        let my_id = self.tag_id();
        let other_id = other.tag_id();
        my_id == other_id
    }
}

/// Any type that an engine wants to return as "data" needs to implement this trait. This should be
/// as easy as defining a tag to represent it that implements [`TypeTag`], and then returning it for
/// the `type_tag` method.
/// ```
/// use std::any::Any;
/// use deltakernel::{DataExtractor, DeltaResult};
/// use deltakernel::engine_data::{DataVisitor, EngineData, TypeTag};
/// use deltakernel::schema::SchemaRef;
/// struct MyTypeTag;
/// impl TypeTag for MyTypeTag {}
/// struct MyDataType; // Whatever the engine wants here
/// impl EngineData for MyDataType {
///    fn type_tag(&self) -> &dyn TypeTag {
///        &MyTypeTag
///    }
///    fn as_any(&self) -> &(dyn Any + 'static) { self }
///    fn into_any(self: Box<Self>) -> Box<dyn Any> { self }
/// }
/// struct MyDataExtractor {
///   expected_tag: MyTypeTag,
/// }
/// impl DataExtractor for MyDataExtractor {
///   fn extract(&self, blob: &dyn EngineData, _schema: SchemaRef, visitor: &mut dyn DataVisitor) -> DeltaResult<()> {
///     assert!(self.expected_tag.eq(blob.type_tag())); // Ensure correct data type
///     // extract the data and call back visitor
///     Ok(())
///   }
///   fn length(&self, blob: &dyn EngineData) -> usize {
///     assert!(self.expected_tag.eq(blob.type_tag())); // Ensure correct data type
///     let len = 0; // actually get the len here
///     len
///   }
/// }
/// ```
pub trait EngineData: Send {
    fn type_tag(&self) -> &dyn TypeTag;

    // TODO(nick) implement this and below when it doesn't cause a compiler error
    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}
