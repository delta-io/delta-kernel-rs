use crate::{schema::SchemaRef, DeltaResult, Error};

use tracing::debug;

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

// a trait that an engine exposes to give access to a list
pub trait EngineList {
    fn len(&self, row_index: usize) -> usize;
    fn get(&self, row_index: usize, list_index: usize) -> String;
    fn materialize(&self, row_index: usize) -> Vec<String>;
}

pub struct ListItem<'a> {
    list: &'a dyn EngineList,
    row: usize,
}

impl<'a> ListItem<'a> {
    pub fn new(list: &'a dyn EngineList, row: usize) -> ListItem<'a> {
        ListItem { list, row }
    }

    pub fn len(&self) -> usize {
        self.list.len(self.row)
    }

    pub fn get(&self, list_index: usize) -> String {
        self.list.get(self.row, list_index)
    }

    pub fn materialize(&self) -> Vec<String> {
        self.list.materialize(self.row)
    }
}

// a trait that an engine exposes to give access to a map
pub trait EngineMap {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str>;
    fn materialize(&self, row_index: usize) -> HashMap<String, Option<String>>;
}

pub struct MapItem<'a> {
    map: &'a dyn EngineMap,
    row: usize,
}

impl<'a> MapItem<'a> {
    pub fn new(map: &'a dyn EngineMap, row: usize) -> MapItem<'a> {
        MapItem { map, row }
    }

    pub fn get(&self, key: &str) -> Option<&'a str> {
        self.map.get(self.row, key)
    }

    pub fn materialize(&self) -> HashMap<String, Option<String>> {
        self.map.materialize(self.row)
    }
}

macro_rules! impl_default_get {
    ( $(($name: ident, $typ: ty)), * ) => {
        $(
            fn $name(&'a self, _row_index: usize, field_name: &str) -> DeltaResult<Option<$typ>> {
                debug!("Asked for type {} on {field_name}, but using default error impl.", stringify!($typ));
                Err(Error::UnexpectedColumnType(format!("{field_name} is not of type {}", stringify!($typ))))
            }
        )*
    };
}

pub trait GetData<'a> {
    impl_default_get!(
        (get_bool, bool),
        (get_int, i32),
        (get_long, i64),
        (get_str, &'a str),
        (get_list, ListItem<'a>),
        (get_map, MapItem<'a>)
    );
}

pub trait TypedGetData<'a, T> {
    fn get_opt(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<T>>;
    fn get(&'a self, row_index: usize, field_name: &str) -> DeltaResult<T> {
        let val = self.get_opt(row_index, field_name)?;
        val.ok_or_else(||Error::MissingData(format!(
            "Data missing for field {field_name}"
        )))
    }
}

macro_rules! impl_typed_get_data {
    ( $(($name: ident, $typ: ty)), * ) => {
        $(
            impl<'a> TypedGetData<'a, $typ> for dyn GetData<'a> +'_ {
                fn get_opt(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<$typ>> {
                    self.$name(row_index, field_name)
                }
            }
        )*
    };
}

impl_typed_get_data!(
    (get_bool, bool),
    (get_int, i32),
    (get_long, i64),
    (get_str, &'a str),
    (get_list, ListItem<'a>),
    (get_map, MapItem<'a>)
);

impl<'a> TypedGetData<'a, String> for dyn GetData<'a> + '_ {
    fn get_opt(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<String>> {
        self.get_str(row_index, field_name)
            .map(|s| s.map(|s| s.to_string()))
    }
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
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()>;
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
/// TODO(Nick): Make this code again
/// use std::any::Any;
/// use deltakernel::DeltaResult;
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
pub trait EngineData: Send {
    fn extract(&self, schema: SchemaRef, visitor: &mut dyn DataVisitor) -> DeltaResult<()>;
    // Return the number of items (rows?) in blob
    fn length(&self) -> usize;

    fn type_tag(&self) -> &dyn TypeTag;

    // TODO(nick) implement this and below here in the trait when it doesn't cause a compiler error
    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}
