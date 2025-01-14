//! Traits that engines need to implement in order to pass data between themselves and kernel.

use crate::schema::{ColumnName, DataType};
use crate::{AsAny, DeltaResult, Error};

use tracing::debug;

use std::collections::HashMap;

/// a trait that an engine exposes to give access to a list
pub trait EngineList {
    /// Return the length of the list at the specified row_index in the raw data
    fn len(&self, row_index: usize) -> usize;
    /// Get the item at `list_index` from the list at `row_index` in the raw data, and return it as a [`String`]
    fn get(&self, row_index: usize, list_index: usize) -> String;
    /// Materialize the entire list at row_index in the raw data into a `Vec<String>`
    fn materialize(&self, row_index: usize) -> Vec<String>;
}

/// A list item is useful if the Engine needs to know what row of raw data it needs to access to
/// implement the [`EngineList`] trait. It simply wraps such a list, and the row.
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, list_index: usize) -> String {
        self.list.get(self.row, list_index)
    }

    pub fn materialize(&self) -> Vec<String> {
        self.list.materialize(self.row)
    }
}

/// a trait that an engine exposes to give access to a map
pub trait EngineMap {
    /// Get the item with the specified key from the map at `row_index` in the raw data, and return it as an `Option<&'a str>`
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str>;
    /// Materialize the entire map at `row_index` in the raw data into a `HashMap`
    fn materialize(&self, row_index: usize) -> HashMap<String, String>;
}

/// A map item is useful if the Engine needs to know what row of raw data it needs to access to
/// implement the [`EngineMap`] trait. It simply wraps such a map, and the row.
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

    pub fn materialize(&self) -> HashMap<String, String> {
        self.map.materialize(self.row)
    }
}

macro_rules! impl_default_get {
    ( $(($name: ident, $typ: ty)), * ) => {
        $(
            fn $name(&'a self, _row_index: usize, field_name: &str) -> DeltaResult<Option<$typ>> {
                debug!("Asked for type {} on {field_name}, but using default error impl.", stringify!($typ));
                Err(Error::UnexpectedColumnType(format!("{field_name} is not of type {}", stringify!($typ))).with_backtrace())
            }
        )*
    };
}

/// When calling back into a [`RowVisitor`], the engine needs to provide a slice of items that
/// implement this trait. This allows type_safe extraction from the raw data by the kernel. By
/// default all these methods will return an `Error` that an incorrect type has been asked
/// for. Therefore, for each "data container" an Engine has, it is only necessary to implement the
/// `get_x` method for the type it holds.
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

macro_rules! impl_null_get {
    ( $(($name: ident, $typ: ty)), * ) => {
        $(
            fn $name(&'a self, _row_index: usize, _field_name: &str) -> DeltaResult<Option<$typ>> {
                Ok(None)
            }
        )*
    };
}

impl<'a> GetData<'a> for () {
    impl_null_get!(
        (get_bool, bool),
        (get_int, i32),
        (get_long, i64),
        (get_str, &'a str),
        (get_list, ListItem<'a>),
        (get_map, MapItem<'a>)
    );
}

/// This is a convenience wrapper over `GetData` to allow code like: `let name: Option<String> =
/// getters[1].get_opt(row_index, "metadata.name")?;`
pub trait TypedGetData<'a, T> {
    fn get_opt(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<T>>;
    fn get(&'a self, row_index: usize, field_name: &str) -> DeltaResult<T> {
        let val = self.get_opt(row_index, field_name)?;
        val.ok_or_else(|| {
            Error::MissingData(format!("Data missing for field {field_name}")).with_backtrace()
        })
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

/// Provide an impl to get a list field as a `Vec<String>`. Note that this will allocate the vector
/// and allocate for each string entry.
impl<'a> TypedGetData<'a, Vec<String>> for dyn GetData<'a> + '_ {
    fn get_opt(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<Vec<String>>> {
        let list_opt: Option<ListItem<'_>> = self.get_opt(row_index, field_name)?;
        Ok(list_opt.map(|list| list.materialize()))
    }
}

/// Provide an impl to get a map field as a `HashMap<String, String>`. Note that this will
/// allocate the map and allocate for each entry
impl<'a> TypedGetData<'a, HashMap<String, String>> for dyn GetData<'a> + '_ {
    fn get_opt(
        &'a self,
        row_index: usize,
        field_name: &str,
    ) -> DeltaResult<Option<HashMap<String, String>>> {
        let map_opt: Option<MapItem<'_>> = self.get_opt(row_index, field_name)?;
        Ok(map_opt.map(|map| map.materialize()))
    }
}

/// A `RowVisitor` can be called back to visit extracted data. Aside from calling
/// [`RowVisitor::visit`] on the visitor passed to [`EngineData::visit_rows`], engines do
/// not need to worry about this trait.
pub trait RowVisitor {
    /// The names and types of leaf fields this visitor accesses. The `EngineData` being visited
    /// validates these types when extracting column getters, and [`RowVisitor::visit`] will receive
    /// one getter for each selected field, in the requested order. The column names are used by
    /// [`RowVisitor::visit_rows_of`] to select fields from a "typical" `EngineData`; callers whose
    /// engine data has different column names can manually invoke [`EngineData::visit_rows`].
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]);

    /// Have the visitor visit the data. This will be called on a visitor passed to
    /// [`EngineData::visit_rows`]. For each leaf in the schema that was passed to `extract` a
    /// "getter" of type [`GetData`] will be present. This can be used to actually get at the data
    /// for each row. You can `use` the `TypedGetData` trait if you want to have a way to extract
    /// typed data that will fail if the "getter" is for an unexpected type.  The data in `getters`
    /// does not outlive the call to this function (i.e. it should be copied if needed).
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()>;

    /// Visit the rows of an [`EngineData`], selecting the leaf column names given by
    /// [`RowVisitor::selected_column_names_and_types`]. This is a thin wrapper around
    /// [`EngineData::visit_rows`] which in turn will eventually invoke [`RowVisitor::visit`].
    fn visit_rows_of(&mut self, data: &dyn EngineData) -> DeltaResult<()>
    where
        Self: Sized,
    {
        data.visit_rows(self.selected_column_names_and_types().0, self)
    }
}

/// Any type that an engine wants to return as "data" needs to implement this trait. The bulk of the
/// work is in the [`EngineData::visit_rows`] method. See the docs for that method for more details.
/// ```rust
/// # use std::any::Any;
/// # use delta_kernel::DeltaResult;
/// # use delta_kernel::engine_data::{RowVisitor, EngineData, GetData};
/// # use delta_kernel::expressions::ColumnName;
/// struct MyDataType; // Whatever the engine wants here
/// impl MyDataType {
///   fn do_extraction<'a>(&self) -> Vec<&'a dyn GetData<'a>> {
///      /// Actually do the extraction into getters
///      todo!()
///   }
/// }
///
/// impl EngineData for MyDataType {
///   fn visit_rows(&self, leaf_columns: &[ColumnName], visitor: &mut dyn RowVisitor) -> DeltaResult<()> {
///     let getters = self.do_extraction(); // do the extraction
///     visitor.visit(self.len(), &getters); // call the visitor back with the getters
///     Ok(())
///   }
///   fn len(&self) -> usize {
///     todo!() // actually get the len here
///   }
/// }
/// ```
pub trait EngineData: AsAny {
    /// Visits a subset of leaf columns in each row of this data, passing a `GetData` item for each
    /// requested column to the visitor's `visit` method (along with the number of rows of data to
    /// be visited).
    fn visit_rows(
        &self,
        column_names: &[ColumnName],
        visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()>;

    /// Return the number of items (rows) in blob
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
