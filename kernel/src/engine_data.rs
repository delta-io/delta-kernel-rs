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
pub trait ListItem {
    fn len(&self, row_index: usize) -> usize;
    fn get(&self, row_index: usize, list_index: usize) -> String;
}

// a map that can go inside a DataItem
pub trait MapItem {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str>;
    fn materialize(&self, row_index: usize) -> HashMap<String, Option<String>>;
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
    List(&'a dyn ListItem),
    Map(&'a dyn MapItem),
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
        (as_map, Map, &dyn MapItem)
    );
}

/// A `DataVisitor` can be called back to visit extracted data. Aside from calling
/// [`DataVisitor::visit`] on the visitor passed to [`crate::DataExtractor::extract`], engines do
/// not need to worry about this trait.
pub trait DataVisitor {
    // Receive some data from a call to `extract`. The data in [vals] should not be assumed to live
    // beyond the call to this funtion (i.e. it should be copied if needed)
    // The row_index parameter must be the index of the found row in the data batch being processed.
    fn visit(&mut self, row_index: usize, vals: &[Option<DataItem<'_>>]);
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
/// use deltakernel::DataExtractor;
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
///   fn extract(&self, blob: &dyn EngineData, _schema: SchemaRef, visitor: &mut dyn DataVisitor) -> () {
///     assert!(self.expected_tag.eq(blob.type_tag())); // Ensure correct data type
///     // extract the data and call back visitor
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

    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}
