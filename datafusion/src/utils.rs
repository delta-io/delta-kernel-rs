use datafusion_common::HashMap;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
use delta_kernel::object_store::path::Path;
use delta_kernel::{DeltaResult, Error as DeltaError, FileMeta, FileSlice};
use itertools::Itertools;
use url::Url;

pub(crate) trait AsObjectStoreUrl {
    fn as_object_store_url(&self) -> ObjectStoreUrl;
}

impl AsObjectStoreUrl for Url {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        get_store_url(self)
    }
}

impl AsObjectStoreUrl for FileMeta {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.location.as_object_store_url()
    }
}

impl AsObjectStoreUrl for &FileMeta {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.location.as_object_store_url()
    }
}

impl AsObjectStoreUrl for FileSlice {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.0.as_object_store_url()
    }
}

pub(crate) trait AsObjectStorePath {
    fn as_object_store_path(&self) -> Path;
}

impl AsObjectStorePath for Url {
    fn as_object_store_path(&self) -> Path {
        Path::from_url_path(self.path()).unwrap()
    }
}

pub(crate) fn group_by_store<T: IntoIterator<Item = impl AsObjectStoreUrl>>(
    files: T,
) -> std::collections::HashMap<ObjectStoreUrl, Vec<T::Item>> {
    files
        .into_iter()
        .map(|item| (item.as_object_store_url(), item))
        .into_group_map()
}

pub(crate) fn grouped_partitioned_files(
    files: &[FileMeta],
) -> DeltaResult<HashMap<ObjectStoreUrl, Vec<PartitionedFile>>> {
    group_by_store(files)
        .into_iter()
        .map(to_partitioned_files)
        .try_collect::<_, HashMap<_, _>, _>()
}

fn to_partitioned_files(
    arg: (ObjectStoreUrl, Vec<&FileMeta>),
) -> DeltaResult<(ObjectStoreUrl, Vec<PartitionedFile>)> {
    let (url, files) = arg;
    let part_files = files
        .into_iter()
        .map(|f| {
            let path = f.location.as_object_store_path();
            let mut partitioned_file = PartitionedFile::new(path.to_string(), f.size);
            // NB: we need to reassign the location since the 'new' method does
            // incorrect or inconsistent encoding internally.
            partitioned_file.object_meta.location = path;
            Ok::<_, DeltaError>(partitioned_file)
        })
        .try_collect::<_, Vec<_>, _>()?;
    Ok::<_, DeltaError>((url, part_files))
}

fn get_store_url(url: &url::Url) -> ObjectStoreUrl {
    ObjectStoreUrl::parse(format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    ))
    // Safety: The url is guaranteed to be valid as we construct it
    // from a valid url and pass only the parts that object store url
    // expects.
    .unwrap()
}
