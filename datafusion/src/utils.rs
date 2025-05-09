use datafusion_execution::object_store::ObjectStoreUrl;
use delta_kernel::{FileMeta, FileSlice};
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

pub(crate) fn group_by_store<T: IntoIterator<Item = impl AsObjectStoreUrl>>(
    files: T,
) -> std::collections::HashMap<ObjectStoreUrl, Vec<T::Item>> {
    files
        .into_iter()
        .map(|item| (item.as_object_store_url(), item))
        .into_group_map()
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
