use datafusion_common::Result;
use datafusion_execution::object_store::ObjectStoreUrl;

pub(crate) fn get_store_url(url: &url::Url) -> Result<ObjectStoreUrl> {
    ObjectStoreUrl::parse(format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    ))
}
