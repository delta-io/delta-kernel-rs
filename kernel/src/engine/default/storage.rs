#[cfg(feature = "cloud")]
use hdfs_native_object_store::HdfsObjectStore;
use object_store::parse_url_opts as parse_url_opts_object_store;
use object_store::path::Path;
use object_store::{Error, ObjectStore};
use url::Url;

pub fn parse_url_opts<I, K, V>(url: &Url, options: I) -> Result<(Box<dyn ObjectStore>, Path), Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    match url.scheme() {
        #[cfg(feature = "cloud")]
        "hdfs" | "viewfs" => parse_url_opts_hdfs_native(url, options),
        _ => parse_url_opts_object_store(url, options),
    }
}

#[cfg(feature = "cloud")]
pub fn parse_url_opts_hdfs_native<I, K, V>(
    url: &Url,
    options: I,
) -> Result<(Box<dyn ObjectStore>, Path), Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let options_map = options
        .into_iter()
        .map(|(k, v)| (k.as_ref().to_string(), v.into()))
        .collect();
    let store = HdfsObjectStore::with_config(url.as_str(), options_map)?;
    let path = Path::parse(url.path())?;
    Ok((Box::new(store), path))
}
