use object_store::parse_url_opts as object_store_parse_url_opts;
use object_store::path::Path;
use object_store::{Error, ObjectStore};
use url::Url;

pub fn parse_url_opts<I, K, V>(url: &Url, options: I) -> Result<(Box<dyn ObjectStore>, Path), Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    object_store_parse_url_opts(url, options)
}
