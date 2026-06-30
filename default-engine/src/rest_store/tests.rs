use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use delta_kernel::object_store::path::Path;
// 0.13 puts the get/put/copy convenience methods on ObjectStoreExt; 0.12 has them on
// ObjectStore directly, where importing the Ext trait too would make the calls ambiguous.
#[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
use delta_kernel::object_store::ObjectStoreExt;
use delta_kernel::object_store::{
    Error as ObjectStoreError, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload,
    Result as ObjectStoreResult, UpdateVersion,
};
use futures::StreamExt as _;
use reqwest::header::HeaderMap;
use reqwest::Client;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

fn token_producer(
    calls: Arc<AtomicUsize>,
    ttl: Option<std::time::Duration>,
) -> RefreshingHeaderProvider {
    RefreshingHeaderProvider::new(move || {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        let headers =
            headers_from_pairs([("authorization".to_string(), format!("Bearer token-{n}"))])?;
        Ok((headers, ttl))
    })
}

/// Without a TTL, [`RefreshingHeaderProvider`] produces on every call -- refreshed per request.
#[test]
fn refreshing_header_provider_without_ttl_produces_each_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = token_producer(calls.clone(), None);
    let first = provider.headers().unwrap();
    let second = provider.headers().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(first.get("authorization").unwrap(), "Bearer token-0");
    assert_eq!(second.get("authorization").unwrap(), "Bearer token-1");
}

/// With a TTL, headers are cached and the closure runs once until the TTL nears expiry.
#[test]
fn refreshing_header_provider_with_ttl_caches() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = token_producer(calls.clone(), Some(std::time::Duration::from_secs(3600)));
    let first = provider.headers().unwrap();
    let second = provider.headers().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.get("authorization").unwrap(), "Bearer token-0");
    assert_eq!(second.get("authorization").unwrap(), "Bearer token-0");
}

/// A minimal REST dialect for tests: files at `/files/{path}`, directories at `/dirs/{path}`, and
/// a `{ "contents": [{path, size}], "nextPageToken" }` list body. Other field names and the
/// canonical 404 -> NotFound / 409 -> AlreadyExists status mapping come from
/// [`RestEndpointConfig::default`].
fn test_config() -> RestEndpointConfig {
    RestEndpointConfig {
        files_prefix: "files".into(),
        directories_prefix: "dirs".into(),
        entry_size_field: "size".into(),
        ..Default::default()
    }
}

fn store_for(server: &MockServer, headers: HeaderMap) -> RestObjectStore {
    RestObjectStore::new(
        server.uri(),
        Client::new(),
        Arc::new(StaticHeaderProvider::new(headers)),
        Arc::new(test_config()),
    )
}

#[tokio::test]
async fn get_returns_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".as_slice()))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let bytes = store
        .get(&Path::from("a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"hello");
}

/// A malformed `Content-Range` surfaces as an error rather than silently degrading the
/// reported range/size (matches `object_store`'s own client).
#[tokio::test]
async fn get_malformed_content_range_yields_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-range", "bytes not-a-range")
                .set_body_bytes(b"hello".as_slice()),
        )
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let err = store.get(&Path::from("a.txt")).await.unwrap_err();
    assert!(
        matches!(err, ObjectStoreError::Generic { .. }),
        "got {err:?}"
    );
}

/// A ranged GET whose server ignores the range and returns a non-partial `200` with the full
/// body must error rather than silently treating the whole body as the requested slice (mirrors
/// object_store's `NotPartial` behavior).
#[tokio::test]
async fn ranged_get_non_partial_response_yields_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"full-body".as_slice()))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let err = store
        .get_opts(
            &Path::from("a.txt"),
            GetOptions {
                range: Some((0..4).into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, ObjectStoreError::Generic { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn head_uses_http_head_and_returns_meta_without_body() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/files/a.txt"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "42")
                .insert_header("etag", "\"abc\""),
        )
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let res = store
        .get_opts(
            &Path::from("a.txt"),
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(res.meta.size, 42);
    assert_eq!(res.meta.e_tag.as_deref(), Some("\"abc\""));
    // head must not stream a body.
    let body = res.bytes().await.unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn put_overwrite_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    store
        .put(&Path::from("a.txt"), PutPayload::from_static(b"hi"))
        .await
        .unwrap();
}

#[tokio::test]
async fn put_create_conflict_maps_to_already_exists() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(409))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let err = store
        .put_opts(
            &Path::from("a.txt"),
            PutPayload::from_static(b"hi"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectStoreError::AlreadyExists { .. }));
}

#[tokio::test]
async fn put_update_is_not_supported() {
    let server = MockServer::start().await;
    let store = store_for(&server, HeaderMap::new());
    let err = store
        .put_opts(
            &Path::from("a.txt"),
            PutPayload::from_static(b"hi"),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some("v1".to_string()),
                    version: None,
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectStoreError::NotSupported { .. }));
}

#[tokio::test]
async fn list_paginates_across_pages() {
    let server = MockServer::start().await;
    // First page (no page_token) returns one file + a token; second page returns two.
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .and(wiremock::matchers::query_param_is_missing("page_token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"contents":[{"path":"d/1","size":1}],"nextPageToken":"p2"}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .and(wiremock::matchers::query_param("page_token", "p2"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/2","size":2},{"path":"d/3","size":3}]}"#,
            ),
        )
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let metas: Vec<_> = store
        .list(Some(&Path::from("d")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let paths: Vec<_> = metas
        .iter()
        .map(|m| m.location.as_ref().to_string())
        .collect();
    assert_eq!(paths, vec!["d/1", "d/2", "d/3"]);
}

#[tokio::test]
async fn list_missing_directory_is_empty() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let metas: Vec<_> = store
        .list(Some(&Path::from("missing")))
        .collect::<Vec<_>>()
        .await;
    assert!(metas.is_empty());
}

/// A first-page 404 lists as empty, but a 404 *mid-pagination* (page_token set) means the
/// listing was truncated -- it must surface as an error, not a silent partial result.
#[tokio::test]
async fn list_not_found_mid_pagination_yields_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .and(wiremock::matchers::query_param_is_missing("page_token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"contents":[{"path":"d/1","size":1}],"nextPageToken":"p2"}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .and(wiremock::matchers::query_param("page_token", "p2"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
    // First-page entry is yielded, then the mid-pagination 404 surfaces as an error.
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert!(
        matches!(results[1], Err(ObjectStoreError::NotFound { .. })),
        "got {:?}",
        results[1]
    );
}

/// `list_with_offset` must exclude the offset entry itself even if the backend echoes it
/// back, so log replay never re-ingests the offset commit.
#[tokio::test]
async fn list_with_offset_excludes_offset_entry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/2","size":2},{"path":"d/3","size":3}]}"#,
            ),
        )
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let paths: Vec<_> = store
        .list_with_offset(Some(&Path::from("d")), &Path::from("d/2"))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap().location.as_ref().to_string())
        .collect();
    assert_eq!(paths, vec!["d/3"]);
}

/// An out-of-order page violates the sorted-listing contract and must surface as an error
/// rather than feeding log replay a misordered listing.
#[tokio::test]
async fn list_out_of_order_entries_yields_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(
                r#"{"contents":[{"path":"d/3","size":3},{"path":"d/1","size":1}]}"#,
            ),
        )
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().location.as_ref(), "d/3");
    assert!(matches!(results[1], Err(ObjectStoreError::Generic { .. })));
}

/// The sorted-listing contract holds *across* pages: a later page whose first entry sorts before
/// the previous page's last entry must surface as an error, not feed log replay a misordered list.
#[tokio::test]
async fn list_out_of_order_across_pages_yields_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .and(wiremock::matchers::query_param_is_missing("page_token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"contents":[{"path":"d/3","size":3}],"nextPageToken":"p2"}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .and(wiremock::matchers::query_param("page_token", "p2"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"contents":[{"path":"d/1","size":1}]}"#),
        )
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().location.as_ref(), "d/3");
    assert!(
        matches!(results[1], Err(ObjectStoreError::Generic { .. })),
        "got {:?}",
        results[1]
    );
}

/// An unparseable entry path fails the whole page rather than being silently dropped, consistent
/// with the module's fail-loudly philosophy.
#[test]
fn parse_list_rejects_unparseable_entry_path() {
    let config = test_config();
    // A `\0` byte in the path is not a valid object-store path.
    let body = "{\"contents\":[{\"path\":\"d/\\u0000bad\",\"size\":1}]}";
    assert!(matches!(
        config.parse_list(body.as_bytes()),
        Err(ObjectStoreError::Generic { .. })
    ));
}

#[tokio::test]
async fn auth_headers_are_sent() {
    let server = MockServer::start().await;
    // Only matches when the provider's header is present.
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .and(header("x-test-auth", "secret"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok".as_slice()))
        .mount(&server)
        .await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-test-auth",
        reqwest::header::HeaderValue::from_static("secret"),
    );
    let store = store_for(&server, headers);
    let bytes = store
        .get(&Path::from("a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"ok");
}

#[tokio::test]
async fn unsupported_operations_error_not_panic() {
    let server = MockServer::start().await;
    let store = store_for(&server, HeaderMap::new());
    assert!(matches!(
        store
            .copy(&Path::from("a"), &Path::from("b"))
            .await
            .unwrap_err(),
        ObjectStoreError::NotSupported { .. }
    ));
    assert!(matches!(
        store
            .list_with_delimiter(Some(&Path::from("d")))
            .await
            .unwrap_err(),
        ObjectStoreError::NotSupported { .. }
    ));
}

#[tokio::test]
async fn config_driven_contract_parses_list_and_reads() {
    let server = MockServer::start().await;
    let config = RestEndpointConfig {
        files_prefix: "api/2.0/fs/files".into(),
        directories_prefix: "api/2.0/fs/directories".into(),
        ..Default::default()
    };
    // Listing with one file and one subdirectory; the directory entry must be filtered out.
    Mock::given(method("GET"))
        .and(path("/api/2.0/fs/directories/d"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"contents":[{"path":"d/f1","fileSize":7,"isDirectory":false,"lastModified":1000},{"path":"d/sub","isDirectory":true}]}"#,
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/fs/files/d/f1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"content".as_slice()))
        .mount(&server)
        .await;
    let store = RestObjectStore::new(
        server.uri(),
        Client::new(),
        Arc::new(StaticHeaderProvider::new(HeaderMap::new())),
        Arc::new(config),
    );
    let metas: Vec<_> = store
        .list(Some(&Path::from("d")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].location.as_ref(), "d/f1");
    assert_eq!(metas[0].size, 7);
    assert_eq!(metas[0].last_modified.timestamp_millis(), 1000);
    let bytes = store
        .get(&Path::from("d/f1"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"content");
}

#[tokio::test]
async fn delete_issues_http_delete() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new());
    store.delete(&Path::from("a.txt")).await.unwrap();
}

fn store_with_strip_prefix(server: &MockServer, prefix: &str) -> RestObjectStore {
    let config = RestEndpointConfig {
        directories_prefix: "dirs".into(),
        entry_strip_prefix: Some(prefix.into()),
        ..Default::default()
    };
    RestObjectStore::new(
        server.uri(),
        Client::new(),
        Arc::new(StaticHeaderProvider::new(HeaderMap::new())),
        Arc::new(config),
    )
}

#[tokio::test]
async fn list_strips_entry_prefix() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"contents":[{"path":"/TablesById/u/d/f1","fileSize":1}]}"#),
        )
        .mount(&server)
        .await;
    let store = store_with_strip_prefix(&server, "/TablesById/u");
    let metas: Vec<_> = store
        .list(Some(&Path::from("d")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].location.as_ref(), "d/f1");
}

#[tokio::test]
async fn list_rejects_entry_outside_strip_prefix() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dirs/d"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"contents":[{"path":"/other/x"}]}"#),
        )
        .mount(&server)
        .await;
    let store = store_with_strip_prefix(&server, "/TablesById/u");
    let results: Vec<_> = store.list(Some(&Path::from("d"))).collect::<Vec<_>>().await;
    assert!(matches!(
        results.as_slice(),
        [Err(ObjectStoreError::Generic { .. })]
    ));
}

#[tokio::test]
async fn get_retries_transient_5xx() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok".as_slice()))
        .with_priority(2)
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new()).with_max_retries(1);
    let bytes = store
        .get(&Path::from("a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"ok");
}

/// PUT returns 500; the verified Create then reads back via `read_back`, with the read-back
/// response supplied by `get`. `max_retries` is 0, so an absent read-back fails immediately.
async fn put_create_with_readback(
    server: &MockServer,
    get: ResponseTemplate,
) -> ObjectStoreResult<()> {
    Mock::given(method("PUT"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(500))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(get)
        .mount(server)
        .await;
    store_for(server, HeaderMap::new())
        .with_verify_on_ambiguous(true)
        .put_opts(
            &Path::from("a.txt"),
            PutPayload::from_static(b"hi"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .map(|_| ())
}

/// An ambiguous 5xx whose read-back matches what we wrote is treated as a success.
#[tokio::test]
async fn put_create_verified_matching_write_succeeds() {
    let server = MockServer::start().await;
    let get = ResponseTemplate::new(200).set_body_bytes(b"hi".as_slice());
    put_create_with_readback(&server, get).await.unwrap();
}

/// An ambiguous 5xx whose read-back differs is a real conflict.
#[tokio::test]
async fn put_create_verified_conflicting_write_is_already_exists() {
    let server = MockServer::start().await;
    let get = ResponseTemplate::new(200).set_body_bytes(b"other".as_slice());
    let err = put_create_with_readback(&server, get).await.unwrap_err();
    assert!(matches!(err, ObjectStoreError::AlreadyExists { .. }));
}

/// An ambiguous 5xx whose read-back is absent (write never landed) fails to confirm.
#[tokio::test]
async fn put_create_verified_absent_write_fails_to_confirm() {
    let server = MockServer::start().await;
    let err = put_create_with_readback(&server, ResponseTemplate::new(404))
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectStoreError::Generic { .. }));
}

/// Exhausting retries on persistent 5xx surfaces an error rather than looping or succeeding.
#[tokio::test]
async fn get_retry_exhaustion_surfaces_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let store = store_for(&server, HeaderMap::new()).with_max_retries(1);
    let err = store.get(&Path::from("a.txt")).await.unwrap_err();
    assert!(matches!(err, ObjectStoreError::Generic { .. }));
}

/// A verified Create retries while the read-back is absent, then succeeds once it lands.
#[tokio::test]
async fn put_create_verified_retries_until_write_lands() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(404))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hi".as_slice()))
        .with_priority(2)
        .mount(&server)
        .await;
    store_for(&server, HeaderMap::new())
        .with_verify_on_ambiguous(true)
        .with_max_retries(1)
        .put_opts(
            &Path::from("a.txt"),
            PutPayload::from_static(b"hi"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

/// On a retry, a `409` is the writer's own already-landed write rather than a foreign conflict:
/// attempt1 PUT 500 with an absent read-back consumes a retry, then attempt2 PUT 409 reconciles
/// via read-back to matching bytes and succeeds (not `AlreadyExists`).
#[tokio::test]
async fn put_create_verified_retry_conflict_matches_own_write_succeeds() {
    let server = MockServer::start().await;
    // Attempt 1 PUT -> 500 (ambiguous); attempt 2 PUT -> 409 (our own landed write).
    Mock::given(method("PUT"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(409))
        .with_priority(2)
        .mount(&server)
        .await;
    // Read-back after attempt 1 -> 404 (absent, consume a retry); after attempt 2 -> matching body.
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(404))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hi".as_slice()))
        .with_priority(2)
        .mount(&server)
        .await;
    store_for(&server, HeaderMap::new())
        .with_verify_on_ambiguous(true)
        .with_max_retries(2)
        .put_opts(
            &Path::from("a.txt"),
            PutPayload::from_static(b"hi"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_missing_maps_to_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/files/a.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let err = store_for(&server, HeaderMap::new())
        .delete(&Path::from("a.txt"))
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectStoreError::NotFound { .. }));
}

#[cfg(any(not(feature = "arrow-57"), feature = "arrow-58"))]
#[tokio::test]
async fn delete_stream_deletes_each_path() {
    let server = MockServer::start().await;
    for p in ["a", "b"] {
        Mock::given(method("DELETE"))
            .and(path(format!("/files/{p}")))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
    }
    let store = store_for(&server, HeaderMap::new());
    let paths = futures::stream::iter([Ok(Path::from("a")), Ok(Path::from("b"))]).boxed();
    let deleted: Vec<_> = store.delete_stream(paths).collect::<Vec<_>>().await;
    assert_eq!(deleted.len(), 2);
    assert!(deleted.iter().all(|r| r.is_ok()));
}

#[test]
fn build_rest_client_rejects_partial_mtls() {
    let opts = RestClientOptions {
        cert_path: Some("/x/cert.pem".into()),
        ..Default::default()
    };
    assert!(build_rest_client(&opts).is_err());
}

#[test]
fn build_rest_client_rejects_malformed_dns_override() {
    let missing_eq = RestClientOptions {
        dns_overrides: vec!["no-equals".into()],
        ..Default::default()
    };
    assert!(build_rest_client(&missing_eq).is_err());
    let bad_addr = RestClientOptions {
        dns_overrides: vec!["host=not-an-addr".into()],
        ..Default::default()
    };
    assert!(build_rest_client(&bad_addr).is_err());
}

#[test]
fn build_rest_client_plain_and_valid_dns_override_succeed() {
    assert!(build_rest_client(&RestClientOptions::default()).is_ok());
    let opts = RestClientOptions {
        // Empty entries are skipped; a valid `host=ip:port` is accepted.
        dns_overrides: vec!["".into(), "example.com=127.0.0.1:8443".into()],
        timeout_secs: Some(5),
        ..Default::default()
    };
    assert!(build_rest_client(&opts).is_ok());
}
