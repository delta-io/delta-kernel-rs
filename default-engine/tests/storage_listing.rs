use std::sync::Arc;

use delta_kernel::object_store::aws::AmazonS3Builder;
use delta_kernel::object_store::azure::MicrosoftAzureBuilder;
use delta_kernel::object_store::gcp::GoogleCloudStorageBuilder;
use delta_kernel::object_store::path::Path;
use delta_kernel::{DeltaResult, Engine};
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::storage::{insert_url_handler, EngineStore};
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use rstest::rstest;
use url::Url;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PREFIX: &str = "table/_delta_log/";
const OFFSET: &str = "table/_delta_log/00000000000000000010.json";
const FIRST: &str = "table/_delta_log/00000000000000000011.json";
const SECOND: &str = "table/_delta_log/00000000000000000012.json";
const LAST: &str = "table/_delta_log/z-other";
const GCS_CREDENTIALS: &str = r#"{
    "private_key": "", "private_key_id": "", "client_email": "", "disable_oauth": true
}"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    S3,
    Gcs,
    Azure,
}

#[derive(Clone, Copy, Debug)]
enum Constructor {
    New,
    ConvenienceBuilder,
    UrlStore,
    UrlBuilder,
    CustomHandler,
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constructors_preserve_shallow_offset_listing_and_lazy_pages(
    #[values(Provider::S3, Provider::Gcs, Provider::Azure)] provider: Provider,
    #[values(
        Constructor::New,
        Constructor::ConvenienceBuilder,
        Constructor::UrlStore,
        Constructor::UrlBuilder,
        Constructor::CustomHandler
    )]
    constructor: Constructor,
    #[values(false, true)] consume_all: bool,
) {
    let server = MockServer::start().await;
    let (offset_param, token_param) = match provider {
        Provider::S3 | Provider::Gcs => ("start-after", "continuation-token"),
        Provider::Azure => ("startFrom", "marker"),
    };
    let first_keys = match provider {
        Provider::Azure => vec![OFFSET, FIRST, SECOND],
        _ => vec![FIRST, SECOND],
    };
    Mock::given(method("GET"))
        .and(path("/bucket"))
        .and(query_param("prefix", PREFIX))
        .and(query_param("delimiter", "/"))
        .and(query_param(offset_param, OFFSET))
        .and(query_param_is_missing(token_param))
        .respond_with(list_response(provider, &first_keys, &[], Some("page-2")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/bucket"))
        .and(query_param("prefix", PREFIX))
        .and(query_param("delimiter", "/"))
        .and(query_param_is_missing(offset_param))
        .and(query_param(token_param, "page-2"))
        .respond_with(list_response(
            provider,
            &[],
            &[
                "table/_delta_log/_sidecars/",
                "table/_delta_log/_staged_commits/",
            ],
            Some("page-3"),
        ))
        .expect(u64::from(consume_all))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/bucket"))
        .and(query_param("prefix", PREFIX))
        .and(query_param("delimiter", "/"))
        .and(query_param_is_missing(offset_param))
        .and(query_param(token_param, "page-3"))
        .respond_with(list_response(provider, &[LAST], &[], None))
        .expect(u64::from(consume_all))
        .mount(&server)
        .await;

    let table_url = provider.table_url();
    let engine = build_engine(provider, constructor, &server.uri());
    let start = table_url
        .join("_delta_log/00000000000000000010.json")
        .unwrap();
    let mut files = engine.storage_handler().list_from(&start).unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
    for expected in [FIRST, SECOND] {
        let file = files.next().unwrap().unwrap();
        assert_eq!(
            file.location,
            table_url.join(&format!("/{expected}")).unwrap()
        );
    }
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    if consume_all {
        let remaining = files.collect::<DeltaResult<Vec<_>>>().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining[0].location,
            table_url.join(&format!("/{LAST}")).unwrap()
        );
    } else {
        drop(files);
    }
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        if consume_all { 3 } else { 1 }
    );
    server.verify().await;
}

impl Provider {
    fn table_url(self) -> Url {
        Url::parse(match self {
            Self::S3 => "s3://bucket/table/",
            Self::Gcs => "gs://bucket/table/",
            Self::Azure => "abfss://bucket@account.dfs.core.windows.net/table/",
        })
        .unwrap()
    }

    fn options(self, endpoint: &str) -> Vec<(&'static str, String)> {
        let endpoint_key = match self {
            Self::Gcs => "base_url",
            _ => "endpoint",
        };
        let mut options = vec![
            (endpoint_key, endpoint.to_string()),
            ("allow_http", "true".to_string()),
            ("skip_signature", "true".to_string()),
        ];
        if self == Self::Gcs {
            options.push(("service_account_key", GCS_CREDENTIALS.to_string()));
        }
        options
    }

    fn explicit_store(self, endpoint: &str) -> EngineStore {
        match self {
            Self::S3 => EngineStore::with_paginated(Arc::new(
                AmazonS3Builder::new()
                    .with_bucket_name("bucket")
                    .with_endpoint(endpoint)
                    .with_allow_http(true)
                    .with_skip_signature(true)
                    .build()
                    .unwrap(),
            )),
            Self::Gcs => EngineStore::with_paginated(Arc::new(
                GoogleCloudStorageBuilder::new()
                    .with_bucket_name("bucket")
                    .with_base_url(endpoint)
                    .with_service_account_key(GCS_CREDENTIALS)
                    .with_skip_signature(true)
                    .build()
                    .unwrap(),
            )),
            Self::Azure => EngineStore::with_paginated(Arc::new(
                MicrosoftAzureBuilder::new()
                    .with_account("account")
                    .with_container_name("bucket")
                    .with_endpoint(endpoint.to_string())
                    .with_allow_http(true)
                    .with_skip_signature(true)
                    .build()
                    .unwrap(),
            )),
        }
    }
}

fn build_engine(
    provider: Provider,
    constructor: Constructor,
    endpoint: &str,
) -> DefaultEngine<TokioBackgroundExecutor> {
    let table_url = provider.table_url();
    let options = provider.options(endpoint);
    match constructor {
        Constructor::New => DefaultEngineBuilder::new(provider.explicit_store(endpoint)).build(),
        Constructor::ConvenienceBuilder => {
            DefaultEngine::builder(provider.explicit_store(endpoint)).build()
        }
        Constructor::UrlStore => {
            DefaultEngineBuilder::new(EngineStore::from_url_opts(&table_url, options).unwrap())
                .build()
        }
        Constructor::UrlBuilder => DefaultEngineBuilder::from_url_opts(&table_url, options)
            .unwrap()
            .build(),
        Constructor::CustomHandler => {
            let scheme = format!("listing-test-{}", uuid::Uuid::new_v4());
            insert_url_handler(
                &scheme,
                Arc::new(move |url, options| {
                    assert_eq!(url.path(), table_url.path());
                    let store = EngineStore::from_url_opts(&table_url, options).unwrap();
                    Ok((store, Path::parse(url.path())?))
                }),
            )
            .unwrap();
            let url = Url::parse(&format!("{scheme}://bucket/table/")).unwrap();
            DefaultEngineBuilder::from_url_opts(&url, options)
                .unwrap()
                .build()
        }
    }
}

fn list_response(
    provider: Provider,
    keys: &[&str],
    prefixes: &[&str],
    token: Option<&str>,
) -> ResponseTemplate {
    let mut body = String::new();
    let (opening, closing, token_tag) = match provider {
        Provider::Azure => ("<EnumerationResults><Blobs>", "</Blobs>", "NextMarker"),
        _ => ("<ListBucketResult>", "", "NextContinuationToken"),
    };
    body.push_str(opening);
    for key in keys {
        body.push_str(&match provider {
            Provider::Azure => format!(
                "<Blob><Name>{key}</Name><Properties>\
                 <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>\
                 <Content-Length>1</Content-Length><Content-Type>application/json</Content-Type>\
                 </Properties></Blob>"
            ),
            _ => format!(
                "<Contents><Key>{key}</Key><Size>1</Size>\
                 <LastModified>2021-07-01T10:44:59Z</LastModified></Contents>"
            ),
        });
    }
    for prefix in prefixes {
        body.push_str(&match provider {
            Provider::Azure => format!("<BlobPrefix><Name>{prefix}</Name></BlobPrefix>"),
            _ => format!("<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"),
        });
    }
    body.push_str(closing);
    if let Some(token) = token {
        body.push_str(&format!("<{token_tag}>{token}</{token_tag}>"));
    }
    body.push_str(match provider {
        Provider::Azure => "</EnumerationResults>",
        _ => "</ListBucketResult>",
    });
    ResponseTemplate::new(200).set_body_raw(body, "application/xml")
}
