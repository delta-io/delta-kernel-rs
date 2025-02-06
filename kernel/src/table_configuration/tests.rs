use std::collections::HashMap;

use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::table_configuration::{InvalidTableConfigurationError, SupportError};
use crate::table_features::{ReaderFeatures, WriterFeatures};
use crate::table_properties::property_names::ENABLE_DELETION_VECTORS;

use super::TableConfiguration;

#[test]
fn dv_supported_not_enabled() {
    let metadata = Metadata {
            configuration: HashMap::from_iter([(
                "delta.enableChangeDataFeed".to_string(),
                "true".to_string(),
            )]),
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
    let protocol = Protocol::new(
        3,
        7,
        Some([ReaderFeatures::DeletionVectors]),
        Some([WriterFeatures::DeletionVectors]),
    );
    let table_root = Url::try_from("file:///").unwrap();
    let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
    table_config
        .is_deletion_vector_supported()
        .expect("dvs are supported in features");
    let Err(SupportError::MissingTableProperty(property)) =
        table_config.is_deletion_vector_enabled()
    else {
        panic!("deletion vector enabled should fail when property is not set");
    };
    assert_eq!(property, ENABLE_DELETION_VECTORS);
}
#[test]
fn dv_enabled() {
    let metadata = Metadata {
            configuration: HashMap::from_iter([(
                "delta.enableChangeDataFeed".to_string(),
                "true".to_string(),
            ),
            (
                "delta.enableDeletionVectors".to_string(),
                "true".to_string(),
            )]),
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
    let protocol = Protocol::new(
        3,
        7,
        Some([ReaderFeatures::DeletionVectors]),
        Some([WriterFeatures::DeletionVectors]),
    );
    let table_root = Url::try_from("file:///").unwrap();
    let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();
    assert_eq!(table_config.is_deletion_vector_supported(), Ok(()));
    assert_eq!(table_config.is_deletion_vector_enabled(), Ok(()));
}
#[ignore] // TODO (Oussama): Remove if this is no longer necessary
#[test]
fn fails_on_unsupported_feature() {
    let metadata = Metadata {
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
    let protocol = Protocol::new(
        3,
        7,
        Some([ReaderFeatures::V2Checkpoint]),
        Some([WriterFeatures::V2Checkpoint]),
    );
    let table_root = Url::try_from("file:///").unwrap();
    TableConfiguration::try_new(metadata, protocol, table_root, 0)
        .expect_err("V2 checkpoint is not supported in kernel");
}
#[test]
fn dv_not_supported() {
    let metadata = Metadata {
            configuration: HashMap::from_iter([(
                "delta.enableChangeDataFeed".to_string(),
                "true".to_string(),
            )]),
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            ..Default::default()
        };
    let protocol = Protocol::new(
        3,
        7,
        Some([ReaderFeatures::TimestampWithoutTimezone]),
        Some([
            WriterFeatures::TimestampWithoutTimezone,
            WriterFeatures::DeletionVectors,
        ]),
    );
    let table_root = Url::try_from("file:///").unwrap();
    let table_config = TableConfiguration::try_new(metadata, protocol, table_root, 0).unwrap();

    assert_eq!(
        Err(SupportError::MissingReaderFeature(
            ReaderFeatures::DeletionVectors
        )),
        table_config.is_deletion_vector_supported()
    );

    assert_eq!(
        Err(SupportError::MissingReaderFeature(
            ReaderFeatures::DeletionVectors
        )),
        table_config.is_deletion_vector_enabled()
    )
}

#[test]
fn test_validate_protocol() {
    assert_eq!(
        Err(InvalidTableConfigurationError::ReaderFeaturesNotFound),
        TableConfiguration::validate_protocol(3, 7, &None, &Some(Default::default())),
    );
    assert_eq!(
        Err(InvalidTableConfigurationError::WriterFeaturesNotFound),
        TableConfiguration::validate_protocol(3, 7, &Some(Default::default()), &None),
    );
    assert!(matches!(
        TableConfiguration::validate_protocol(3, 7, &None, &None),
        Err(InvalidTableConfigurationError::WriterFeaturesNotFound)
            | Err(InvalidTableConfigurationError::ReaderFeaturesNotFound)
    ));
}
//#[test]
//fn test_v2_checkpoint_unsupported() {
//    let protocol = Protocol {
//        min_reader_version: 3,
//        min_writer_version: 7,
//        reader_features: Some([ReaderFeatures::V2Checkpoint.into()]),
//        writer_features: Some([ReaderFeatures::V2Checkpoint.into()]),
//    };
//
//    let protocol = Protocol::try_new(
//        4,
//        7,
//        Some([ReaderFeatures::V2Checkpoint]),
//        Some([ReaderFeatures::V2Checkpoint]),
//    )
//    .unwrap();
//    assert!(protocol.ensure_read_supported().is_err());
//}
//
//#[test]
//fn test_ensure_read_supported() {
//    let protocol = Protocol {
//        min_reader_version: 3,
//        min_writer_version: 7,
//        reader_features: Some(vec![]),
//        writer_features: Some(vec![]),
//    };
//    assert!(protocol.ensure_read_supported().is_ok());
//
//    let empty_features: [String; 0] = [];
//    let protocol = Protocol::try_new(
//        3,
//        7,
//        Some([ReaderFeatures::V2Checkpoint]),
//        Some(&empty_features),
//    )
//    .unwrap();
//    assert!(protocol.ensure_read_supported().is_err());
//
//    let protocol = Protocol::try_new(
//        3,
//        7,
//        Some(&empty_features),
//        Some([WriterFeatures::V2Checkpoint]),
//    )
//    .unwrap();
//    assert!(protocol.ensure_read_supported().is_ok());
//
//    let protocol = Protocol::try_new(
//        3,
//        7,
//        Some([ReaderFeatures::V2Checkpoint]),
//        Some([WriterFeatures::V2Checkpoint]),
//    )
//    .unwrap();
//    assert!(protocol.ensure_read_supported().is_err());
//
//    let protocol = Protocol {
//        min_reader_version: 1,
//        min_writer_version: 7,
//        reader_features: None,
//        writer_features: None,
//    };
//    assert!(protocol.ensure_read_supported().is_ok());
//
//    let protocol = Protocol {
//        min_reader_version: 2,
//        min_writer_version: 7,
//        reader_features: None,
//        writer_features: None,
//    };
//    assert!(protocol.ensure_read_supported().is_ok());
//}
//
//#[test]
//fn test_ensure_write_supported() {
//    let protocol = Protocol {
//        min_reader_version: 3,
//        min_writer_version: 7,
//        reader_features: Some(vec![]),
//        writer_features: Some(vec![]),
//    };
//    assert!(protocol.ensure_write_supported().is_ok());
//
//    let protocol = Protocol::try_new(
//        3,
//        7,
//        Some([ReaderFeatures::DeletionVectors]),
//        Some([WriterFeatures::DeletionVectors]),
//    )
//    .unwrap();
//    assert!(protocol.ensure_write_supported().is_err());
//}
//
//#[test]
//fn test_ensure_supported_features() {
//    let supported_features = [
//        ReaderFeatures::ColumnMapping,
//        ReaderFeatures::DeletionVectors,
//    ]
//    .into_iter()
//    .collect();
//    let table_features = vec![ReaderFeatures::ColumnMapping.to_string()];
//    ensure_supported_features(&table_features, &supported_features).unwrap();
//
//    // test unknown features
//    let table_features = vec![ReaderFeatures::ColumnMapping.to_string(), "idk".to_string()];
//    let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
//    match error {
//        Error::Unsupported(e) if e ==
//            "Unknown ReaderFeatures [\"idk\"]. Supported ReaderFeatures are [ColumnMapping, DeletionVectors]"
//        => {},
//        Error::Unsupported(e) if e ==
//            "Unknown ReaderFeatures [\"idk\"]. Supported ReaderFeatures are [DeletionVectors, ColumnMapping]"
//        => {},
//        _ => panic!("Expected unsupported error"),
//    }
//}
