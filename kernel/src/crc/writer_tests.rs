//! Integration tests for CRC writing and roundtrip verification.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use url::Url;

    use crate::actions::{DomainMetadata, Metadata, Protocol};
    use crate::crc::reader::try_read_crc_file;
    use crate::crc::writer::{compute_crc_for_create_table, write_crc_file};
    use crate::crc::Crc;
    use crate::engine::default::DefaultEngineBuilder;
    use crate::path::ParsedLogPath;
    use crate::transaction::PostCommitContext;
    use crate::Engine;

    fn test_engine() -> impl Engine {
        DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()
    }

    fn test_url() -> Url {
        Url::parse("memory:///test_table/").unwrap()
    }

    fn make_base_crc() -> Crc {
        Crc {
            table_size_bytes: 0,
            num_files: 0,
            num_metadata: 1,
            num_protocol: 1,
            metadata: Metadata::default(),
            protocol: Protocol::default(),
            txn_id: None,
            in_commit_timestamp_opt: None,
            set_transactions: None,
            domain_metadata: None,
            file_size_histogram: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        }
    }

    #[test]
    fn test_compute_crc_for_create_table() {
        let ctx = PostCommitContext {
            num_add_files: 5,
            total_add_file_size_bytes: 1000,
            num_remove_files: 0,
            total_remove_file_size_bytes: 0,
            domain_metadata_actions: vec![],
            in_commit_timestamp: Some(1234567890),
            operation: Some("CREATE TABLE".to_string()),
        };

        let mut crc = make_base_crc();
        compute_crc_for_create_table(&ctx, &mut crc);

        assert_eq!(crc.table_size_bytes, 1000);
        assert_eq!(crc.num_files, 5);
        assert_eq!(crc.in_commit_timestamp_opt, Some(1234567890));
    }

    #[test]
    fn test_compute_crc_post_commit() {
        use crate::crc::writer::compute_crc_post_commit;

        let mut old_crc = make_base_crc();
        old_crc.table_size_bytes = 5000;
        old_crc.num_files = 10;
        old_crc.domain_metadata = Some(vec![DomainMetadata::new(
            "existing_domain".to_string(),
            "old_config".to_string(),
        )]);

        let ctx = PostCommitContext {
            num_add_files: 3,
            total_add_file_size_bytes: 600,
            num_remove_files: 1,
            total_remove_file_size_bytes: 200,
            domain_metadata_actions: vec![DomainMetadata::new(
                "new_domain".to_string(),
                "new_config".to_string(),
            )],
            in_commit_timestamp: Some(9999),
            operation: Some("WRITE".to_string()),
        };

        let mut crc = make_base_crc();
        compute_crc_post_commit(&old_crc, &ctx, &mut crc);

        // 5000 + 600 - 200 = 5400
        assert_eq!(crc.table_size_bytes, 5400);
        // 10 + 3 - 1 = 12
        assert_eq!(crc.num_files, 12);
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));

        // Domain metadata: old + new
        let dm = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(dm.len(), 2);
        let domains: Vec<&str> = dm.iter().map(|d| d.domain()).collect();
        assert!(domains.contains(&"existing_domain"));
        assert!(domains.contains(&"new_domain"));
    }

    #[test]
    fn test_write_and_read_crc_roundtrip() {
        let engine = test_engine();
        let table_root = test_url();

        // Create a CRC with some data
        let mut crc = make_base_crc();
        crc.table_size_bytes = 42;
        crc.num_files = 7;
        crc.in_commit_timestamp_opt = Some(1694758257000);
        crc.domain_metadata = Some(vec![DomainMetadata::new(
            "delta.rowTracking".to_string(),
            r#"{"rowIdHighWaterMark":100}"#.to_string(),
        )]);

        // Write CRC at version 0
        write_crc_file(&engine, &table_root, &crc, 0).unwrap();

        // Read it back
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);
        let read_crc = try_read_crc_file(&engine, &crc_path).unwrap();

        // Verify required fields
        assert_eq!(read_crc.table_size_bytes, 42);
        assert_eq!(read_crc.num_files, 7);
        assert_eq!(read_crc.num_metadata, 1);
        assert_eq!(read_crc.num_protocol, 1);
        assert_eq!(read_crc.in_commit_timestamp_opt, Some(1694758257000));

        // Verify P&M roundtrip (default values)
        assert_eq!(read_crc.metadata, Metadata::default());
        assert_eq!(read_crc.protocol, Protocol::default());

        // Verify domain metadata roundtrip
        let dm = read_crc.domain_metadata.as_ref().unwrap();
        assert_eq!(dm.len(), 1);
        assert_eq!(dm[0].domain(), "delta.rowTracking");
        assert_eq!(dm[0].configuration(), r#"{"rowIdHighWaterMark":100}"#);
    }

    #[test]
    fn test_write_crc_idempotent() {
        let engine = test_engine();
        let table_root = test_url();

        let crc = make_base_crc();

        // Write CRC at version 0
        write_crc_file(&engine, &table_root, &crc, 0).unwrap();

        // Writing again should succeed (FileAlreadyExists is OK)
        let result = write_crc_file(&engine, &table_root, &crc, 0);
        assert!(result.is_ok(), "Second write should be idempotent");
    }

    #[test]
    fn test_domain_metadata_merge_with_tombstone() {
        use crate::crc::writer::compute_crc_post_commit;

        let mut old_crc = make_base_crc();
        old_crc.domain_metadata = Some(vec![
            DomainMetadata::new("keep_me".to_string(), "config1".to_string()),
            DomainMetadata::new("remove_me".to_string(), "config2".to_string()),
        ]);

        let ctx = PostCommitContext {
            domain_metadata_actions: vec![
                DomainMetadata::remove("remove_me".to_string(), String::new()),
                DomainMetadata::new("new_one".to_string(), "config3".to_string()),
            ],
            ..Default::default()
        };

        let mut crc = make_base_crc();
        compute_crc_post_commit(&old_crc, &ctx, &mut crc);

        let dm = crc.domain_metadata.as_ref().unwrap();
        let domains: Vec<&str> = dm.iter().map(|d| d.domain()).collect();
        assert!(
            domains.contains(&"keep_me"),
            "should keep non-removed domain"
        );
        assert!(
            !domains.contains(&"remove_me"),
            "should remove tombstoned domain"
        );
        assert!(domains.contains(&"new_one"), "should add new domain");
        assert_eq!(dm.len(), 2);
    }
}
