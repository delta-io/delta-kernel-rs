use std::collections::HashSet;

use crate::actions::{get_log_domain_metadata_schema, DomainMetadata, INTERNAL_DOMAIN_PREFIX};
use crate::error::Error;
use crate::row_tracking::{RowTrackingDomainMetadata, ROW_TRACKING_DOMAIN_NAME};
use crate::table_features::TableFeature;
use crate::{DeltaResult, Engine, IntoEngineData};

use super::{EngineDataResultIterator, Transaction};

impl<S> Transaction<S> {
    /// Validate domain metadata operations for both create-table and existing-table transactions.
    ///
    /// Enforces the following rules:
    /// - DomainMetadata feature must be supported if any domain operations are present
    /// - System domains (in system_domain_metadata_additions) must correspond to a known feature
    /// - User domains cannot use the delta.* prefix (system-reserved)
    /// - Domain removals are not allowed in create-table transactions
    /// - No duplicate domains within a single transaction (across both user and system)
    pub(super) fn validate_domain_metadata_operations(&self) -> DeltaResult<()> {
        // Feature validation (applies to all transactions with domain operations)
        let has_domain_ops = !self.system_domain_metadata_additions.is_empty()
            || !self.user_domain_metadata_additions.is_empty()
            || !self.user_domain_removals.is_empty();

        // Early return if no domain operations to validate
        if !has_domain_ops {
            return Ok(());
        }

        if !self
            .read_snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::DomainMetadata)
        {
            return Err(Error::unsupported(
                "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature",
            ));
        }

        let is_create = self.is_create_table();
        let mut seen_domains = HashSet::with_capacity(
            self.system_domain_metadata_additions.len()
                + self.user_domain_metadata_additions.len()
                + self.user_domain_removals.len(),
        );

        // Validate SYSTEM domain additions (from transforms, e.g., clustering)
        // System domains are only populated during create-table
        for dm in &self.system_domain_metadata_additions {
            let domain = dm.domain();

            // Validate the system domain corresponds to a known feature
            self.validate_system_domain_feature(domain)?;

            // Check for duplicates
            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        // Validate USER domain additions (via with_domain_metadata API)
        for dm in &self.user_domain_metadata_additions {
            let domain = dm.domain();

            // Users cannot add system domains via the public API
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates (spans both system and user domains)
            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        // No removals allowed for create-table.
        // Note: CreateTableTransaction does not expose with_domain_metadata_removed(),
        // so this is a defensive check. See #1768.
        if is_create && !self.user_domain_removals.is_empty() {
            return Err(Error::unsupported(
                "Domain metadata removals are not supported in create-table transactions",
            ));
        }

        // Validate domain removals (for non-create-table)
        for domain in &self.user_domain_removals {
            // Cannot remove system domains
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates
            if !seen_domains.insert(domain.as_str()) {
                return Err(Error::generic(format!(
                    "Metadata for domain {} already specified in this transaction",
                    domain
                )));
            }
        }

        Ok(())
    }

    /// Validate that a system domain corresponds to a known feature and that the feature is supported.
    ///
    /// This prevents arbitrary `delta.*` domains from being added during table creation.
    /// Each known system domain must have its corresponding feature enabled in the protocol.
    fn validate_system_domain_feature(&self, domain: &str) -> DeltaResult<()> {
        let table_config = self.read_snapshot.table_configuration();

        // Map domain to its required feature
        let required_feature = match domain {
            ROW_TRACKING_DOMAIN_NAME => Some(TableFeature::RowTracking),
            // Will be changed to a constant in a follow up clustering create table feature PR
            "delta.clustering" => Some(TableFeature::ClusteredTable),
            _ => {
                return Err(Error::generic(format!(
                    "Unknown system domain '{}'. Only known system domains are allowed.",
                    domain
                )));
            }
        };

        // If the domain requires a feature, validate it's supported
        if let Some(feature) = required_feature {
            if !table_config.is_feature_supported(&feature) {
                return Err(Error::generic(format!(
                    "System domain '{}' requires the '{}' feature to be enabled",
                    domain, feature
                )));
            }
        }

        Ok(())
    }

    /// Generate removal actions for user domain metadata by scanning the log.
    ///
    /// This performs an expensive log replay operation to fetch the previous configuration
    /// value for each domain being removed, as required by the Delta spec for tombstones.
    /// Returns an empty vector if there are no domain removals.
    pub(super) fn generate_user_domain_removal_actions(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        if self.user_domain_removals.is_empty() {
            return Ok(vec![]);
        }

        // Scan log to fetch existing configurations for tombstones.
        // Pass the specific set of domains to remove so that log replay can terminate early
        // once all target domains have been found, instead of replaying the entire log.
        let domains: HashSet<&str> = self
            .user_domain_removals
            .iter()
            .map(String::as_str)
            .collect();
        let existing_domains = self
            .read_snapshot
            .log_segment()
            .scan_domain_metadatas(Some(&domains), engine)?;

        // Create removal tombstones with pre-image configurations
        Ok(self
            .user_domain_removals
            .iter()
            .filter_map(|domain| {
                // If domain doesn't exist in the log, this is a no-op (filter it out)
                existing_domains.get(domain).map(|existing| {
                    DomainMetadata::remove(domain.clone(), existing.configuration().to_owned())
                })
            })
            .collect())
    }

    /// Generate domain metadata actions with validation. Handle both user and system domains.
    ///
    /// This function may perform an expensive log replay operation if there are any domain removals.
    /// The log replay is required to fetch the previous configuration value for the domain to preserve
    /// in removal tombstones as mandated by the Delta spec.
    pub(super) fn generate_domain_metadata_actions<'a>(
        &'a self,
        engine: &'a dyn Engine,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
    ) -> DeltaResult<EngineDataResultIterator<'a>> {
        let is_create = self.is_create_table();

        // Validate domain operations (includes feature validation)
        self.validate_domain_metadata_operations()?;

        // TODO(sanuj) Create-table must not have row tracking or removals
        // Defensive. Needs to be updated when row tracking support is added.
        if is_create {
            if row_tracking_high_watermark.is_some() {
                return Err(Error::internal_error(
                    "CREATE TABLE cannot have row tracking domain metadata",
                ));
            }
            // user_domain_removals already validated above, but be explicit
            debug_assert!(self.user_domain_removals.is_empty());
        }

        // Generate removal actions (empty for create-table due to validation above)
        let removal_actions = self.generate_user_domain_removal_actions(engine)?;

        // Generate row tracking domain action (None for create-table)
        let row_tracking_domain_action = row_tracking_high_watermark
            .map(DomainMetadata::try_from)
            .transpose()?
            .into_iter();

        // Chain all domain actions and convert to EngineData
        // System domains first, then row tracking, then user domains, then removals
        Ok(Box::new(
            self.system_domain_metadata_additions
                .clone()
                .into_iter()
                .chain(row_tracking_domain_action)
                .chain(self.user_domain_metadata_additions.clone())
                .chain(removal_actions)
                .map(|dm| dm.into_engine_data(get_log_domain_metadata_schema().clone(), engine)),
        ))
    }
}
