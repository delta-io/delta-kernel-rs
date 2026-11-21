use std::collections::HashSet;

use super::TransactionState;
use crate::actions::{DomainMetadata, INTERNAL_DOMAIN_PREFIX};
use crate::error::KernelError;
use crate::log_segment::DomainMetadataMap;
use crate::row_tracking::{
    ROW_TRACKING_DOMAIN_NAME, ROW_TRACKING_INITIAL_HIGH_WATER_MARK, RowTrackingDomainMetadata,
    parse_row_tracking_high_water_mark,
};
use crate::table_features::TableFeature;
use crate::{Engine, KernelResult};

impl TransactionState {
    /// Validate domain metadata operations for both create-table and existing-table transactions.
    ///
    /// Enforces the following rules:
    /// - DomainMetadata feature must be supported if any domain operations are present
    /// - System domains must correspond to a known feature
    /// - User domains cannot use the delta.* prefix (system-reserved)
    /// - Domain removals are not allowed in create-table transactions
    /// - No duplicate domains within a single transaction (across user and system operations)
    pub(super) fn validate_domain_metadata_operations(&self) -> KernelResult<()> {
        // Feature validation (applies to all transactions with domain operations)
        let has_domain_ops = !self.system_domain_metadata_additions.is_empty()
            || self.provided_row_tracking_high_water_mark.is_some()
            || !self.user_domain_metadata_additions.is_empty()
            || !self.user_domain_removals.is_empty();

        // Early return if no domain operations to validate
        if !has_domain_ops {
            return Ok(());
        }

        if !self
            .effective_table_config
            .is_feature_supported(&TableFeature::DomainMetadata)
        {
            return Err(KernelError::unsupported(
                "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature",
            ));
        }

        let is_create = self.is_create_table();
        let mut seen_domains = HashSet::with_capacity(
            self.system_domain_metadata_additions.len()
                + usize::from(self.provided_row_tracking_high_water_mark.is_some())
                + self.user_domain_metadata_additions.len()
                + self.user_domain_removals.len(),
        );

        // Validate system-domain additions produced by create-table transforms.
        for dm in &self.system_domain_metadata_additions {
            let domain = dm.domain();

            // Validate the system domain corresponds to a known feature
            self.validate_system_domain_feature(domain)?;

            // Check for duplicates
            if !seen_domains.insert(domain) {
                return Err(KernelError::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        // Validate the dedicated existing-table row-tracking operation with the other domains.
        if self.provided_row_tracking_high_water_mark.is_some() {
            self.validate_system_domain_feature(ROW_TRACKING_DOMAIN_NAME)?;
            if !seen_domains.insert(ROW_TRACKING_DOMAIN_NAME) {
                return Err(KernelError::generic(format!(
                    "Metadata for domain {ROW_TRACKING_DOMAIN_NAME} already specified in this transaction"
                )));
            }
        }

        // Validate USER domain additions (via with_domain_metadata API)
        for dm in &self.user_domain_metadata_additions {
            let domain = dm.domain();

            // Users cannot add system domains via the public API
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(KernelError::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates (spans both system and user domains)
            if !seen_domains.insert(domain) {
                return Err(KernelError::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        // No removals allowed for create-table.
        // Note: CreateTableTransaction does not expose with_domain_metadata_removed(),
        // so this is a defensive check. See #1768.
        if is_create && !self.user_domain_removals.is_empty() {
            return Err(KernelError::unsupported(
                "Domain metadata removals are not supported in create-table transactions",
            ));
        }

        // Validate domain removals (for non-create-table)
        for domain in &self.user_domain_removals {
            // Cannot remove system domains
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(KernelError::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates
            if !seen_domains.insert(domain.as_str()) {
                return Err(KernelError::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        Ok(())
    }

    /// Validate that a system domain corresponds to a known feature and that the feature is
    /// supported.
    ///
    /// This prevents arbitrary `delta.*` domains from entering a transaction through internal
    /// transforms or dedicated system-domain operations. Each known system domain must have its
    /// corresponding feature enabled in the protocol.
    fn validate_system_domain_feature(&self, domain: &str) -> KernelResult<()> {
        let table_config = &self.effective_table_config;

        // Map domain to its required feature
        let required_feature = match domain {
            ROW_TRACKING_DOMAIN_NAME => Some(TableFeature::RowTracking),
            // Will be changed to a constant in a follow up clustering create table feature PR
            "delta.clustering" => Some(TableFeature::ClusteredTable),
            _ => {
                return Err(KernelError::generic(format!(
                    "Unknown system domain '{domain}'. Only known system domains are allowed."
                )));
            }
        };

        // If the domain requires a feature, validate it's supported
        if let Some(feature) = required_feature
            && !table_config.is_feature_supported(&feature)
        {
            return Err(KernelError::generic(format!(
                "System domain '{domain}' requires the '{feature}' feature to be enabled"
            )));
        }

        Ok(())
    }

    /// Generates domain metadata actions (assumes domain metadata operations have already
    /// been validated)
    ///
    /// Incorporates the calculated row-tracking high-water mark and returns the domain changes
    /// for action encoding, commit metadata, and the post-commit
    /// [`CrcDelta`](crate::crc::CrcDelta). Returns an error if a provided high-water mark is
    /// below the calculated value or if domain state cannot be read.
    ///
    /// This function may perform an expensive log replay operation if there are any domain
    /// removals. The log replay is required to fetch the previous configuration value for the
    /// domain to preserve in removal tombstones as mandated by the Delta spec.
    pub(super) fn generate_domain_metadata_actions(
        &self,
        engine: &dyn Engine,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
    ) -> KernelResult<Vec<DomainMetadata>> {
        self.generate_domain_metadata_actions_with(row_tracking_high_watermark, |domains| {
            self.read_snapshot()?
                .reader()
                .get_domain_metadatas_internal(engine, Some(domains))
        })
    }

    /// Generates domain actions using the supplied lookup for existing domain state.
    /// Only domains needed for removals or high-water-mark validation are requested.
    pub(super) fn generate_domain_metadata_actions_with(
        &self,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
        load_domains: impl FnOnce(&HashSet<&str>) -> KernelResult<DomainMetadataMap>,
    ) -> KernelResult<Vec<DomainMetadata>> {
        let mut domains: HashSet<&str> = self
            .user_domain_removals
            .iter()
            .map(String::as_str)
            .collect();
        if self.provided_row_tracking_high_water_mark.is_some()
            && row_tracking_high_watermark.is_none()
        {
            domains.insert(ROW_TRACKING_DOMAIN_NAME);
        }
        let existing_domains = if domains.is_empty() {
            DomainMetadataMap::new()
        } else {
            load_domains(&domains)?
        };
        // Removal tombstones preserve the previous configuration. Absent domains are no-ops.
        let removal_actions = self.user_domain_removals.iter().filter_map(|domain| {
            existing_domains.get(domain).map(|existing| {
                DomainMetadata::remove(domain.clone(), existing.configuration().to_owned())
            })
        });

        let row_tracking_high_watermark = if let Some(provided) =
            self.provided_row_tracking_high_water_mark
        {
            let calculated = match row_tracking_high_watermark {
                Some(metadata) => metadata.high_water_mark(),
                None => existing_domains
                    .get(ROW_TRACKING_DOMAIN_NAME)
                    .map(|metadata| parse_row_tracking_high_water_mark(metadata.configuration()))
                    .transpose()?
                    .unwrap_or(ROW_TRACKING_INITIAL_HIGH_WATER_MARK),
            };
            if provided < calculated {
                return Err(KernelError::generic(format!(
                    "Provided row-tracking high-water mark {provided} cannot be less than the \
                         calculated value {calculated}",
                )));
            }
            Some(RowTrackingDomainMetadata::new(provided))
        } else {
            row_tracking_high_watermark
        };

        // Generate the single row-tracking domain action, if any.
        let row_tracking_domain_action = row_tracking_high_watermark
            .map(DomainMetadata::try_from)
            .transpose()?
            .into_iter();

        // Chain all domain actions: system domains, row tracking, user domains, removals
        let dm_actions_vec: Vec<DomainMetadata> = self
            .system_domain_metadata_additions
            .iter()
            .cloned()
            .chain(row_tracking_domain_action)
            .chain(self.user_domain_metadata_additions.iter().cloned())
            .chain(removal_actions)
            .collect();

        Ok(dm_actions_vec)
    }
}

/// Returns the unique domains, or the caller's error for the first duplicate.
pub(super) fn validate_unique_domains<'a>(
    domains: impl IntoIterator<Item = &'a str>,
    duplicate_error: impl FnOnce(&str) -> KernelError,
) -> KernelResult<HashSet<&'a str>> {
    let domains = domains.into_iter();
    let mut seen = HashSet::with_capacity(domains.size_hint().0);
    for domain in domains {
        if !seen.insert(domain) {
            return Err(duplicate_error(domain));
        }
    }
    Ok(seen)
}
