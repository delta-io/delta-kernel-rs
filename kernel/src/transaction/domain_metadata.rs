use std::collections::HashSet;

use super::Transaction;
use crate::actions::{DomainMetadata, INTERNAL_DOMAIN_PREFIX};
use crate::coroutine::Channel;
use crate::error::Error;
use crate::row_tracking::{RowTrackingDomainMetadata, ROW_TRACKING_DOMAIN_NAME};
use crate::table_features::TableFeature;
use crate::{DeltaResult, Snapshot};

impl<S> Transaction<S> {
    /// Validate domain metadata operations for both create-table and existing-table transactions.
    ///
    /// Enforces the following rules:
    /// - DomainMetadata feature must be supported if any domain operations are present
    /// - System domains must correspond to a known feature
    /// - User domains cannot use the delta.* prefix (system-reserved)
    /// - Domain removals are not allowed in create-table transactions
    /// - No duplicate domains within a single transaction (across user and system operations)
    pub(super) fn validate_domain_metadata_operations(&self) -> DeltaResult<()> {
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
            return Err(Error::unsupported(
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
                return Err(Error::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        // Validate the dedicated existing-table row-tracking operation with the other domains.
        if self.provided_row_tracking_high_water_mark.is_some() {
            self.validate_system_domain_feature(ROW_TRACKING_DOMAIN_NAME)?;
            if !seen_domains.insert(ROW_TRACKING_DOMAIN_NAME) {
                return Err(Error::generic(format!(
                    "Metadata for domain {ROW_TRACKING_DOMAIN_NAME} already specified in this transaction"
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
                    "Metadata for domain {domain} already specified in this transaction"
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
    fn validate_system_domain_feature(&self, domain: &str) -> DeltaResult<()> {
        let table_config = &self.effective_table_config;

        // Map domain to its required feature
        let required_feature = match domain {
            ROW_TRACKING_DOMAIN_NAME => Some(TableFeature::RowTracking),
            // Will be changed to a constant in a follow up clustering create table feature PR
            "delta.clustering" => Some(TableFeature::ClusteredTable),
            _ => {
                return Err(Error::generic(format!(
                    "Unknown system domain '{domain}'. Only known system domains are allowed."
                )));
            }
        };

        // If the domain requires a feature, validate it's supported
        if let Some(feature) = required_feature {
            if !table_config.is_feature_supported(&feature) {
                return Err(Error::generic(format!(
                    "System domain '{domain}' requires the '{feature}' feature to be enabled"
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
    pub(super) async fn generate_user_domain_removal_actions(
        snapshot: &Snapshot,
        user_domain_removals: &[String],
        channel: &Channel,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        if user_domain_removals.is_empty() {
            return Ok(vec![]);
        }

        // Scan log to fetch existing configurations for tombstones.
        // Pass the specific set of domains to remove so that log replay can terminate early
        // once all target domains have been found, instead of replaying the entire log.
        let domains: HashSet<&str> = user_domain_removals.iter().map(String::as_str).collect();
        let existing_domains = snapshot
            .get_domain_metadatas_internal(channel, Some(&domains))
            .await?;

        // Create removal tombstones with pre-image configurations
        Ok(user_domain_removals
            .iter()
            .filter_map(|domain| {
                // If domain doesn't exist in the log, this is a no-op (filter it out)
                existing_domains.get(domain).map(|existing| {
                    DomainMetadata::remove(domain.clone(), existing.configuration().to_owned())
                })
            })
            .collect())
    }

    /// Generate user and system domain metadata actions.
    ///
    /// This function may perform an expensive log replay operation if there are any domain
    /// removals. The log replay is required to fetch the previous configuration value for the
    /// domain to preserve in removal tombstones as mandated by the Delta spec.
    pub(super) fn generate_domain_metadata_actions(
        &self,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
        removal_actions: Vec<DomainMetadata>,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        let is_create = self.is_create_table();

        if is_create {
            // user_domain_removals already validated above, but be explicit
            debug_assert!(self.user_domain_removals.is_empty());
        }

        let row_tracking_high_watermark =
            if let Some(provided) = self.provided_row_tracking_high_water_mark {
                let calculated = row_tracking_high_watermark
                    .as_ref()
                    .map(|metadata| metadata.high_water_mark())
                    .unwrap_or(RowTrackingDomainMetadata::MISSING_ROW_ID_HIGH_WATERMARK);
                if provided < calculated {
                    return Err(Error::generic(format!(
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
        Ok(self
            .system_domain_metadata_additions
            .iter()
            .cloned()
            .chain(row_tracking_domain_action)
            .chain(self.user_domain_metadata_additions.iter().cloned())
            .chain(removal_actions)
            .collect())
    }
}
