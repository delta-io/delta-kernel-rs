use crate::PlanBuilder;
use crate::actions::ActionKind;
use crate::schema::SchemaRef;

/// Requested or available representations of an action relation.
///
/// Source builders return actual read options, which can differ from requested options: a commit
/// may supply JSON as fallback for requested parsed stats. Availability describes nullable columns;
/// parsing fills row-level fallbacks. Parsed partition types always come from the snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActionOptions {
    pub(crate) actions: Vec<ActionKind>,
    pub(crate) json_stats: bool,
    pub(crate) parsed_stats: Option<SchemaRef>,
    pub(crate) parsed_partition_values: bool,
}

/// An action relation and its available representations.
pub(crate) struct ActionPlan {
    pub(crate) plan: PlanBuilder,
    pub(crate) options: ActionOptions,
}
