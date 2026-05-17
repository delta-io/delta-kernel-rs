//! The [`Plan`] envelope.
//!
//! A [`Plan`] pairs a transforms-only plan tree with the sink describing how
//! its output is consumed. Every complete plan has exactly one sink, and the
//! sink lives on the envelope — never inside the tree.

use super::declarative::DeclarativePlanNode;
use super::nodes::{RelationHandle, SinkType};

/// Complete plan: a transforms-only [`DeclarativePlanNode`] tree terminated
/// by a [`SinkType`].
#[derive(Debug, Clone)]
pub struct Plan {
    pub root: DeclarativePlanNode,
    pub sink: SinkType,
}

impl Plan {
    /// Assemble a plan from a root subtree and a sink.
    pub fn new(root: DeclarativePlanNode, sink: SinkType) -> Self {
        Self { root, sink }
    }
}

/// Terminal value of read-style state machines.
///
/// Names the relation the caller will read after executing [`Self::plans`]. The
/// SM body returns this value; the engine first runs every plan in `plans`
/// (each terminating in a [`SinkType::Relation`], [`SinkType::Load`], or
/// [`SinkType::Consume`] sink) and then materializes the row stream
/// referenced by `result_relation`.
#[derive(Debug, Clone)]
pub struct ResultPlan {
    /// Plans that must execute before the result relation is readable. Each
    /// plan publishes its output through its sink; downstream plans in the
    /// same vector may reference earlier relations through
    /// [`crate::plans::ir::DeclarativePlanNode::RelationRef`].
    pub plans: Vec<Plan>,
    /// Final relation the caller reads to obtain the SM's row stream.
    pub result_relation: RelationHandle,
}

impl ResultPlan {
    /// Build a result plan from its constituent plan vector and the handle the
    /// caller reads at the end.
    pub fn new(plans: Vec<Plan>, result_relation: RelationHandle) -> Self {
        Self {
            plans,
            result_relation,
        }
    }
}
