//! Builds the OCC plan.
//!
//! ```text
//! read_append:   ScanJson -> Filter(add) -> Project(part) -> Filter(predicate) -> Project -+
//! read_delete:   ScanJson -> Filter(remove) -+-> SemiJoin(read_files) -> Project ----------+
//! delete_delete:                             +-> SemiJoin(file_actions) -> Project --------+
//!                                                          UnionAll -> Aggregate(check, version)
//! ```
//!
//! The delete checks share one scan of the winners' removes. Project(part) and Filter(predicate)
//! run only for [`ReadScope::Files`] with a partition predicate. On an unpartitioned table,
//! Filter(predicate) applies to the adds without Project(part). With [`ReadScope::WholeTable`],
//! the read_delete branch has no SemiJoin.

use std::sync::LazyLock;

use strum::{EnumString, IntoStaticStr};

use super::{LoserFacts, ReadScope, VERSION, WinningCommit, scan_files};
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::expressions::{Expression, col, column_name, lit};
use crate::plans::{Plan, PlanBuilder, RelationRef};
use crate::scan::PrefixColumns;
use crate::schema::{SchemaRef, lazy_schema_ref, try_schema};
use crate::transforms::ExpressionTransform as _;
use crate::{KernelError, KernelResult, Version};

// === Output columns ===
pub(crate) const CHECK: &str = "check";

// === Input and intermediate columns ===
const PART: &str = "part";
const PATH: &str = "path";

static WINNER_ADDS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (ADD_NAME): {
        not_null (PATH): STRING,
        not_null "partitionValues": { STRING => nullable STRING },
    },
};

static WINNER_REMOVES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (REMOVE_NAME): {
        not_null (PATH): STRING,
    },
};

static VIOLATION_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    not_null (CHECK): STRING,
};

/// The winner versions that each OCC check scans.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OccBranchVersions {
    pub(crate) read_append: Vec<Version>,
    /// The versions that both `read_delete` and `delete_delete` scan.
    pub(crate) delete_checks: Vec<Version>,
}

/// Builds the OCC plan, or returns `None` when no branch has a version.
///
/// The plan returns one row for each check and version with a violation. A branch with no
/// versions is left out. `file_actions` must have a `remove.path` column.
pub(crate) fn occ_plan(
    facts: &LoserFacts,
    winners: &[WinningCommit],
    branches: &OccBranchVersions,
    file_actions: &RelationRef,
) -> KernelResult<Option<Plan>> {
    let read_append = read_append_branch(&facts.read_scope, winners, &branches.read_append)?;
    let removes = winner_removes(winners, &branches.delete_checks)?;
    let read_delete = read_delete_branch(&facts.read_scope, removes.clone())?;
    let delete_delete = delete_delete_branch(removes, file_actions)?;
    PlanBuilder::union_all([
        violations(read_append, OccCheck::ReadAppend)?,
        violations(read_delete, OccCheck::ReadDelete)?,
        violations(delete_delete, OccCheck::DeleteDelete)?,
    ])?
    // The aggregate gives one row for each (check, version). Nothing reads the count.
    .aggregate_by([column_name!(CHECK), column_name!(VERSION)], |aggs| {
        aggs.count_star()
    })?
    .build_opt()
}

/// A file-level check of the OCC plan.
///
/// The variant order is the order in which Kernel reports the checks of one version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum OccCheck {
    /// The winner adds a file that T should have read.
    ReadAppend,
    /// The winner removes a file that T read.
    ReadDelete,
    /// The winner removes a file that T also removes.
    DeleteDelete,
}

impl OccCheck {
    /// Parses a `check` value from the plan output.
    pub(crate) fn parse(value: &str) -> KernelResult<Self> {
        value
            .parse()
            .map_err(|_| KernelError::internal_error(format!("Unknown OCC check {value:?}")))
    }
}

/// Winner adds that T should have read. Every add counts, whatever its `dataChange`.
fn read_append_branch(
    read_scope: &ReadScope,
    winners: &[WinningCommit],
    versions: &[Version],
) -> KernelResult<PlanBuilder> {
    let adds = PlanBuilder::scan_json(
        scan_files(winners, versions.iter().copied())?,
        &[VERSION],
        WINNER_ADDS_SCHEMA.clone(),
    )?
    .filter(col!(ADD_NAME).is_not_null())?;
    let ReadScope::Files {
        partition_schema,
        partition_predicate: Some(partition_predicate),
        map_to_struct_options,
        ..
    } = read_scope
    else {
        return Ok(adds);
    };
    // An unpartitioned table has no partition values, so the predicate is a constant.
    if partition_schema.num_fields() == 0 {
        return adds.filter(partition_predicate.clone());
    }
    let part_schema = try_schema! {
        not_null (VERSION): LONG,
        nullable (PART): (partition_schema.as_ref().clone()),
    }?;
    let part = Expression::struct_from([
        col!(VERSION),
        Expression::map_to_struct(
            col!(ADD_NAME, "partitionValues"),
            map_to_struct_options.clone(),
        ),
    ]);
    let mut prefixer = PrefixColumns::new(column_name!(PART));
    let predicate = prefixer.transform_pred(partition_predicate).into_owned();
    adds.project(part, part_schema)?.filter(predicate)
}

/// Winner removes of files that T read. Every remove counts, including `dataChange=false`.
fn read_delete_branch(read_scope: &ReadScope, removes: PlanBuilder) -> KernelResult<PlanBuilder> {
    match read_scope {
        // T read every file, so any winner remove fails T.
        ReadScope::WholeTable => Ok(removes),
        ReadScope::Files { read_files, .. } => removes.semi_join(
            PlanBuilder::relation_source(read_files.clone()),
            [column_name!(REMOVE_NAME, PATH)],
            [column_name!(PATH)],
        ),
    }
}

/// Winner removes of paths that T also removes. Every remove counts, including
/// `dataChange=false`.
fn delete_delete_branch(
    removes: PlanBuilder,
    file_actions: &RelationRef,
) -> KernelResult<PlanBuilder> {
    let loser_removes = PlanBuilder::relation_source(file_actions.clone())
        .filter(col!(REMOVE_NAME).is_not_null())?;
    removes.semi_join(
        loser_removes,
        [column_name!(REMOVE_NAME, PATH)],
        [column_name!(REMOVE_NAME, PATH)],
    )
}

fn winner_removes(winners: &[WinningCommit], versions: &[Version]) -> KernelResult<PlanBuilder> {
    PlanBuilder::scan_json(
        scan_files(winners, versions.iter().copied())?,
        &[VERSION],
        WINNER_REMOVES_SCHEMA.clone(),
    )?
    .filter(col!(REMOVE_NAME).is_not_null())
}

fn violations(branch: PlanBuilder, check: OccCheck) -> KernelResult<PlanBuilder> {
    let name: &'static str = check.into();
    branch.project(
        Expression::struct_from([col!(VERSION), lit(name)]),
        VIOLATION_SCHEMA.clone(),
    )
}
