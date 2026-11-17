//! Builds the OCC plan.
//!
//! ```text
//! read_append:   ScanJson -> Filter(add) -> Project(part) -> Filter(predicate) -> Project -+
//! read_delete:   ScanJson -> Filter(remove) -> SemiJoin(read_files) -> Project -------------+
//! delete_delete: ScanJson -> Filter(remove) -> SemiJoin(file_actions) -> Project -----------+
//!                                                          UnionAll -> Aggregate(check, version)
//! ```
//!
//! The partition steps run only for [`ReadScope::Files`] with a partition predicate. With
//! [`ReadScope::WholeTable`], the read_delete branch has no SemiJoin.

use std::sync::LazyLock;

use strum::{EnumString, IntoStaticStr};

use super::{LoserFacts, ReadScope, VERSION, WinningCommit};
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::expressions::{Expression, col, column_name, lit};
use crate::plans::ir::nodes::ScanFile;
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

/// The winner versions that each OCC branch checks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OccBranchVersions {
    pub(crate) read_append: Vec<Version>,
    pub(crate) read_delete: Vec<Version>,
    pub(crate) delete_delete: Vec<Version>,
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
    let read_delete = read_delete_branch(&facts.read_scope, winners, &branches.read_delete)?;
    let delete_delete = delete_delete_branch(winners, &branches.delete_delete, file_actions)?;
    PlanBuilder::union_all([
        violations(read_append, OccCheck::ReadAppend)?,
        violations(read_delete, OccCheck::ReadDelete)?,
        violations(delete_delete, OccCheck::DeleteDelete)?,
    ])?
    // The count_star exists to group the rows. Nothing reads the count.
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
        scan_files(winners, versions)?,
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
    if partition_schema.fields().len() == 0 {
        return Ok(adds);
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
fn read_delete_branch(
    read_scope: &ReadScope,
    winners: &[WinningCommit],
    versions: &[Version],
) -> KernelResult<PlanBuilder> {
    let removes = winner_removes(winners, versions)?;
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
    winners: &[WinningCommit],
    versions: &[Version],
    file_actions: &RelationRef,
) -> KernelResult<PlanBuilder> {
    let loser_removes = PlanBuilder::relation_source(file_actions.clone())
        .filter(col!(REMOVE_NAME).is_not_null())?;
    winner_removes(winners, versions)?.semi_join(
        loser_removes,
        [column_name!(REMOVE_NAME, PATH)],
        [column_name!(REMOVE_NAME, PATH)],
    )
}

fn winner_removes(winners: &[WinningCommit], versions: &[Version]) -> KernelResult<PlanBuilder> {
    PlanBuilder::scan_json(
        scan_files(winners, versions)?,
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

/// The commit file of each version in `versions`. `winners` must be in ascending version order.
fn scan_files(winners: &[WinningCommit], versions: &[Version]) -> KernelResult<Vec<ScanFile>> {
    versions
        .iter()
        .map(|&version| {
            winners
                .binary_search_by_key(&version, |winner| winner.version)
                .ok()
                .and_then(|index| winners.get(index))
                .ok_or_else(|| {
                    KernelError::internal_error(format!("No winning commit at version {version}"))
                })?
                .scan_file()
        })
        .collect()
}
