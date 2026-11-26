use rustc_abi::ExternAbi;
use rustc_ast::BindingMode;
use rustc_errors::DiagDecorator;
use rustc_hir::def::Res;
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::intravisit::{self, FnKind, Visitor};
use rustc_hir::{Body, Constness, Expr, ExprKind, FnDecl, HirId, Node, PatKind, QPath};
use rustc_lint::{LateContext, LateLintPass, LintContext, LintStore};
use rustc_middle::hir::nested_filter;
use rustc_middle::ty::{self, TyCtxt};
use rustc_session::{declare_lint, declare_lint_pass};
use rustc_span::hygiene::DesugaringKind;
use rustc_span::{Span, sym};

use crate::{called_function, is_crate_item, is_kernel_item, is_kernel_method};

declare_lint! {
    /// Suggest accepting any owned iterator when a vector is only iterated once.
    pub KERNEL_VEC_PARAMETER_INTO_ITERATOR,
    Warn,
    "consider impl IntoIterator for a Vec parameter used only by one iterator consumer"
}

declare_lint! {
    /// Suggest forwarding conversion bounds instead of requiring a concrete DataType.
    pub KERNEL_DATA_TYPE_PARAMETER_INTO,
    Warn,
    "consider impl Into<DataType> for a parameter only forwarded to a conversion constructor"
}

declare_lint_pass!(KernelParameters => [
    KERNEL_VEC_PARAMETER_INTO_ITERATOR,
    KERNEL_DATA_TYPE_PARAMETER_INTO,
]);

pub(super) fn register(store: &mut LintStore) {
    store.register_lints(&[
        KERNEL_VEC_PARAMETER_INTO_ITERATOR,
        KERNEL_DATA_TYPE_PARAMETER_INTO,
    ]);
    store.register_late_lint_pass(Box::new(|_| Box::new(KernelParameters)));
}

impl<'tcx> LateLintPass<'tcx> for KernelParameters {
    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        kind: FnKind<'tcx>,
        decl: &'tcx FnDecl<'tcx>,
        body: &'tcx Body<'tcx>,
        span: Span,
        def_id: LocalDefId,
    ) {
        let Some(header) = kind.header() else { return };
        let def_id = def_id.to_def_id();
        if span.from_expansion()
            || header.abi != ExternAbi::Rust
            || matches!(header.constness, Constness::Const { .. })
            || header.is_async()
            || cx.tcx.trait_of_assoc(def_id).is_some()
            || cx.tcx.trait_impl_of_assoc(def_id).is_some()
            || cx.tcx.codegen_fn_attrs(def_id).contains_extern_indicator()
        {
            return;
        }

        for (param, syntax_ty) in body.params.iter().zip(decl.inputs) {
            let PatKind::Binding(BindingMode::NONE, binding, name, None) = param.pat.kind else {
                continue;
            };
            if name.name.as_str() == "self" {
                continue;
            }
            let ty = cx.typeck_results().pat_ty(param.pat);
            let ty::Adt(adt, args) = ty.kind() else {
                continue;
            };
            let is_vec = cx.tcx.is_diagnostic_item(sym::Vec, adt.did());
            let is_data_type = is_kernel_item(cx, adt.did(), "schema::DataType");
            if !is_vec && !is_data_type {
                continue;
            }
            let mut uses = ParameterUses {
                tcx: cx.tcx,
                binding,
                uses: Vec::new(),
            };
            uses.visit_body(body);
            let [usage] = uses.uses.as_slice() else {
                continue;
            };
            // Capturing a parameter can impose Send/lifetime bounds on the new iterator type.
            if cx.tcx.hir_enclosing_body_owner(usage.hir_id).to_def_id() != def_id
                || (usage.span.from_expansion()
                    && usage.span.desugaring_kind() != Some(DesugaringKind::ForLoop))
            {
                continue;
            }

            if is_vec
                && let Some(iterator) = consuming_iterator(cx, usage)
                && has_known_consumer(cx, iterator)
            {
                cx.emit_span_lint(
                    KERNEL_VEC_PARAMETER_INTO_ITERATOR,
                    syntax_ty.span,
                    DiagDecorator(|diag| {
                        diag.primary_message("this Vec parameter is only consumed by one iterator pipeline");
                        diag.help(format!("consider impl IntoIterator<Item = {}>", args.type_at(0)));
                        diag.note("review call-site type inference and public API compatibility before changing the signature");
                    }),
                );
            }
            if is_data_type && forwards_into_data_type(cx, usage) {
                cx.emit_span_lint(
                    KERNEL_DATA_TYPE_PARAMETER_INTO,
                    syntax_ty.span,
                    DiagDecorator(|diag| {
                        diag.primary_message("this DataType parameter is only forwarded to an Into<DataType> constructor");
                        diag.help("consider impl Into<delta_kernel::schema::DataType>");
                        diag.note("review call-site type inference and public API compatibility before changing the signature");
                    }),
                );
            }
        }
    }
}

struct ParameterUses<'tcx> {
    tcx: TyCtxt<'tcx>,
    binding: HirId,
    uses: Vec<&'tcx Expr<'tcx>>,
}

impl<'tcx> Visitor<'tcx> for ParameterUses<'tcx> {
    type NestedFilter = nested_filter::OnlyBodies;

    fn maybe_tcx(&mut self) -> Self::MaybeTyCtxt {
        self.tcx
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if let ExprKind::Path(QPath::Resolved(_, path)) = expr.kind
            && path.res == Res::Local(self.binding)
        {
            self.uses.push(expr);
        }
        intravisit::walk_expr(self, expr);
    }
}

fn parent_expr<'tcx>(cx: &LateContext<'tcx>, expr: &Expr<'_>) -> Option<&'tcx Expr<'tcx>> {
    match cx.tcx.parent_hir_node(expr.hir_id) {
        Node::Expr(parent) => Some(parent),
        _ => None,
    }
}

fn is_into_iter(cx: &LateContext<'_>, def_id: DefId) -> bool {
    let expected = cx.tcx.lang_items().into_iter_fn();
    Some(def_id) == expected
        || cx
            .tcx
            .trait_item_of(def_id)
            .is_some_and(|id| Some(id) == expected)
}

fn consuming_iterator<'tcx>(cx: &LateContext<'tcx>, usage: &Expr<'_>) -> Option<&'tcx Expr<'tcx>> {
    let parent = parent_expr(cx, usage)?;
    if parent.span.from_expansion()
        && parent.span.desugaring_kind() != Some(DesugaringKind::ForLoop)
    {
        return None;
    }
    match parent.kind {
        ExprKind::MethodCall(_, receiver, [], _)
            if receiver.hir_id == usage.hir_id
                && cx
                    .typeck_results()
                    .type_dependent_def_id(parent.hir_id)
                    .is_some_and(|id| is_into_iter(cx, id)) =>
        {
            Some(parent)
        }
        ExprKind::Call(callee, [arg])
            if arg.hir_id == usage.hir_id
                && called_function(cx, callee).is_some_and(|id| is_into_iter(cx, id)) =>
        {
            Some(parent)
        }
        _ => None,
    }
}

fn has_known_consumer<'tcx>(cx: &LateContext<'tcx>, mut iterator: &'tcx Expr<'tcx>) -> bool {
    let mut filtered_count = false;
    loop {
        if iterator.span.desugaring_kind() == Some(DesugaringKind::ForLoop) {
            return true;
        }
        let Some(parent) = parent_expr(cx, iterator) else {
            return false;
        };
        if parent.span.from_expansion()
            && parent.span.desugaring_kind() != Some(DesugaringKind::ForLoop)
        {
            return false;
        }
        if let ExprKind::Call(callee, [arg]) = parent.kind
            && arg.hir_id == iterator.hir_id
            && parent.span.desugaring_kind() == Some(DesugaringKind::ForLoop)
            && called_function(cx, callee).is_some_and(|id| is_into_iter(cx, id))
        {
            return true;
        }
        let ExprKind::MethodCall(_, receiver, _, _) = parent.kind else {
            return false;
        };
        if receiver.hir_id != iterator.hir_id {
            return false;
        }
        let Some(method) = cx.typeck_results().type_dependent_def_id(parent.hir_id) else {
            return false;
        };
        let trait_method = cx.tcx.trait_item_of(method).unwrap_or(method);
        if is_crate_item(cx, trait_method, "itertools", "Itertools::join") {
            return true;
        }
        if cx.tcx.trait_of_assoc(trait_method) != cx.tcx.lang_items().iterator_trait() {
            return false;
        }
        match cx.tcx.item_name(trait_method).as_str() {
            "count" => return filtered_count,
            "all" | "any" | "collect" | "find" | "find_map" | "fold" | "for_each" | "max"
            | "max_by" | "max_by_key" | "min" | "min_by" | "min_by_key" | "next" | "nth"
            | "position" | "product" | "reduce" | "sum" | "try_fold" | "try_for_each" => {
                return true;
            }
            "filter" | "filter_map" => {
                filtered_count = true;
                iterator = parent;
            }
            "chain" | "enumerate" | "flat_map" | "flatten" | "fuse" | "inspect" | "map"
            | "map_while" | "peekable" | "scan" | "skip" | "skip_while" | "step_by" | "take"
            | "take_while" | "zip" => {
                filtered_count = false;
                iterator = parent;
            }
            _ => return false,
        }
    }
}

fn forwards_into_data_type(cx: &LateContext<'_>, usage: &Expr<'_>) -> bool {
    let Some(parent) = parent_expr(cx, usage) else {
        return false;
    };
    if parent.span.from_expansion() {
        return false;
    }
    let ExprKind::Call(callee, args) = parent.kind else {
        return false;
    };
    let Some(callee) = called_function(cx, callee) else {
        return false;
    };
    let Some(index) = args.iter().position(|arg| arg.hir_id == usage.hir_id) else {
        return false;
    };
    let known_constructor = [
        ("schema::StructField", "new"),
        ("schema::StructField", "nullable"),
        ("schema::StructField", "not_null"),
        ("schema::ArrayType", "new"),
        ("schema::MapType", "new"),
        ("expressions::scalars::Scalar", "null"),
        ("expressions::CastExpression", "new"),
        ("expressions::Expression", "cast"),
    ]
    .iter()
    .any(|(owner, name)| is_kernel_method(cx, callee, owner, name))
        || is_kernel_item(cx, callee, "expressions::null_lit");
    if !known_constructor {
        return false;
    }
    let sig = cx
        .tcx
        .fn_sig(callee)
        .instantiate_identity()
        .skip_norm_wip()
        .skip_binder();
    let Some(&formal) = sig.inputs().get(index) else {
        return false;
    };
    if !matches!(formal.kind(), ty::Param(_)) {
        return false;
    }
    cx.tcx
        .predicates_of(callee)
        .predicates
        .iter()
        .any(|(predicate, _)| {
            let ty::ClauseKind::Trait(bound) = predicate.kind().skip_binder() else {
                return false;
            };
            if bound.trait_ref.self_ty() != formal
                || !cx.tcx.is_diagnostic_item(sym::Into, bound.trait_ref.def_id)
            {
                return false;
            }
            let ty::Adt(target, _) = bound.trait_ref.args.type_at(1).kind() else {
                return false;
            };
            is_kernel_item(cx, target.did(), "schema::DataType")
        })
}
