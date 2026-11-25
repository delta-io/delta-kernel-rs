#![feature(rustc_private)]

extern crate rustc_ast;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_middle;
extern crate rustc_session;

use rustc_ast::LitKind;
use rustc_errors::DiagDecorator;
use rustc_hir::def::DefKind;
use rustc_hir::def_id::DefId;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_middle::ty;
use rustc_session::{declare_lint, declare_lint_pass};

dylint_linting::dylint_library!();

declare_lint! {
    /// Prefer named constructors when a field's nullability is statically known.
    pub KERNEL_NAMED_NULLABILITY,
    Warn,
    "use StructField::nullable or StructField::not_null for literal nullability"
}

declare_lint! {
    /// Prefer the free expression constructor in inline expression trees.
    pub KERNEL_EXPRESSION_LITERAL,
    Warn,
    "use lit instead of Expression::literal"
}

declare_lint! {
    /// Prefer the null expression constructor over a nested scalar constructor.
    pub KERNEL_NULL_LITERAL,
    Warn,
    "use null_lit instead of lit(Scalar::Null(...))"
}

declare_lint! {
    /// Prefer named predicate constants when the boolean value is literal.
    pub KERNEL_PREDICATE_CONSTANT,
    Warn,
    "use Predicate::TRUE or Predicate::FALSE for a literal boolean"
}

declare_lint_pass!(KernelConstructors => [
    KERNEL_NAMED_NULLABILITY,
    KERNEL_EXPRESSION_LITERAL,
    KERNEL_NULL_LITERAL,
    KERNEL_PREDICATE_CONSTANT,
]);

/// Registers the Kernel constructor lints with the Dylint driver.
#[unsafe(no_mangle)]
pub fn register_lints(_sess: &rustc_session::Session, store: &mut rustc_lint::LintStore) {
    store.register_lints(&[
        KERNEL_NAMED_NULLABILITY,
        KERNEL_EXPRESSION_LITERAL,
        KERNEL_NULL_LITERAL,
        KERNEL_PREDICATE_CONSTANT,
    ]);
    store.register_late_lint_pass(Box::new(|_| Box::new(KernelConstructors)));
}

impl<'tcx> LateLintPass<'tcx> for KernelConstructors {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if expr.span.from_expansion() {
            return;
        }
        let ExprKind::Call(callee, args) = expr.kind else {
            return;
        };
        let Some(def_id) = called_function(cx, callee) else {
            return;
        };
        let owner = cx.tcx.hir_enclosing_body_owner(expr.hir_id).to_def_id();

        if is_kernel_method(cx, def_id, "schema::StructField", "new")
            && let [_, _, nullable] = args
            && let Some(value) = literal_bool(nullable)
            && !["nullable", "not_null"]
                .iter()
                .any(|name| is_kernel_method(cx, owner, "schema::StructField", name))
        {
            let constructor = if value { "nullable" } else { "not_null" };
            cx.emit_span_lint(
                KERNEL_NAMED_NULLABILITY,
                expr.span,
                DiagDecorator(|diag| {
                    diag.primary_message("field nullability is a boolean literal");
                    diag.help(format!("use StructField::{constructor}(name, data_type)"));
                }),
            );
        }

        if is_kernel_method(cx, def_id, "expressions::Expression", "literal")
            && !is_kernel_item(cx, owner, "expressions::lit")
        {
            cx.emit_span_lint(
                KERNEL_EXPRESSION_LITERAL,
                expr.span,
                DiagDecorator(|diag| {
                    diag.primary_message("expression literals have a shorter constructor");
                    diag.help("use delta_kernel::expressions::lit(value)");
                }),
            );
        }

        if is_kernel_item(cx, def_id, "expressions::lit")
            && let [arg] = args
            && is_null_scalar(cx, arg)
            && !is_kernel_item(cx, owner, "expressions::null_lit")
        {
            cx.emit_span_lint(
                KERNEL_NULL_LITERAL,
                expr.span,
                DiagDecorator(|diag| {
                    diag.primary_message("null expressions have a dedicated constructor");
                    diag.help("use delta_kernel::expressions::null_lit(data_type)");
                }),
            );
        }

        if is_kernel_method(cx, def_id, "expressions::Predicate", "literal")
            && let [arg] = args
            && let Some(value) = literal_bool(arg)
            && !["TRUE", "FALSE"]
                .iter()
                .any(|name| is_kernel_method(cx, owner, "expressions::Predicate", name))
        {
            let constant = if value { "TRUE" } else { "FALSE" };
            cx.emit_span_lint(
                KERNEL_PREDICATE_CONSTANT,
                expr.span,
                DiagDecorator(|diag| {
                    diag.primary_message("predicate value is a boolean literal");
                    diag.help(format!("use Predicate::{constant}"));
                }),
            );
        }
    }
}

fn literal_bool(expr: &Expr<'_>) -> Option<bool> {
    if let ExprKind::Lit(lit) = expr.kind
        && let LitKind::Bool(value) = lit.node
    {
        Some(value)
    } else {
        None
    }
}

fn called_function(cx: &LateContext<'_>, expr: &Expr<'_>) -> Option<DefId> {
    if let ExprKind::Path(ref path) = expr.kind {
        cx.qpath_res(path, expr.hir_id).opt_def_id()
    } else {
        None
    }
}

fn is_kernel_item(cx: &LateContext<'_>, def_id: DefId, path: &str) -> bool {
    if cx.tcx.crate_name(def_id.krate).as_str() != "delta_kernel" {
        return false;
    }
    let definition = cx.tcx.def_path(def_id);
    let expected: Vec<_> = path.split("::").collect();
    definition.data.len() == expected.len()
        && definition
            .data
            .iter()
            .zip(expected)
            .all(|(component, expected)| {
                component
                    .data
                    .get_opt_name()
                    .is_some_and(|actual| actual.as_str() == expected)
            })
}

fn is_kernel_method(cx: &LateContext<'_>, def_id: DefId, self_type: &str, name: &str) -> bool {
    if cx
        .tcx
        .opt_item_name(def_id)
        .is_none_or(|item| item.as_str() != name)
    {
        return false;
    }
    let Some(impl_id) = cx.tcx.inherent_impl_of_assoc(def_id) else {
        return false;
    };
    let ty::Adt(adt, _) = cx
        .tcx
        .type_of(impl_id)
        .instantiate_identity()
        .skip_norm_wip()
        .kind()
    else {
        return false;
    };
    is_kernel_item(cx, adt.did(), self_type)
}

fn is_null_scalar(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
    let ExprKind::Call(callee, [_]) = expr.kind else {
        return false;
    };
    let Some(ctor) = called_function(cx, callee) else {
        return false;
    };
    if !matches!(cx.tcx.def_kind(ctor), DefKind::Ctor(..)) {
        return false;
    }
    let variant = cx.tcx.parent(ctor);
    cx.tcx.item_name(variant).as_str() == "Null"
        && is_kernel_item(cx, cx.tcx.parent(variant), "expressions::scalars::Scalar")
}
