use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use rustc_errors::DiagDecorator;
use rustc_hir::def::DefKind;
use rustc_hir::{BinOpKind, Expr, ExprKind, MatchSource, Node};
use rustc_lint::{LateContext, LateLintPass, LintContext, LintStore};
use rustc_session::{declare_lint, impl_lint_pass};
use rustc_span::{FileName, Symbol, sym};

declare_lint! {
    /// Identify formatter-wrapped call arguments worth extracting into named statements.
    pub KERNEL_MULTILINE_NESTED_CALLS,
    Warn,
    "consider named statements for nested call arguments when rustfmt wraps the combined call"
}

#[derive(Default)]
struct NestedCalls {
    formatted: HashMap<PathBuf, bool>,
}
impl_lint_pass!(NestedCalls => [KERNEL_MULTILINE_NESTED_CALLS]);

pub(super) fn register(store: &mut LintStore) {
    store.register_lints(&[KERNEL_MULTILINE_NESTED_CALLS]);
    store.register_late_lint_pass(Box::new(|_| Box::<NestedCalls>::default()));
}

impl<'tcx> LateLintPass<'tcx> for NestedCalls {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if cx
            .get_lint_level_spec(KERNEL_MULTILINE_NESTED_CALLS)
            .is_allow()
            || !candidate(cx, expr)
            || !statement_context(cx, expr)
            || outer_candidate(cx, expr)
        {
            return;
        }
        let skip = [sym::rustfmt, Symbol::intern("skip")];
        if std::iter::once(expr.hir_id)
            .chain(cx.tcx.hir_parent_id_iter(expr.hir_id))
            .any(|id| {
                cx.tcx
                    .hir_attrs(id)
                    .iter()
                    .any(|attr| attr.path_matches(&skip))
            })
        {
            return;
        }
        let map = cx.sess().source_map();
        let FileName::Real(name) = map.span_to_filename(expr.span) else {
            return;
        };
        let Some(path) = name.local_path().and_then(|path| path.canonicalize().ok()) else {
            return;
        };
        let formatted = self.formatted.entry(path.clone()).or_insert_with(|| {
            Command::new(cx.sess().opts.sysroot.path().join("bin/rustfmt"))
                .args([
                    "--check",
                    "--edition",
                    &cx.sess().edition().to_string(),
                    "--config",
                    "skip_children=true",
                ])
                .arg(&path)
                .current_dir(path.parent().unwrap())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        });
        if !*formatted {
            return;
        }
        cx.emit_span_lint(KERNEL_MULTILINE_NESTED_CALLS, expr.span, DiagDecorator(|diag| {
            diag.primary_message("rustfmt wraps this call containing a nested call argument");
            diag.help("consider named intermediate statements in the same execution scope");
            diag.note("preserve argument evaluation order, ? propagation timing, conditional execution, and temporary/drop/borrow lifetimes; no automatic rewrite is proposed");
        }));
    }
}

fn arguments<'tcx>(expr: &Expr<'tcx>) -> Option<&'tcx [Expr<'tcx>]> {
    match expr.kind {
        ExprKind::Call(_, args) | ExprKind::MethodCall(_, _, args, _) => Some(args),
        _ => None,
    }
}

fn peel<'tcx>(mut expr: &'tcx Expr<'tcx>) -> &'tcx Expr<'tcx> {
    loop {
        match expr.kind {
            ExprKind::AddrOf(_, _, inner) | ExprKind::DropTemps(inner) => expr = inner,
            ExprKind::Match(branch, _, MatchSource::TryDesugar(_)) => {
                let ExprKind::Call(_, [inner]) = branch.kind else {
                    return expr;
                };
                expr = inner;
            }
            _ => return expr,
        }
    }
}

fn candidate(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
    let map = cx.sess().source_map();
    let span = match expr.kind {
        ExprKind::MethodCall(_, _, _, invocation) => invocation,
        ExprKind::Call(callee, _) if map.is_multiline(callee.span) => {
            expr.span.with_lo(callee.span.hi())
        }
        _ => expr.span,
    };
    !expr.span.from_expansion()
        && map.is_valid_span(span).is_ok()
        && map.is_multiline(span)
        && map
            .span_to_snippet(span)
            .is_ok_and(|source| !source.contains("//") && !source.contains("/*"))
        && arguments(expr).is_some_and(|args| {
            args.iter().any(|arg| {
                let nested = peel(arg);
                !nested.span.from_expansion() && arguments(nested).is_some()
            })
        })
}

fn statement_context(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
    let owner = cx.tcx.hir_enclosing_body_owner(expr.hir_id);
    if !matches!(cx.tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn)
        || cx.tcx.hir_body_const_context(owner).is_some()
    {
        return false;
    }
    for (_, node) in cx.tcx.hir_parent_iter(expr.hir_id) {
        match node {
            Node::Block(_) | Node::Stmt(_) => break,
            Node::Expr(parent) => match parent.kind {
                ExprKind::If(..)
                | ExprKind::Closure(_)
                | ExprKind::Match(_, _, MatchSource::Normal)
                | ExprKind::Binary(
                    rustc_hir::BinOp {
                        node: BinOpKind::And | BinOpKind::Or,
                        ..
                    },
                    _,
                    _,
                ) => return false,
                _ if parent.span.from_expansion() && parent.span.desugaring_kind().is_none() => {
                    return false;
                }
                _ => {}
            },
            _ => {}
        }
    }
    true
}

fn outer_candidate(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
    for (_, node) in cx.tcx.hir_parent_iter(expr.hir_id) {
        let Node::Expr(parent) = node else {
            return false;
        };
        if candidate(cx, parent)
            && arguments(parent)
                .is_some_and(|args| args.iter().any(|arg| peel(arg).hir_id == expr.hir_id))
        {
            return true;
        }
        if matches!(
            parent.kind,
            ExprKind::AddrOf(..)
                | ExprKind::DropTemps(_)
                | ExprKind::Match(_, _, MatchSource::TryDesugar(_))
        ) || (parent.span.desugaring_kind()
            == Some(rustc_span::hygiene::DesugaringKind::QuestionMark))
        {
            continue;
        }
        return false;
    }
    false
}
