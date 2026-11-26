use rustc_ast::BindingMode;
use rustc_errors::DiagDecorator;
use rustc_hir::def::DefKind;
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{
    Block, Expr, ExprKind, HirId, LoopSource, MatchSource, Node, Pat, PatKind, StmtKind,
};
use rustc_lint::{LateContext, LateLintPass, LintContext, LintStore};
use rustc_session::{declare_lint, declare_lint_pass};

declare_lint! {
    /// Identify substantial success blocks that can be reviewed for a guard clause.
    pub KERNEL_PREFER_GUARD_CLAUSE,
    Warn,
    "consider a let-else guard for a loop-tail if-let with short nondivergent skip work"
}

declare_lint_pass!(KernelGuardClauses => [KERNEL_PREFER_GUARD_CLAUSE]);

pub(super) fn register(store: &mut LintStore) {
    store.register_lints(&[KERNEL_PREFER_GUARD_CLAUSE]);
    store.register_late_lint_pass(Box::new(|_| Box::new(KernelGuardClauses)));
}

impl<'tcx> LateLintPass<'tcx> for KernelGuardClauses {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if expr.span.from_expansion() {
            return;
        }
        let ExprKind::If(condition, then, Some(otherwise)) = expr.kind else {
            return;
        };
        let ExprKind::Let(binding) = condition.kind else {
            return;
        };
        let (ExprKind::Block(success, None), ExprKind::Block(failure, None)) =
            (then.kind, otherwise.kind)
        else {
            return;
        };
        if success.targeted_by_break
            || failure.targeted_by_break
            || operations(success) < 2
            || operations(failure) == 0
            || operations(failure) > 3
            || !cx.typeck_results().expr_ty(then).is_unit()
            || last_expr(success).is_some_and(|tail| cx.typeck_results().expr_ty(tail).is_never())
        {
            return;
        }
        let mut pattern = PlainBindings { plain: true };
        pattern.visit_pat(binding.pat);
        if !pattern.plain {
            return;
        }
        let owner = cx.tcx.hir_enclosing_body_owner(expr.hir_id);
        if !matches!(cx.tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn) {
            return;
        }
        let Some(block) = tail_block(cx, expr) else {
            return;
        };
        if block.targeted_by_break {
            return;
        }
        if immediate_loop_body(cx, block).is_none() {
            return;
        }
        let skip_iteration = cx.typeck_results().expr_ty(otherwise).is_unit()
            && failure.stmts.iter().all(|stmt| match stmt.kind {
                StmtKind::Expr(expr) | StmtKind::Semi(expr) => skip_work(expr),
                _ => false,
            })
            && failure.expr.is_none_or(skip_work);
        if !skip_iteration {
            return;
        }
        cx.emit_span_lint(
            KERNEL_PREFER_GUARD_CLAUSE,
            expr.span,
            DiagDecorator(|diag| {
                diag.primary_message("this loop-tail if-let with skip work is a guard-clause candidate");
                diag.help("consider let PATTERN = VALUE else { existing skip work; continue; }; followed by the success body");
                diag.note("review binding scope, temporary lifetimes, and drop order before flattening; this is not an automatic equivalence proof");
            }),
        );
    }
}

fn operations(block: &Block<'_>) -> usize {
    block.stmts.len() + usize::from(block.expr.is_some())
}

fn last_expr<'tcx>(block: &Block<'tcx>) -> Option<&'tcx Expr<'tcx>> {
    block.expr.or_else(|| {
        block.stmts.last().and_then(|stmt| match stmt.kind {
            StmtKind::Expr(expr) | StmtKind::Semi(expr) => Some(expr),
            _ => None,
        })
    })
}

fn skip_work(expr: &Expr<'_>) -> bool {
    // Logging macros are opaque work retained inside the proposed else block.
    (expr.span.from_expansion() && expr.span.desugaring_kind().is_none())
        || matches!(
            expr.kind,
            ExprKind::Call(..)
                | ExprKind::MethodCall(..)
                | ExprKind::Assign(..)
                | ExprKind::AssignOp(..)
        )
}

fn tail_block<'tcx>(cx: &LateContext<'tcx>, expr: &Expr<'_>) -> Option<&'tcx Block<'tcx>> {
    match cx.tcx.parent_hir_node(expr.hir_id) {
        Node::Block(block) if block.expr.is_some_and(|tail| tail.hir_id == expr.hir_id) => {
            Some(block)
        }
        Node::Stmt(stmt) => match cx.tcx.parent_hir_node(stmt.hir_id) {
            Node::Block(block)
                if block.expr.is_none()
                    && (cx.typeck_results().expr_ty(expr).is_unit()
                        || cx.typeck_results().expr_ty(expr).is_never())
                    && block
                        .stmts
                        .last()
                        .is_some_and(|last| last.hir_id == stmt.hir_id) =>
            {
                Some(block)
            }
            _ => None,
        },
        _ => None,
    }
}

fn immediate_loop_body(cx: &LateContext<'_>, block: &Block<'_>) -> Option<HirId> {
    let mut parents = cx.tcx.hir_parent_iter(block.hir_id);
    let (_, Node::Expr(parent)) = parents.next()? else {
        return None;
    };
    match parent.kind {
        ExprKind::Loop(body, _, LoopSource::Loop, _) if body.hir_id == block.hir_id => {
            Some(parent.hir_id)
        }
        ExprKind::Block(body, None) if body.hir_id == block.hir_id => {
            let (_, outer) = parents.next()?;
            match outer {
                Node::Arm(arm) if arm.body.hir_id == parent.hir_id => {
                    let (_, Node::Expr(matched)) = parents.next()? else {
                        return None;
                    };
                    if !matches!(
                        matched.kind,
                        ExprKind::Match(_, _, MatchSource::ForLoopDesugar)
                    ) {
                        return None;
                    }
                    let (_, Node::Stmt(_)) = parents.next()? else {
                        return None;
                    };
                }
                Node::Expr(Expr {
                    kind: ExprKind::If(_, then, _),
                    ..
                }) if then.hir_id == parent.hir_id => {}
                _ => return None,
            }
            let (_, Node::Block(body)) = parents.next()? else {
                return None;
            };
            let (_, Node::Expr(loop_expr)) = parents.next()? else {
                return None;
            };
            if let ExprKind::Loop(loop_body, _, LoopSource::ForLoop | LoopSource::While, _) =
                loop_expr.kind
                && loop_body.hir_id == body.hir_id
            {
                Some(loop_expr.hir_id)
            } else {
                None
            }
        }
        _ => None,
    }
}

struct PlainBindings {
    plain: bool,
}

impl<'tcx> Visitor<'tcx> for PlainBindings {
    fn visit_pat(&mut self, pat: &'tcx Pat<'tcx>) {
        if pat.span.from_expansion()
            || matches!(pat.kind, PatKind::Binding(mode, _, _, subpattern) if mode != BindingMode::NONE || subpattern.is_some())
            || matches!(
                pat.kind,
                PatKind::Or(_) | PatKind::Ref(..) | PatKind::Deref(_) | PatKind::Guard(..)
            )
        {
            self.plain = false;
        }
        intravisit::walk_pat(self, pat);
    }
}
