// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Closed, preparation-only admission; no value evaluator or inference table.
//!
//! Structural metadata is not value-refined SqlBuild metadata. In particular,
//! this boundary must not publish a computed NOT_NULL flag or cast precision.
#![allow(dead_code)] // Explicit private entrypoints, not a public execution route.

use super::ColumnResolver;
use crate::constant_fold::ConstantFoldMode;
use crate::expression::Expression;
use crate::EvalError;
use tidb_ast::{BinaryOp, CastStyle, CastType, Expr};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparationPurpose {
    SqlBuild,
    StructuralOnly,
}

impl PreparationPurpose {
    pub(super) fn fold(
        self,
        resolver: &impl ColumnResolver,
        expression: &mut Expression,
        mode: ConstantFoldMode,
    ) {
        // Even the default resolver's metadata-only null-flag hook evaluates.
        if self == Self::SqlBuild {
            resolver.fold_constant(expression, mode);
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StructuralLimits {
    pub max_nodes: usize,
    pub max_depth: usize,
}

/// An unevaluated tree, not a compiled program or fully refined planner type.
pub(crate) struct StructuralExpression {
    root: Expression,
}

impl StructuralExpression {
    pub(crate) fn checked(
        mut root: Expression,
        limits: StructuralLimits,
    ) -> Result<Self, EvalError> {
        check_trees(std::iter::once(&root), limits, 1, 0)?;
        // Detach GoSharedSlice-backed type metadata without cloning the tree.
        let mut pending = vec![&mut root];
        while let Some(node) = pending.pop() {
            let field = match node {
                Expression::Constant(constant) => &mut constant.ret_type,
                Expression::Column(column) => &mut column.ret_type,
                Expression::ScalarFunction(function) => {
                    pending.extend(function.args.iter_mut());
                    &mut function.ret_type
                }
                Expression::CorrelatedColumn(_) => unreachable!("checked structural tree"),
            };
            if let Some(field) = field {
                *field = field.deep_copy_like_go();
            }
        }
        Ok(Self { root })
    }

    pub(crate) fn as_expression(&self) -> &Expression {
        &self.root
    }

    /// Transfer the checked tree to another explicit preparation boundary.
    /// Do not recursively Clone merely to extract it from `as_expression`.
    pub(crate) fn into_expression(self) -> Expression {
        self.root
    }
}

fn unsupported() -> EvalError {
    EvalError::Unsupported(
        "StructuralOnly: expression needs a separately admitted preparation path",
    )
}

struct Budget {
    limits: StructuralLimits,
    nodes: usize,
}

impl Budget {
    fn push<'a, T>(
        &mut self,
        pending: &mut Vec<(&'a T, usize)>,
        node: &'a T,
        depth: usize,
    ) -> Result<(), EvalError> {
        if depth > self.limits.max_depth || self.nodes >= self.limits.max_nodes {
            return Err(EvalError::Unsupported(
                "StructuralOnly: preparation limit exceeded",
            ));
        }
        self.nodes += 1;
        pending.push((node, depth));
        Ok(())
    }
}

/// Check the entire syntax before any name resolver or value-capable hook runs.
pub(crate) fn check_ast(root: &Expr, limits: StructuralLimits) -> Result<(), EvalError> {
    let mut budget = Budget { limits, nodes: 0 };
    let mut pending = Vec::new();
    budget.push(&mut pending, root, 1)?;
    while let Some((node, depth)) = pending.pop() {
        let next = depth.checked_add(1).ok_or_else(unsupported)?;
        match node {
            Expr::Int(_) | Expr::String(_) | Expr::Null | Expr::Column(_) => {}
            Expr::Paren(child) => budget.push(&mut pending, child.as_ref(), next)?,
            Expr::Binary(BinaryOp::LogicAnd | BinaryOp::LogicOr, left, right) => {
                budget.push(&mut pending, right.as_ref(), next)?;
                budget.push(&mut pending, left.as_ref(), next)?;
            }
            Expr::Func { name, args, .. }
                if matches!(
                    name.to_ascii_lowercase().as_str(),
                    "if" | "ifnull" | "coalesce"
                ) =>
            {
                crate::builtin_registry::verify_args_by_count(
                    &name.to_ascii_lowercase(),
                    args.len(),
                )?;
                for child in args.iter().rev() {
                    budget.push(&mut pending, child, next)?;
                }
            }
            Expr::Case {
                value: None,
                when_clauses,
                else_clause,
            } if !when_clauses.is_empty() => {
                if let Some(child) = else_clause {
                    budget.push(&mut pending, child.as_ref(), next)?;
                }
                for (condition, result) in when_clauses.iter().rev() {
                    budget.push(&mut pending, result, next)?;
                    budget.push(&mut pending, condition, next)?;
                }
            }
            Expr::Cast(cast)
                if !cast.array
                    && cast.style == CastStyle::Cast
                    && matches!(cast.cast_type, CastType::Signed) =>
            {
                budget.push(&mut pending, cast.expr.as_ref(), next)?;
            }
            // In particular, reject simple CASE before expanding its selector.
            _ => return Err(unsupported()),
        }
    }
    Ok(())
}

pub(crate) fn require_bigint(expression: &Expression) -> Result<(), EvalError> {
    match expression.static_type() {
        Some(field) if signed_bigint(field) => Ok(()),
        _ => Err(unsupported()),
    }
}

fn signed_bigint(field: &FieldType) -> bool {
    field.code() == FieldTypeCode::LongLong && !field.is_unsigned() && !field.is_array()
}

/// Admission facts only. Actual result types still come from the old builders.
pub(crate) fn check_control(name: &str, args: &[Expression]) -> Result<(), EvalError> {
    let arity = match name {
        "and" | "or" | "ifnull" => args.len() == 2,
        "if" => args.len() == 3,
        "coalesce" => !args.is_empty(),
        "case" => args.len() >= 2,
        _ => false,
    };
    if !arity {
        return Err(unsupported());
    }
    for arg in args {
        require_bigint(arg)?;
    }
    Ok(())
}

pub(crate) fn check_arguments(
    name: &str,
    args: &[Expression],
    limits: StructuralLimits,
) -> Result<(), EvalError> {
    // The new call itself consumes one node and one depth level. Bound the
    // argument scan before even its metadata-only type checks.
    if limits.max_nodes == 0 || limits.max_depth == 0 || args.len() >= limits.max_nodes {
        return Err(EvalError::Unsupported(
            "StructuralOnly: preparation limit exceeded",
        ));
    }
    check_control(name, args)?;
    check_trees(args.iter(), limits, 2, 1)
}

fn check_trees<'a>(
    roots: impl IntoIterator<Item = &'a Expression>,
    limits: StructuralLimits,
    depth: usize,
    nodes: usize,
) -> Result<(), EvalError> {
    let mut budget = Budget { limits, nodes };
    let mut pending = Vec::new();
    for root in roots {
        budget.push(&mut pending, root, depth)?;
    }
    while let Some((node, depth)) = pending.pop() {
        if let Expression::ScalarFunction(function) = node {
            if function.args.len() > limits.max_nodes.saturating_sub(budget.nodes) {
                return Err(EvalError::Unsupported(
                    "StructuralOnly: preparation limit exceeded",
                ));
            }
        }
        check_node(node)?;
        if let Expression::ScalarFunction(function) = node {
            let next = depth.checked_add(1).ok_or_else(unsupported)?;
            for child in function.args.iter().rev() {
                budget.push(&mut pending, child, next)?;
            }
        }
    }
    Ok(())
}

/// At-site check after binding/inference, before implicit wrapping or probing.
pub(crate) fn check_node(node: &Expression) -> Result<(), EvalError> {
    let field = node.static_type().ok_or_else(unsupported)?;
    if field.is_array() {
        return Err(unsupported());
    }
    match node {
        Expression::Constant(constant) => {
            if constant.pb_origin().is_some() {
                return Err(unsupported());
            }
            match constant.literal_value() {
                Some(Datum::Int(_)) if signed_bigint(field) => Ok(()),
                Some(Datum::Null)
                    if signed_bigint(field) || field.code() == FieldTypeCode::Null =>
                {
                    Ok(())
                }
                Some(Datum::String(_)) if field.code() == FieldTypeCode::VarString => Ok(()),
                _ => Err(unsupported()),
            }
        }
        Expression::Column(column)
            if column.index >= 0
                && column.virtual_expr.is_none()
                && column.correlated_col_unique_id == 0
                && column.pb_origin().is_none() =>
        {
            require_bigint(node)
        }
        Expression::ScalarFunction(function) => {
            if function.pb_origin().is_some()
                || function.pb_signature().is_some()
                || function.has_values_offset()
                || function.has_grouping_metadata()
            {
                return Err(unsupported());
            }
            require_bigint(node)?;
            if function.func_name.lowercase() == "cast_signed" && function.args.len() == 1 {
                return Ok(());
            }
            check_control(function.func_name.lowercase(), &function.args)
        }
        _ => Err(unsupported()),
    }
}
