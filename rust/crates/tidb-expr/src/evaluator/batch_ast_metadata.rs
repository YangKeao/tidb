// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Pure bounds for genuine scalar-build metadata, including non-value operands.
//! This walk neither resolves names nor constructs or evaluates expressions.
use super::*;

pub(in crate::evaluator::batch) fn check(
    root: &Expr,
    limits: BatchSourceLimits,
) -> LocalResult<()> {
    struct Budget<'a> {
        pending: Vec<(&'a Expr, usize)>,
        scheduled: usize,
        bytes: usize,
        limits: BatchSourceLimits,
    }
    impl<'a> Budget<'a> {
        fn push(&mut self, node: &'a Expr, depth: usize) -> LocalResult<()> {
            if self.scheduled >= self.limits.tree.max_nodes || depth > self.limits.tree.max_depth {
                return Err(resource("AST metadata node/depth cap exceeded"));
            }
            let payload = payload(node, self.limits.max_metadata_bytes)?;
            self.bytes = self
                .bytes
                .checked_add(512)
                .and_then(|n| n.checked_add(payload.checked_mul(4)?))
                .ok_or_else(|| resource("AST metadata extent overflow"))?;
            if self.bytes > self.limits.max_metadata_bytes {
                return Err(resource("AST metadata byte cap exceeded"));
            }
            self.pending
                .try_reserve(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            self.pending.push((node, depth));
            self.scheduled += 1;
            Ok(())
        }
    }
    let mut budget = Budget {
        pending: Vec::new(),
        scheduled: 0,
        bytes: 0,
        limits,
    };
    budget.push(root, 1)?;
    while let Some((node, depth)) = budget.pending.pop() {
        let next = depth
            .checked_add(1)
            .ok_or_else(|| resource("AST metadata depth overflow"))?;
        match node {
            Expr::Func { args, .. }
            | Expr::GenericFuncCall { args, .. }
            | Expr::Aggregate { args, .. }
            | Expr::GroupConcat { args, .. }
            | Expr::Window { args, .. }
            | Expr::Row(args) => {
                for arg in args.iter().rev() {
                    budget.push(arg, next)?;
                }
            }
            Expr::Binary(_, left, right)
            | Expr::MemberOf {
                expr: left,
                array: right,
            }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            }
            | Expr::Regexp {
                expr: left,
                pattern: right,
                ..
            }
            | Expr::Position {
                substr: left,
                str: right,
            }
            | Expr::TimestampAdd {
                interval: left,
                expr: right,
                ..
            }
            | Expr::TimestampDiff {
                expr1: left,
                expr2: right,
                ..
            } => {
                budget.push(right, next)?;
                budget.push(left, next)?;
            }
            Expr::In { expr, list, .. } => {
                for item in list.iter().rev() {
                    budget.push(item, next)?;
                }
                budget.push(expr, next)?;
            }
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                if let Some(value) = else_clause {
                    budget.push(value, next)?;
                }
                for (condition, value) in when_clauses.iter().rev() {
                    budget.push(value, next)?;
                    budget.push(condition, next)?;
                }
                if let Some(value) = value {
                    budget.push(value, next)?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                budget.push(high, next)?;
                budget.push(low, next)?;
                budget.push(expr, next)?;
            }
            Expr::Trim { expr, remstr, .. } => {
                if let Some(value) = remstr {
                    budget.push(value, next)?;
                }
                budget.push(expr, next)?;
            }
            Expr::CharsetBinary { value, .. }
            | Expr::Assign { value, .. }
            | Expr::Extract { value, .. }
            | Expr::Interval { value, .. }
            | Expr::Unary(_, value)
            | Expr::Paren(value)
            | Expr::ConvertUsing { expr: value, .. }
            | Expr::Collate { expr: value, .. }
            | Expr::Is { expr: value, .. }
            | Expr::GetFormat { expr: value, .. }
            | Expr::WeightString { expr: value, .. } => budget.push(value, next)?,
            Expr::Cast(cast) => budget.push(&cast.expr, next)?,
            // Other syntax is rejected by both the original scalar value and
            // metadata builders before they consume executable descendants.
            _ => {}
        }
    }
    Ok(())
}
fn payload(node: &Expr, maximum: usize) -> LocalResult<usize> {
    let sum = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or_else(|| resource("AST metadata payload overflow"))
    };
    match node {
        Expr::Int(s)
        | Expr::Decimal(s)
        | Expr::Hex(s)
        | Expr::String(s)
        | Expr::RawString(s)
        | Expr::UserVar(s)
        | Expr::SysVar { name: s, .. }
        | Expr::Assign { name: s, .. }
        | Expr::Func { name: s, .. }
        | Expr::CharsetBinary { charset: s, .. }
        | Expr::ConvertUsing { charset: s, .. }
        | Expr::Collate { collation: s, .. }
        | Expr::Extract { unit: s, .. }
        | Expr::Interval { unit: s, .. }
        | Expr::TimestampAdd { unit: s, .. }
        | Expr::TimestampDiff { unit: s, .. } => Ok(s.len()),
        Expr::GenericFuncCall { schema, name, .. } => sum(schema.len(), name.len()),
        Expr::CharsetString { charset, value } => sum(charset.len(), value.len()),
        Expr::Bit(value) => value
            .as_bytes()
            .len()
            .checked_mul(8)
            .ok_or_else(|| resource("AST bit rendering extent overflow")),
        Expr::ParamMarker {
            value: Some(value), ..
        } => native_payload_bytes(value),
        Expr::Column(path) | Expr::Default(Some(path)) => {
            let base = array_bytes::<String>(path.len())?;
            if base > maximum {
                return Err(resource("AST qualified name exceeds metadata cap"));
            }
            path.iter().try_fold(base, |n, s| {
                let n = sum(n, s.len())?;
                if n > maximum {
                    Err(resource("AST qualified name exceeds metadata cap"))
                } else {
                    Ok(n)
                }
            })
        }
        Expr::Cast(cast) => Ok(match &cast.cast_type {
            tidb_ast::CastType::Char {
                charset: Some(charset),
                ..
            } => charset.len(),
            _ => 0,
        }),
        _ => Ok(0),
    }
}
