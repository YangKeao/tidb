// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Consuming actual operands with the original lazy SQL byte/escape boundaries.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;
use tidb_query_expr::NativeLikeInvocation;

fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native LIKE work allowance exceeded".into()),
        None,
    ))
}
fn check(n: usize, maximum: usize) -> Result<(), EvalError> {
    if maximum == usize::MAX || n > maximum {
        Err(resource())
    } else {
        Ok(())
    }
}
fn extent(n: usize, factor: usize, extra: usize) -> Result<usize, EvalError> {
    n.checked_mul(factor)
        .and_then(|n| n.checked_add(extra))
        .ok_or_else(resource)
}
fn bytes(value: &Datum, pattern: bool, maximum: usize) -> Result<Vec<u8>, EvalError> {
    let invalid = || {
        EvalError::Unsupported(if pattern {
            "invalid LIKE pattern scalar domain"
        } else {
            "invalid LIKE operand scalar domain"
        })
    };
    // This is SQL Datum.ToString, including Go float formatting and strict Raw,
    // not the separate expression coerce_bytes or UTF-8 string helper.
    let work =
        tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()).map_err(|e| {
            match e {
                tidb_query_expr::NativeStringCoercionError::Unsupported(_) => invalid(),
                tidb_query_expr::NativeStringCoercionError::ResourceLimit => resource(),
            }
        })?;
    check(work, maximum)?;
    let bytes = value.sql_bytes().map_err(|_| invalid())?;
    check(bytes.capacity(), maximum)?;
    Ok(bytes)
}
fn escape(
    value: &Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Option<u8>, EvalError> {
    if matches!(
        value,
        Datum::VectorFloat32(_) | Datum::MinNotNull | Datum::MaxValue
    ) {
        // These original strict kind errors precede all context callbacks and
        // have no formatter work or payload copy to charge.
        let converted = crate::cast::cast_arg_as_int(value, source, ctx)?;
        return Ok(crate::arg_eval_type::eval_int(&converted)?.map(|v| v as u8));
    }
    let n = match value {
        Datum::String(v) => v.bytes().len(),
        Datum::Bytes(v) | Datum::Raw(v) => v.len(),
        Datum::BinaryLiteral(v) | Datum::Bit(v) => v.as_bytes().len(),
        Datum::Json(v) => v.value().len(),
        Datum::Decimal(v) => v.coefficient_bytes().len(),
        _ => 0,
    };
    check(extent(n, 32, 2048)?, maximum)?;
    if let Datum::Decimal(v) = value {
        // The integer projection uses storage, not visible text precision.
        // Only the original exceptional diagnostic demands that formatting.
        let display = if source.is_some_and(FieldType::is_unsigned) {
            v.round_to_i64_saturating() < 0
        } else {
            v.round_to_i64().is_none()
        };
        if display {
            let n = n
                .checked_add(v.scale().max(v.storage_scale()) as usize)
                .ok_or_else(resource)?;
            check(extent(n, 32, 2048)?, maximum)?;
        }
    }
    let converted = crate::cast::cast_arg_as_int(value, source, ctx)?;
    Ok(crate::arg_eval_type::eval_int(&converted)?.map(|v| v as u8))
}

/// Each advance consumes the old state before any conversion/callback can fail.
#[derive(Debug)]
pub(crate) enum Prepared {
    Text,
    Pattern(Vec<u8>),
    Escape(Vec<u8>, Vec<u8>),
}
pub(crate) enum Step {
    Need(Prepared),
    Null,
    Match(Match),
}
pub(crate) struct Match {
    text: Vec<u8>,
    pattern: Vec<u8>,
    escape: u8,
}
impl Prepared {
    pub(crate) fn argument(&self) -> usize {
        match self {
            Self::Text => 0,
            Self::Pattern(_) => 1,
            Self::Escape(_, _) => 2,
        }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        match self {
            Self::Text => 0,
            Self::Pattern(text) => text.capacity(),
            Self::Escape(text, pattern) => text.capacity().saturating_add(pattern.capacity()),
        }
    }
    pub(crate) fn advance(
        self,
        value: &Datum,
        source: Option<&FieldType>,
        ctx: &dyn Columns,
        maximum: usize,
    ) -> Result<Step, EvalError> {
        check(self.retained_bytes(), maximum)?;
        let free = maximum - self.retained_bytes();
        if value.is_null() {
            return Ok(Step::Null);
        }
        Ok(match self {
            Self::Text => Step::Need(Self::Pattern(bytes(value, false, free)?)),
            Self::Pattern(text) => Step::Need(Self::Escape(text, bytes(value, true, free)?)),
            Self::Escape(text, pattern) => match escape(value, source, ctx, free)? {
                None => Step::Null,
                Some(escape) => Step::Match(Match {
                    text,
                    pattern,
                    escape,
                }),
            },
        })
    }
}
impl Match {
    pub(crate) fn evaluate(
        self,
        invocation: &NativeLikeInvocation,
        maximum: usize,
    ) -> Result<Datum, EvalError> {
        let held = self
            .text
            .capacity()
            .checked_add(self.pattern.capacity())
            .ok_or_else(resource)?;
        check(held, maximum)?;
        value(
            invocation,
            &self.text,
            &self.pattern,
            self.escape,
            maximum - held,
        )
        .map(|v| Datum::Int(i64::from(v)))
    }
}
pub(crate) fn value(
    invocation: &NativeLikeInvocation,
    text: &[u8],
    pattern: &[u8],
    escape: u8,
    maximum: usize,
) -> Result<bool, EvalError> {
    invocation
        .evaluate_bounded(text, pattern, escape, maximum)
        .map_err(|_| resource())
}

pub(crate) fn default_budget() -> usize {
    super::control_value::default_budget()
}

/// The AST caller has already evaluated both children. A NULL on either side
/// suppresses both byte conversions, unlike the lazy scalar signature.
pub(crate) fn ast(
    ilike: bool,
    left: &Datum,
    right: &Datum,
    escape: Option<u8>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if left.is_null() || right.is_null() {
        return Ok(Datum::Null);
    }
    let text = bytes(left, false, maximum)?;
    let remaining = maximum.checked_sub(text.capacity()).ok_or_else(resource)?;
    let pattern = bytes(right, true, remaining)?;
    let collation = tidb_datatype::Collation::Utf8Mb4Bin.native_policy();
    let invocation = if ilike {
        NativeLikeInvocation::ilike(collation, None)
    } else {
        NativeLikeInvocation::like(collation, None)
    };
    Match {
        text,
        pattern,
        escape: escape.unwrap_or(92),
    }
    .evaluate(&invocation, maximum)
}
