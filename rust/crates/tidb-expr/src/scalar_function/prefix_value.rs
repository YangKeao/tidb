// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Detached original CAST/UNION value boundaries and their fresh demand phases.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrefixKind {
    Union,
    UnknownCast,
    OptionalCast,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrefixPhase {
    Union,
    Cast,
    Common,
    Consumed,
}
pub(crate) enum PrefixStep {
    Need(PrefixPhase),
    Value(Datum),
}

/// The existing ordinary CAST target recognizer, independent of SQL fields.
pub(crate) fn ordinary_cast_target(target: &str) -> bool {
    matches!(
        target,
        "signed"
            | "unsigned"
            | "unsigned_in_union"
            | "char"
            | "binary"
            | "decimal"
            | "date"
            | "datetime"
            | "time"
            | "year"
            | "double"
            | "float"
            | "vector"
            | "json"
    )
}
impl PrefixKind {
    pub(crate) fn for_function(function: &ScalarFunction) -> Option<Self> {
        if function.pb_signature().is_some()
            || function.pb_origin().is_some()
            || function.args.len() != 1
        {
            return None;
        }
        let name = function.func_name.lowercase();
        if name.ends_with("_in_union") {
            Some(Self::Union)
        } else if name
            .strip_prefix("cast_")
            .is_some_and(|target| !ordinary_cast_target(target))
        {
            Some(Self::UnknownCast)
        } else if name
            .strip_prefix("cast_")
            .is_some_and(|target| target != "json" && ordinary_cast_target(target))
            && (function.get_static_type().is_none() || function.args[0].static_type().is_none())
        {
            // The original ordinary non-JSON value boundary accepts genuine
            // optional source/result declarations after demanding its child.
            Some(Self::OptionalCast)
        } else {
            None
        }
    }
    pub(crate) fn first(self) -> PrefixPhase {
        match self {
            Self::Union => PrefixPhase::Union,
            Self::UnknownCast | Self::OptionalCast => PrefixPhase::Cast,
        }
    }
}

/// ScalarFunction's original source-specific UNION value branch. None means
/// the original caller falls through and must demand its live argument again.
pub(crate) fn union_value(
    name: &str,
    value: Datum,
    source: Option<&FieldType>,
    result_type: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    if value.is_null() {
        return Some(Ok(Datum::Null));
    }
    let result = crate::func::eval_func_values_in(name, &[value], ctx)?;
    Some((|| {
        let result = result?;
        // The dispatcher models the source-specific inUnion decision. Apply
        // the merged DECIMAL precision/scale afterward, as the original
        // signature's ProduceDecWithSpecifiedTp boundary requires.
        if result_type
            .is_some_and(|field_type| field_type.code() == tidb_datatype::FieldTypeCode::NewDecimal)
        {
            let ret_type =
                result_type.ok_or(EvalError::Unsupported("a cast with no result type"))?;
            return crate::cast::eval_cast(
                &cast_type_of("decimal", ret_type)?,
                result,
                source,
                ctx,
            );
        }
        Ok(result)
    })())
}

/// ScalarFunction's original non-JSON ordinary CAST value branch. Source and
/// target absence stay genuine; neither is inferred from the actual value.
pub(crate) fn cast_value(
    target: &str,
    value: Datum,
    source: Option<&FieldType>,
    result_type: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let ret_type = result_type.ok_or(EvalError::Unsupported("a cast with no result type"))?;
    crate::cast::eval_cast(&cast_type_of(target, ret_type)?, value, source, ctx)
}
