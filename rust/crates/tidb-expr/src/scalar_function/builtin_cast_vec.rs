// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_cast_vec.go`: typed source column followed by conversion.

use super::{vec_eval, Kernel, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};

/// Go `builtinCast*As*Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let Kernel::Cast {
        source,
        target,
        ..
    } = function.pb_builtin.kernel
    else {
        unreachable!()
    };
    let arg = &function.args[0];
    if source == EvalType::String
        && matches!(target, EvalType::Real | EvalType::Decimal)
        && super::is_numeric_binary_literal(arg)
    {
        return vec_eval::eval_into(arg, target, ctx, input, result);
    }
    if source == EvalType::String
        && target == EvalType::Int
        && arg.static_type().is_some_and(|f| f.is_hybrid())
    {
        return vec_eval::eval_into(arg, EvalType::Int, ctx, input, result);
    }
    if source == EvalType::String && matches!(target, EvalType::Int | EvalType::Real)
        && matches!(arg, crate::expression::Expression::ScalarFunction(child) if child.pb_code() == tidb_proto::tipb::ScalarFuncSig::DayName) {
        return vec_eval::eval_into(arg, target, ctx, input, result);
    }
    let reuse = source == target && source != EvalType::String;
    let mut buffer = Column::default();
    if reuse {
        vec_eval::eval_into(arg, source, ctx, input, result)?;
    } else {
        vec_eval::eval_into(arg, source, ctx, input, &mut buffer)?;
        crate::chunk_executor::allocate(target, input.num_rows(), result);
    }
    // Go CastJSONAsJSON delegates directly, retaining the source column.
    if source == EvalType::Json && target == EvalType::Json {
        return Ok(());
    }
    for i in 0..input.num_rows() {
        let source_column = if reuse { &*result } else { &buffer };
        let value = vec_eval::value(source_column, i, source, arg.static_type());
        let value = function.pb_builtin.cast_value_in(function, value, true, ctx)?;
        vec_eval::put(result, i, target, &value)?;
    }
    Ok(())
}
