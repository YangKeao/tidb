// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Value/context phases for stateful calls; no expression or driver is retained.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    Values,
    Benchmark,
    Sleep,
    GetLock,
    ReleaseLock,
    FreeLock,
    UsedLock,
    ReleaseAll,
}
impl Operation {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "values" => Self::Values,
            "benchmark" => Self::Benchmark,
            "sleep" => Self::Sleep,
            "get_lock" => Self::GetLock,
            "release_lock" => Self::ReleaseLock,
            "is_free_lock" => Self::FreeLock,
            "is_used_lock" => Self::UsedLock,
            "release_all_locks" => Self::ReleaseAll,
            _ => return None,
        })
    }
    pub(crate) fn arity(self) -> usize {
        match self {
            Self::Values | Self::ReleaseAll => 0,
            Self::Benchmark | Self::GetLock => 2,
            _ => 1,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Values => "values",
            Self::Benchmark => "benchmark",
            Self::Sleep => "sleep",
            Self::GetLock => "get_lock",
            Self::ReleaseLock => "release_lock",
            Self::FreeLock => "is_free_lock",
            Self::UsedLock => "is_used_lock",
            Self::ReleaseAll => "release_all_locks",
        }
    }
}
pub(crate) struct Descriptor {
    pub operation: Operation,
    pub argument_types: Box<[Option<FieldType>]>,
    pub values_offset: Option<usize>,
}
impl Descriptor {
    pub(crate) fn capture(function: &ScalarFunction) -> Option<Self> {
        Some(Self {
            operation: Operation::from_name(function.func_name.lowercase())?,
            argument_types: function
                .args
                .iter()
                .map(|a| a.static_type().cloned())
                .collect(),
            values_offset: function.values_offset,
        })
    }
    pub(crate) fn operation_name(&self) -> &'static str {
        self.operation.name()
    }
    pub(crate) fn validate_arity(&self) -> Result<(), EvalError> {
        if self.argument_types.len() == self.operation.arity() {
            return Ok(());
        }
        Err(if self.operation == Operation::Benchmark {
            EvalError::Unsupported("BENCHMARK arguments")
        } else {
            EvalError::WrongParameterCount(self.operation.name())
        })
    }
    pub(crate) fn immediate(&self, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        self.validate_arity()?;
        match self.operation {
            Operation::Values => current_value(self.values_offset, ctx),
            Operation::ReleaseAll => release_all(ctx),
            _ => unreachable!("only immediate stateful services use this phase"),
        }
    }
    pub(crate) fn benchmark_count(
        &self,
        value: &Datum,
        ctx: &dyn Columns,
    ) -> Result<Option<i64>, EvalError> {
        let Some(count) = crate::func::benchmark_loop_count_borrowed(
            value,
            self.argument_types[0].as_ref(),
            ctx,
        )?
        else {
            return Ok(None);
        };
        if count < 0 {
            return Ok(None);
        }
        crate::func::ensure_benchmark_eval_type(self.argument_types[1].as_ref())?;
        Ok(Some(count))
    }
    pub(crate) fn sleep(&self, value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        sleep(value, ctx)
    }
    pub(crate) fn sleep_after_seconds(
        &self,
        seconds: Option<f64>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        sleep_after_seconds(seconds, ctx)
    }
    pub(crate) fn lock_name(&self, value: &Datum, ctx: &dyn Columns) -> Result<String, EvalError> {
        lock_name(value, self.argument_types[0].as_ref(), ctx)
    }
    pub(crate) fn acquire(
        &self,
        name: &str,
        value: &Datum,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        acquire(name, value, self.argument_types[1].as_ref(), ctx)
    }
    pub(crate) fn lock_service(&self, name: &str, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        lock_service(self.operation, name, ctx)
    }
}

/// Only string/bytes require an owned input before warning/context callbacks.
pub(crate) fn borrowed_sleep_input(value: &Datum) -> bool {
    !matches!(value, Datum::String(_) | Datum::Bytes(_))
}
pub(crate) fn prepare_sleep_nontext(value: &Datum) -> Option<Result<Option<f64>, EvalError>> {
    if value.is_null() {
        return Some(Ok(None));
    }
    crate::ops::to_f64_nontext(value).map(|result| result.map(Some))
}

pub(crate) fn sleep_after_seconds(
    seconds: Option<f64>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let Some(seconds) = seconds else {
        ctx.handle_sleep_incorrect_argument()?;
        return Ok(Datum::Int(0));
    };
    if seconds < 0.0 {
        ctx.handle_sleep_incorrect_argument()?;
        return Ok(Datum::Int(0));
    }
    if seconds > f64::MAX / 1_000_000_000.0 {
        return Err(EvalError::IncorrectArguments(
            "Incorrect arguments to sleep".to_owned(),
        ));
    }
    if seconds <= 0.0 {
        return Ok(Datum::Int(0));
    }
    let duration = std::time::Duration::try_from_secs_f64(seconds)
        .map_err(|_| EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned()))?;
    Ok(Datum::Int(i64::from(ctx.sleep_for(duration))))
}

pub(crate) fn lock_name(
    value: &Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<String, EvalError> {
    let name = crate::cast::cast_arg_as_string(value, source, ctx)?;
    super::advisory_lock_name(name)
}

pub(crate) fn acquire(
    name: &str,
    value: &Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let timeout = crate::cast::cast_arg_as_int(value, source, ctx)?;
    let mut timeout = crate::arg_eval_type::eval_int(&timeout)?.unwrap_or(0);
    if !(0..=MAX_ADVISORY_LOCK_TIMEOUT_SECS).contains(&timeout) {
        ctx.append_warning(
            1292,
            &format!("Truncated incorrect get_lock value: '{timeout}'"),
        );
        timeout = MAX_ADVISORY_LOCK_TIMEOUT_SECS;
    }
    let acquired =
        ctx.acquire_advisory_lock(name, std::time::Duration::from_secs(timeout as u64))?;
    Ok(Datum::Int(i64::from(acquired)))
}

pub(crate) fn lock_service(
    operation: Operation,
    name: &str,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    match operation {
        Operation::ReleaseLock => Ok(Datum::Int(i64::from(ctx.release_advisory_lock(name)?))),
        Operation::FreeLock => Ok(Datum::Int(i64::from(
            ctx.advisory_lock_owner(name)?.is_none(),
        ))),
        Operation::UsedLock => Ok(ctx
            .advisory_lock_owner(name)?
            .map_or(Datum::Null, |owner| Datum::Int(owner as i64))),
        _ => unreachable!("only named one-argument lock services use this phase"),
    }
}

pub(crate) fn sleep(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let seconds = if let Some(prepared) = prepare_sleep_nontext(value) {
        prepared?
    } else {
        Some(crate::ops::to_f64_with_mysql_string(value, ctx)?)
    };
    sleep_after_seconds(seconds, ctx)
}
pub(crate) fn current_value(offset: Option<usize>, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let offset = offset.ok_or(EvalError::Unsupported("VALUES offset is not initialized"))?;
    ctx.current_insert_value(offset)?
        .map_or(Ok(Datum::Null), Ok)
}
pub(crate) fn release_all(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let count = ctx.release_all_advisory_locks()?;
    Ok(Datum::Int(i64::try_from(count).unwrap_or(i64::MAX)))
}

// Exact executed early-branch control. Only tests may traverse its children.
#[cfg(test)]
pub(crate) fn original_eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    row: Row<'_>,
) -> Result<Datum, EvalError> {
    let name = function.func_name.lowercase();
    let value = (|| {
        if name == "values" {
            if !function.args.is_empty() {
                return Err(EvalError::WrongParameterCount("values"));
            }
            let offset = function
                .values_offset
                .ok_or(EvalError::Unsupported("VALUES offset is not initialized"))?;
            return ctx
                .current_insert_value(offset)?
                .map_or(Ok(Datum::Null), Ok);
        }
        if name == "benchmark" {
            let [count, expression] = function.args.as_slice() else {
                return Err(EvalError::Unsupported("BENCHMARK arguments"));
            };
            let Some(loop_count) =
                crate::func::benchmark_loop_count(count.eval(ctx, row)?, count.static_type(), ctx)?
            else {
                return Ok(Datum::Null);
            };
            if loop_count < 0 {
                return Ok(Datum::Null);
            }
            crate::func::ensure_benchmark_eval_type(expression.static_type())?;
            for _ in 0..loop_count {
                expression.eval(ctx, row)?;
            }
            return Ok(Datum::Int(0));
        }
        if name == "sleep" {
            let [argument] = function.args.as_slice() else {
                return Err(EvalError::WrongParameterCount("sleep"));
            };
            let value = argument.eval(ctx, row)?;
            if value.is_null() {
                ctx.handle_sleep_incorrect_argument()?;
                return Ok(Datum::Int(0));
            }
            let seconds = crate::ops::original_to_f64_with_mysql_string(&value, ctx)?;
            if seconds < 0.0 {
                ctx.handle_sleep_incorrect_argument()?;
                return Ok(Datum::Int(0));
            }
            if seconds > f64::MAX / 1_000_000_000.0 {
                return Err(EvalError::IncorrectArguments(
                    "Incorrect arguments to sleep".to_owned(),
                ));
            }
            if seconds <= 0.0 {
                return Ok(Datum::Int(0));
            }
            let duration = std::time::Duration::try_from_secs_f64(seconds).map_err(|_| {
                EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned())
            })?;
            return Ok(Datum::Int(i64::from(ctx.sleep_for(duration))));
        }
        if matches!(
            name,
            "get_lock" | "release_lock" | "is_free_lock" | "is_used_lock" | "release_all_locks"
        ) {
            if name == "release_all_locks" {
                if !function.args.is_empty() {
                    return Err(EvalError::WrongParameterCount("release_all_locks"));
                }
                let count = ctx.release_all_advisory_locks()?;
                return Ok(Datum::Int(i64::try_from(count).unwrap_or(i64::MAX)));
            }

            let expected = if name == "get_lock" { 2 } else { 1 };
            if function.args.len() != expected {
                return Err(EvalError::WrongParameterCount(match name {
                    "get_lock" => "get_lock",
                    "release_lock" => "release_lock",
                    "is_free_lock" => "is_free_lock",
                    "is_used_lock" => "is_used_lock",
                    _ => unreachable!("advisory lock name was matched above"),
                }));
            }
            let raw_name = function.args[0].eval(ctx, row)?;
            let string_name =
                crate::cast::cast_arg_as_string(&raw_name, function.args[0].static_type(), ctx)?;
            let lock_name = advisory_lock_name(string_name)?;

            return match name {
                "get_lock" => {
                    let raw_timeout = function.args[1].eval(ctx, row)?;
                    let int_timeout = crate::cast::cast_arg_as_int(
                        &raw_timeout,
                        function.args[1].static_type(),
                        ctx,
                    )?;
                    let mut timeout = crate::arg_eval_type::eval_int(&int_timeout)?.unwrap_or(0);
                    if !(0..=MAX_ADVISORY_LOCK_TIMEOUT_SECS).contains(&timeout) {
                        ctx.append_warning(
                            1292,
                            &format!("Truncated incorrect get_lock value: '{timeout}'"),
                        );
                        timeout = MAX_ADVISORY_LOCK_TIMEOUT_SECS;
                    }
                    let acquired = ctx.acquire_advisory_lock(
                        &lock_name,
                        std::time::Duration::from_secs(timeout as u64),
                    )?;
                    Ok(Datum::Int(i64::from(acquired)))
                }
                "release_lock" => Ok(Datum::Int(i64::from(
                    ctx.release_advisory_lock(&lock_name)?,
                ))),
                "is_free_lock" => Ok(Datum::Int(i64::from(
                    ctx.advisory_lock_owner(&lock_name)?.is_none(),
                ))),
                "is_used_lock" => Ok(ctx
                    .advisory_lock_owner(&lock_name)?
                    .map_or(Datum::Null, |owner| Datum::Int(owner as i64))),
                _ => unreachable!("advisory lock name was matched above"),
            };
        }
        unreachable!("the original stateful control covers its eight recorded names")
    })()?;
    super::coerce_function_result(name, function.get_static_type(), value)
}

/// The actual optional owner provider, after arity and before result coercion.
pub(crate) fn ddl_owner(argument_count: usize, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    if argument_count != 0 {
        return Err(EvalError::WrongParameterCount("tidb_is_ddl_owner"));
    }
    Ok(Datum::Int(i64::from(ctx.ddl_owner_info()?)))
}
