// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual numeric conversion pauses before its demanded diagnostic and warning.
use super::*;
#[derive(Debug)]
pub(crate) enum Step {
    Ready(Datum),
    Subject(Pending),
}
#[derive(Debug)]
pub(crate) struct Pending {
    state: tidb_query_expr::NativeNumericArgumentRealDecimal,
    target_code: u8,
}
impl Pending {
    pub(crate) fn finish(
        self,
        field: &FieldType,
        subject: Option<&str>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let decimal = self
            .state
            .finish(subject, |message| ctx.handle_truncate(message))?;
        finish_numeric_argument(
            Datum::Decimal(tidb_datatype::Decimal::from_shared_parse(decimal)),
            field,
            self.target_code,
            ctx,
        )
    }
}
pub(crate) fn prepare(
    field: &FieldType,
    value: Datum,
    target: EvalType,
    ctx: &dyn Columns,
    vectorized: bool,
) -> Result<Step, EvalError> {
    use tidb_query_expr::{
        NativeNumericArgumentHead as Head, NativeNumericArgumentNormalization as Normalization,
        NativeNumericArgumentRoute as Route,
    };
    let (normalization, route, target_code) = match tidb_query_expr::native_numeric_argument_head(
        value.as_shared_numeric_input(),
        field.eval_type(),
        target,
        field.is_unsigned(),
        field.is_hybrid(),
    )
    .map_err(EvalError::Unsupported)?
    {
        Head::Preserve => return Ok(Step::Ready(value)),
        Head::Cast {
            normalization,
            route,
            target_code,
        } => (normalization, route, target_code),
    };
    let value = match normalization {
        Normalization::Keep => value,
        Normalization::UInt(value) => Datum::UInt(value),
        Normalization::Real(value) => Datum::Real(value),
    };
    let value = match route {
        Route::String => {
            let bytes = match &value {
                Datum::String(value) => value.bytes(),
                Datum::Bytes(value) => value.as_slice(),
                _ => unreachable!("SDK string route requires actual string bytes"),
            };
            return cast_string_numeric_argument(field, bytes, target, ctx, vectorized)
                .map(Step::Ready);
        }
        Route::JsonReal => {
            let Datum::Json(json) = &value else {
                unreachable!("SDK JSON real route requires an actual JSON datum");
            };
            return tidb_query_expr::native_numeric_argument_json_to_f64(
                json.type_code(),
                json.value(),
                |message| ctx.handle_truncate(message),
            )
            .map(Datum::Real)
            .map(Step::Ready);
        }
        Route::JsonInt => {
            let Datum::Json(json) = &value else {
                unreachable!("SDK JSON integer route requires an actual JSON datum");
            };
            return tidb_query_expr::native_numeric_argument_json_to_i64(
                json.type_code(),
                json.value(),
                |message| ctx.handle_truncate(message),
            )
            .map(Datum::Int)
            .map(Step::Ready);
        }
        Route::ContextDecimal => {
            let warnings = crate::constant::ConversionWarnings(ctx);
            let zone = ctx.time_zone();
            let context = tidb_datatype::ConversionContext::new(
                ctx.type_flags(),
                tidb_datatype::ConversionLocation::from_time_zone(&zone),
                &warnings,
            );
            let decimal = tidb_query_expr::native_numeric_argument_context_decimal_result(
                value.to_decimal_with_context(&context),
            )
            .map_err(numeric_argument_result_error)?;
            Datum::Decimal(decimal)
        }
        Route::RealDecimal => {
            let Datum::Real(real) = &value else {
                unreachable!("SDK real decimal route requires an actual normalized real datum");
            };
            use tidb_query_expr::NativeNumericArgumentLevel;
            let state =
                tidb_query_expr::native_numeric_argument_real_decimal_prepare(*real, || match ctx
                    .truncate_level()
                {
                    crate::ErrorLevel::Error => NativeNumericArgumentLevel::Error,
                    crate::ErrorLevel::Warn => NativeNumericArgumentLevel::Warn,
                    crate::ErrorLevel::Ignore => NativeNumericArgumentLevel::Ignore,
                })
                .map_err(|kind| EvalError::Conversion(numeric_argument_conversion_error(kind)))?;
            if state.requires_expression_subject() {
                return Ok(Step::Subject(Pending { state, target_code }));
            }
            let decimal = state.finish(None, |message| ctx.handle_truncate(message))?;
            Datum::Decimal(tidb_datatype::Decimal::from_shared_parse(decimal))
        }
        Route::Fit => value,
    };
    finish_numeric_argument(value, field, target_code, ctx).map(Step::Ready)
}
