// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Typed consumer phases over immutable context facts and actual values.
use crate::{
    context_value::{ContextRead, ContextValue},
    expression::Expression,
    Columns, Datum, EvalError,
};
use std::borrow::Cow;
use tidb_datatype::{EvalType, FieldType};
#[derive(Debug)]
pub(crate) struct TypedContextValue {
    selector: ContextValue,
    correlated: bool,
}
#[derive(Debug)]
pub(crate) struct TypedChild<'a> {
    field: Cow<'a, FieldType>,
    target: EvalType,
}
#[derive(Debug)]
pub(crate) enum TypedRead<'a> {
    Ready(Datum),
    Child(TypedChild<'a>),
}
impl TypedChild<'_> {
    pub(crate) fn finish(self, value: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        crate::constant::typed_constant_value(&self.field, ctx, self.target, value)
    }
}
impl TypedContextValue {
    pub(crate) fn capture(expression: &Expression) -> Option<Self> {
        Some(Self {
            selector: ContextValue::capture(expression)?,
            correlated: matches!(expression, Expression::CorrelatedColumn(_)),
        })
    }
    pub(crate) fn demands_child(&self) -> bool {
        self.selector.demands_child()
    }
    pub(crate) fn validate_child_type(&self) -> Result<(), EvalError> {
        self.selector
            .target
            .as_ref()
            .map(|_| ())
            .ok_or(EvalError::Unsupported("constant has no result type"))
    }
    pub(crate) fn finish_child(
        &self,
        value: Datum,
        ctx: &dyn Columns,
        target: EvalType,
    ) -> Result<Datum, EvalError> {
        let field = self
            .selector
            .target
            .as_ref()
            .ok_or(EvalError::Unsupported("constant has no result type"))?;
        crate::constant::typed_constant_value(field, ctx, target, value)
    }
    pub(crate) fn begin_bounded(
        &self,
        ctx: &dyn Columns,
        target: EvalType,
    ) -> Result<TypedRead<'_>, EvalError> {
        if !self.correlated {
            return self.begin(ctx, Some(target));
        }
        let value = if let ContextRead::Correlated(binding) = &self.selector.read {
            if let Some(binding) = binding {
                let value = binding
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                crate::tikv::identity_actual::copy(&value)?
            } else {
                Datum::Null
            }
        } else {
            unreachable!("typed correlated selector")
        };
        // Release the live binding guard before any conversion callback.
        typed_correlated_value(self.selector.target.as_ref(), ctx, target, value)
            .map(TypedRead::Ready)
    }
    pub(crate) fn begin(
        &self,
        ctx: &dyn Columns,
        requested: Option<EvalType>,
    ) -> Result<TypedRead<'_>, EvalError> {
        if self.correlated {
            let value = self.selector.read_context(ctx)?;
            return typed_correlated_value(
                self.selector.target.as_ref(),
                ctx,
                requested.expect("correlated requested domain"),
                value,
            )
            .map(TypedRead::Ready);
        }
        let marker = match self.selector.read {
            ContextRead::Parameter(order) => Some(order),
            _ => None,
        };
        begin_constant_facts(
            marker,
            self.selector.target.as_ref(),
            self.selector.demands_child(),
            ctx,
            requested,
        )
    }
}

/// Borrow ordinary caller facts; only a genuinely inferred parameter type is
/// owned. Preparing a source descriptor still snapshots these facts once.
pub(crate) fn begin_constant<'a>(
    constant: &'a crate::constant::Constant,
    ctx: &dyn Columns,
    requested: Option<EvalType>,
) -> Result<TypedRead<'a>, EvalError> {
    begin_constant_facts(
        constant.param_marker.map(|marker| marker.order),
        constant.ret_type.as_ref(),
        constant.deferred_expr.is_some(),
        ctx,
        requested,
    )
}
fn begin_constant_facts<'a>(
    marker: Option<i64>,
    field: Option<&'a FieldType>,
    deferred: bool,
    ctx: &dyn Columns,
    requested: Option<EvalType>,
) -> Result<TypedRead<'a>, EvalError> {
    let get_field = || -> Result<Cow<'a, FieldType>, EvalError> {
        if let Some(order) = marker {
            Ok(Cow::Owned(crate::context_value::read_parameter_type(
                order, ctx,
            )?))
        } else {
            field
                .map(Cow::Borrowed)
                .ok_or(EvalError::Unsupported("constant has no result type"))
        }
    };
    let target = match requested {
        Some(target) => target,
        None => get_field()?.eval_type(),
    };
    let field = get_field()?;
    let state = TypedChild { field, target };
    if let Some(order) = marker {
        let order = usize::try_from(order)
            .map_err(|_| EvalError::Unsupported("unbound prepared parameter"))?;
        state
            .finish(ctx.param_value(order)?, ctx)
            .map(TypedRead::Ready)
    } else if deferred {
        Ok(TypedRead::Child(state))
    } else {
        unreachable!("strict constants retain their direct typed value owner")
    }
}

pub(crate) fn typed_correlated_value(
    field: Option<&FieldType>,
    ctx: &dyn Columns,
    target: EvalType,
    value: Datum,
) -> Result<Datum, EvalError> {
    use tidb_datatype::{EvalType, FieldTypeCode, StringDatum};
    if value.is_null() {
        return Ok(value);
    }
    let field = field.ok_or(EvalError::Unsupported(
        "correlated column has no result type",
    ))?;
    if target == EvalType::Int && field.is_hybrid() {
        let warnings = crate::constant::ConversionWarnings(ctx);
        let zone = ctx.time_zone();
        let context = tidb_datatype::ConversionContext::new(
            ctx.type_flags(),
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        // ToInt64's MysqlBit case reinterprets the unsigned result. Other
        // hybrids use the signed conversion's normal bounds and errors.
        if let Datum::Bit(value) = value {
            let (integer, error) = value.to_int_with_context(&context);
            return error.map_or(Ok(Datum::Int(integer as i64)), |error| {
                Err(EvalError::Conversion(error))
            });
        }

        let converted = value
            .convert_to_in_context(&FieldType::new(FieldTypeCode::LongLong), &context, &zone)
            .map_err(|_| EvalError::Unsupported("correlated integer conversion failed"))?;
        return converted.error.map_or(Ok(converted.value), |error| {
            Err(EvalError::Conversion(error))
        });
    }
    match (target, value) {
        (EvalType::Int, Datum::Int(value)) => Ok(Datum::Int(value)),
        (EvalType::Int, Datum::UInt(value)) => Ok(Datum::Int(value as i64)),
        (EvalType::Real, Datum::Real(value) | Datum::Float32(value)) => Ok(Datum::Real(value)),
        (EvalType::String, value) => Ok(Datum::String(StringDatum::new(
            value.sql_bytes().map_err(|_| {
                EvalError::Unsupported("correlated value cannot be converted to string")
            })?,
            field
                .runtime_collator_with_mode(true)
                .new_collation()
                .unwrap_or(tidb_datatype::Collation::Binary),
        ))),
        (EvalType::Decimal, value @ Datum::Decimal(_))
        | (EvalType::Datetime | EvalType::Timestamp, value @ Datum::Time(_))
        | (EvalType::Duration, value @ Datum::Duration(_))
        | (EvalType::Json, value @ Datum::Json(_))
        | (EvalType::VectorFloat32, value @ Datum::VectorFloat32(_)) => Ok(value),
        _ => Err(EvalError::Unsupported(
            "correlated value is not in its requested evaluation domain",
        )),
    }
}

#[cfg(test)]
mod work_tests {
    use super::*;
    #[test]
    fn typed_work_string_formatting_is_bounded_at_the_original_effect_phase() {
        let mut failures = Vec::new();
        for (correlated, target) in [
            (false, EvalType::String),
            (true, EvalType::String),
            (false, EvalType::Real),
        ] {
            let value = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
                false,
                vec![b'1'],
                1_000_000,
                1_000_000,
            ));
            let ctx = crate::tikv::dynamic_leaf_tests::Context::new(0, Datum::Null);
            let field = FieldType::new(if target == EvalType::Real {
                tidb_datatype::FieldTypeCode::Enum
            } else {
                tidb_datatype::FieldTypeCode::VarString
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tidb_query_expr::local::with_value_kernel_work_limit(65536, || {
                    if correlated {
                        typed_correlated_value(Some(&field), &ctx, target, value)
                    } else {
                        crate::constant::typed_constant_value(&field, &ctx, target, value)
                    }
                })
            }));
            if !result.is_err() {
                failures.push(format!(
                    "typed formatting correlated={correlated} target={target:?} {result:?}"
                ));
            }
            assert_eq!(
                *ctx.events.borrow(),
                if correlated {
                    Vec::<String>::new()
                } else {
                    vec!["zone".into(), "flags".into()]
                }
            );
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        }
        assert!(failures.is_empty(), "{failures:?}");
    }
    #[test]
    fn typed_numeric_warning_rendering_is_bounded_before_diagnostic_allocation() {
        let binary = tidb_datatype::BinaryLiteral::from(vec![255; 32768]);
        let json =
            tidb_datatype::BinaryJSON::parse(&format!("[\"{}\"]", "x".repeat(32768))).unwrap();
        let mut gaps = vec![];
        let describe =
            |result: std::thread::Result<std::result::Result<Datum, EvalError>>| match result {
                Ok(value) => format!("RESULT:{value:?}"),
                Err(payload) => format!(
                    "PANIC:{:?}",
                    payload
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| payload.downcast_ref::<&str>().copied())
                ),
            };

        for (label, value, field, target, correlated) in [
            (
                "binary_int",
                Datum::BinaryLiteral(binary.clone()),
                FieldType::new(tidb_datatype::FieldTypeCode::Bit),
                EvalType::Int,
                false,
            ),
            (
                "binary_real",
                Datum::BinaryLiteral(binary.clone()),
                FieldType::new(tidb_datatype::FieldTypeCode::Bit),
                EvalType::Real,
                false,
            ),
            (
                "binary_decimal",
                Datum::BinaryLiteral(binary.clone()),
                FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal),
                EvalType::Decimal,
                false,
            ),
            (
                "correlated_bit_int",
                Datum::Bit(binary),
                FieldType::new(tidb_datatype::FieldTypeCode::Bit),
                EvalType::Int,
                true,
            ),
            (
                "json_decimal",
                Datum::Json(json),
                FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal),
                EvalType::Decimal,
                false,
            ),
        ] {
            let ctx = crate::tikv::dynamic_leaf_tests::Context::new(0, Datum::Null);
            let native = crate::tikv::dynamic_leaf_tests::Context::new(0, Datum::Null);
            let native_value = value.clone();
            let expected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if correlated {
                    typed_correlated_value(Some(&field), &native, target, native_value)
                } else {
                    crate::constant::typed_constant_value(&field, &native, target, native_value)
                }
            }));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tidb_query_expr::local::with_value_kernel_work_limit(65536, || {
                    if correlated {
                        typed_correlated_value(Some(&field), &ctx, target, value)
                    } else {
                        crate::constant::typed_constant_value(&field, &ctx, target, value)
                    }
                })
            }));
            if describe(result) != describe(expected) {
                gaps.push(label);
            }
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
            assert_eq!(*ctx.events.borrow(), *native.events.borrow(), "{label}");
        }
        assert!(
            gaps.is_empty(),
            "removed work policy changed native diagnostics: {gaps:?}"
        );
        let ctx = crate::tikv::dynamic_leaf_tests::Context::new(0, Datum::Null);
        let null = tidb_query_expr::local::with_value_kernel_work_limit(1, || {
            crate::constant::typed_constant_value(
                &FieldType::new(tidb_datatype::FieldTypeCode::Null),
                &ctx,
                EvalType::Int,
                Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![255; 1 << 20])),
            )
        })
        .unwrap();
        assert_eq!(null, Datum::Null);
        assert!(ctx.events.borrow().is_empty());
        let zero = tidb_query_expr::local::with_value_kernel_work_limit(65536, || {
            crate::constant::typed_constant_value(
                &FieldType::new(tidb_datatype::FieldTypeCode::Bit),
                &ctx,
                EvalType::Int,
                Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![0; 32768])),
            )
        })
        .unwrap();
        assert_eq!(zero, Datum::Int(0));
        assert_eq!(*ctx.events.borrow(), ["zone", "flags"]);
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
    }
}
