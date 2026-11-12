// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Live value selectors and the original deferred conversion over actual data.
use crate::{constant::ConversionWarnings, expression::Expression, Columns, Datum, EvalError};
use std::sync::{Arc, RwLock};
use tidb_datatype::FieldType;
#[derive(Debug)]
pub(crate) enum ContextRead {
    Parameter(i64),
    Failure(&'static str),
    Deferred,
    Correlated(Option<Arc<RwLock<Datum>>>),
}
#[derive(Debug)]
pub(crate) struct ContextValue {
    pub(crate) read: ContextRead,
    pub(crate) target: Option<FieldType>,
    pub(crate) convert_deferred: bool,
    pub(crate) root_broadcast: bool,
}
impl ContextValue {
    pub(crate) fn capture(expression: &Expression) -> Option<Self> {
        match expression {
            Expression::Constant(c) if c.param_marker.is_some() || c.deferred_expr.is_some() => {
                Some(Self::for_constant(c))
            }
            Expression::CorrelatedColumn(c) => Some(Self {
                read: ContextRead::Correlated(c.data.as_ref().map(Arc::clone)),
                target: c.column.ret_type.clone(),
                convert_deferred: false,
                root_broadcast: false,
            }),
            _ => None,
        }
    }
    pub(crate) fn for_constant(c: &crate::constant::Constant) -> Self {
        Self {
            read: match c.param_marker {
                Some(marker) => ContextRead::Parameter(marker.order),
                None => ContextRead::Deferred,
            },
            target: c.ret_type.clone(),
            convert_deferred: c.deferred_expr.is_some(),
            root_broadcast: c.deferred_expr.is_none(),
        }
    }
    pub(crate) fn demands_child(&self) -> bool {
        matches!(self.read, ContextRead::Deferred)
    }
    pub(crate) fn read_context(&self, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        match &self.read {
            ContextRead::Parameter(order) => {
                let order = usize::try_from(*order)
                    .map_err(|_| EvalError::Unsupported("unbound prepared parameter"))?;
                ctx.param_value(order)
            }
            ContextRead::Correlated(binding) => Ok(read_correlated(binding.as_ref())),
            ContextRead::Failure(message) => Err(EvalError::Unsupported(message)),
            ContextRead::Deferred => {
                unreachable!("deferred value is supplied by the shared driver")
            }
        }
    }
    pub(crate) fn finish_expression(
        &self,
        value: Datum,
        ctx: &dyn Columns,
    ) -> Result<Datum, (Datum, EvalError)> {
        let value = self.finish_raw(value, ctx)?;
        Ok(crate::expression::apply_enum_set_integer_view(
            value,
            self.target.as_ref(),
        ))
    }
    pub(crate) fn finish_raw(
        &self,
        value: Datum,
        ctx: &dyn Columns,
    ) -> Result<Datum, (Datum, EvalError)> {
        if self.convert_deferred {
            finish_deferred_value(value, self.target.as_ref(), ctx)
        } else {
            Ok(value)
        }
    }
}
pub(crate) fn read_correlated(binding: Option<&Arc<RwLock<Datum>>>) -> Datum {
    binding.map_or(Datum::Null, |data| {
        data.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    })
}
/// Original generic Constant conversion. NULL and Decimal have different demand
/// from other values; error results preserve the actual unconverted Datum.
pub(crate) fn finish_deferred_value(
    value: Datum,
    target: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Datum, (Datum, EvalError)> {
    if value.is_null() {
        return Ok(value);
    }
    let target = target.ok_or_else(|| {
        (
            value.clone(),
            EvalError::Unsupported("deferred constant has no result type"),
        )
    })?;
    if let Datum::Decimal(value) = value {
        return Ok(Datum::Decimal(
            if i64::from(value.precision_and_frac().1) < target.decimal() {
                value.round_to_scale(target.decimal() as i32)
            } else {
                value
            },
        ));
    }
    let warnings = ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted = value
        .convert_to_in_context(target, &context, &zone)
        .map_err(|_| {
            (
                value.clone(),
                EvalError::Unsupported("deferred conversion diagnostic routing is not yet ported"),
            )
        })?;
    if let Some(error) = converted.error {
        return Err((value, EvalError::Conversion(error)));
    }
    Ok(converted.value)
}

/// Original deferred-root admission reads a parameter for type before rows,
/// even when its child is ultimately outside the typed native closure.
pub(crate) fn read_parameter_type(order: i64, ctx: &dyn Columns) -> Result<FieldType, EvalError> {
    let order =
        usize::try_from(order).map_err(|_| EvalError::Unsupported("unbound prepared parameter"))?;
    let value = ctx.param_value(order)?;
    // Display-width inference follows the original parameter callback.
    Ok(tidb_datatype::infer_param_type_from_datum(&value))
}
pub(crate) fn probe_parameter_type(order: i64, ctx: &dyn Columns) -> Result<(), EvalError> {
    read_parameter_type(order, ctx).map(|_| ())
}
