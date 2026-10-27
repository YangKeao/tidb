// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Public typed entry adapters. Child traversal belongs only to the SDK graph.
use super::legacy_frame::{Plan, PrepareError};
use super::legacy_frame_value::{Value, View};
use super::*;

impl PrepareError {
    pub(super) fn into_legacy(self) -> LegacyEvalError {
        use tidb_query_expr::local::LocalError;
        let error = match self {
            Self::Native(error) => return error.into(),
            Self::Local(error) => error,
            Self::Limit => {
                LocalError::ResourceLimit("legacy source metadata allowance exceeded".into())
            }
            Self::Contract(message) => LocalError::HostContract(message.into()),
        };
        tidb_expr::ComposedExpressionProgram::source_preparation_failure(error).into()
    }
}
impl LegacyEvaluator<'_> {
    fn source_value(&self, source: &SimpleExpr, view: View) -> LegacyResult<Value> {
        Plan::prepare(source, view, self.row.len())
            .map_err(PrepareError::into_legacy)?
            .evaluate(self)
    }
    pub(super) fn eval_expr(&self, source: &SimpleExpr) -> LegacyResult<Option<i128>> {
        match self.source_value(source, View::Int)? {
            Value::Int(value) => Ok(value),
            _ => Err(LegacyEvalError::InvalidResult(
                "legacy integer result view differs",
            )),
        }
    }
    #[cfg(test)]
    pub(super) fn eval_datum(&self, source: &SimpleExpr) -> LegacyResult<tidb_datatype::Datum> {
        match self.source_value(source, View::Datum)? {
            Value::Datum(value) => Ok(value),
            _ => Err(LegacyEvalError::InvalidResult(
                "legacy datum result view differs",
            )),
        }
    }
}
#[cfg(test)]
macro_rules! typed_entry {
    ($method:ident, $view:ident, $kind:ty) => {
        impl LegacyEvaluator<'_> {
            pub(super) fn $method(
                &self,
                source: Option<&SimpleExpr>,
            ) -> LegacyResult<Option<$kind>> {
                let Some(source) = source else {
                    return Ok(None);
                };
                match self.source_value(source, View::$view)? {
                    Value::$view(value) => Ok(value),
                    _ => Err(LegacyEvalError::InvalidResult(
                        "legacy typed result view differs",
                    )),
                }
            }
        }
    };
}
#[cfg(test)]
typed_entry!(eval_decimal, Decimal, tidb_datatype::Decimal);
#[cfg(test)]
typed_entry!(eval_json, Json, tidb_datatype::BinaryJSON);
#[cfg(test)]
typed_entry!(eval_real, Real, f64);
#[cfg(test)]
typed_entry!(eval_duration, Duration, tidb_datatype::MySqlDuration);
#[cfg(test)]
typed_entry!(eval_time, Time, tidb_datatype::Time);
#[cfg(test)]
typed_entry!(eval_bytes, Bytes, Vec<u8>);
