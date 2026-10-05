// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{Columns, Datum, EvalError};
use tidb_query_expr::{NativeCastFloatInput as Input, NativeCastFloatTarget as Target};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{BinaryJSON, ConversionFlags, DateModes, SessionTimeZone};

    #[test]
    fn float_cast_bridge_keeps_conversion_demands_parser_domains_and_context_error_order() {
        #[derive(Default)]
        struct Original {
            warnings: RefCell<Vec<String>>,
            reject: Cell<bool>,
        }
        impl Columns for Original {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("operand already evaluated")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("no floating-cast zone demand")
            }
            fn date_modes(&self) -> DateModes {
                panic!("no floating-cast date mode")
            }
            fn type_flags(&self) -> ConversionFlags {
                panic!("actual default conversion, not statement flags")
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                panic!("use original truncate callback")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("do not bypass truncate callback")
            }
            fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
                self.warnings.borrow_mut().push(message.to_owned());
                if self.reject.get() {
                    Err(EvalError::Unsupported("float truncate veto"))
                } else {
                    Ok(())
                }
            }
        }
        let float32 = Datum::Float32(16_777_217.0);
        let calls = Cell::new(0);
        let value = tidb_query_expr::native_cast_float(
            input(&float32),
            Target::Double,
            || panic!("not JSON"),
            || {
                calls.set(calls.get() + 1);
                float32
                    .to_f64()
                    .map(|converted| (converted.value, converted.event))
            },
            |_| -> Result<(), EvalError> {
                panic!("default conversion has no statement diagnostic")
            },
        )
        .unwrap();
        assert_eq!(value, 16_777_216.0);
        assert_eq!(calls.get(), 1);
        let value = tidb_query_expr::native_cast_float(
            input(&Datum::Real(1e300)),
            Target::Float,
            || panic!("not JSON"),
            || -> Result<(f64, ()), ()> {
                panic!("raw Real FLOAT must not request default conversion")
            },
            |_| -> Result<(), EvalError> { panic!("raw Real FLOAT overflow is silent") },
        )
        .unwrap();
        assert_eq!(value.to_bits(), 0.0f64.to_bits());
        let owner = AsciiPoolOwner::new(
            AsciiPoolPolicy::checked(0, 0, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16).unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let original = Original::default();
        scope.with_columns(&original, |bound| {
            // Zero slots preserves the old pure path; it adds no C4 admission.
            let malformed = Datum::new_bytes(vec![b'1', 0xff]);
            assert_eq!(eval_cast_float_value(&malformed), 0.0);
            assert_eq!(eval_cast_double_in(bound, &malformed), Ok(1.0));
            assert_eq!(
                original.warnings.take(),
                vec!["Truncated incorrect DOUBLE value: '1\u{fffd}'"]
            );
            assert_eq!(
                eval_cast_double_in(bound, &Datum::new_bytes(b"1e".to_vec())),
                Ok(1.0)
            );
            assert_eq!(
                eval_cast_double_in(bound, &Datum::new_bytes(b"1\0garbage".to_vec())),
                Ok(1.0)
            );
            assert!(original.warnings.take().is_empty());
            assert_eq!(
                eval_cast_double_in(bound, &Datum::new_bytes(b"e".to_vec())),
                Ok(0.0)
            );
            assert_eq!(
                original.warnings.take(),
                vec!["Truncated incorrect DOUBLE value: 'e'"]
            );
            assert_eq!(
                eval_cast_double_in(bound, &Datum::new_bytes(b"\0 12".to_vec())),
                Ok(0.0)
            );
            assert_eq!(
                original.warnings.take(),
                vec!["Truncated incorrect DOUBLE value: ''"]
            );
            let json_string = Datum::Json(BinaryJSON::parse("\"1\"").unwrap());
            assert_eq!(eval_cast_float_value(&json_string), 1.0);
            assert_eq!(eval_cast_double_in(bound, &json_string), Ok(0.0));
            assert_eq!(
                original.warnings.take(),
                vec!["Truncated incorrect FLOAT value: '\"1\"'"]
            );
            let overflow = Datum::new_bytes(b"1e300x".to_vec());
            original.reject.set(true);
            assert_eq!(
                eval_cast_float_in(bound, &overflow),
                Err(EvalError::Unsupported("float truncate veto"))
            );
            assert_eq!(
                original.warnings.take(),
                vec!["Truncated incorrect DOUBLE value: '1e300x'"]
            );
            original.reject.set(false);
            assert_eq!(
                eval_cast_float_in(bound, &overflow),
                Err(EvalError::ConstantFloatCastOverflow {
                    value: "1e+300".into()
                })
            );
            assert_eq!(
                original.warnings.take(),
                vec!["Truncated incorrect DOUBLE value: '1e300x'"]
            );
            assert_eq!(
                eval_cast_float_in(bound, &Datum::Real(16_777_217.0)),
                Ok(16_777_216.0)
            );
            assert_eq!(
                eval_cast_float_in(bound, &Datum::Int(16_777_217)),
                Ok(16_777_217.0)
            );
            assert_eq!(
                eval_cast_float_in(bound, &Datum::new_bytes(b"16777217".to_vec())),
                Ok(16_777_217.0)
            );
            assert_eq!(eval_cast_double_in(bound, &float32), Ok(16_777_216.0));
            assert_eq!(
                eval_cast_double_in(bound, &Datum::Real(16_777_217.0)),
                Ok(16_777_217.0)
            );
            for value in [
                Datum::Real(f64::NEG_INFINITY),
                Datum::Float32(f64::INFINITY),
            ] {
                assert_eq!(
                    eval_cast_float_in(bound, &value).unwrap().to_bits(),
                    0.0f64.to_bits()
                );
            }
            let nan = f64::from_bits(0xfff8_0000_1234_5678);
            assert_eq!(
                eval_cast_double_in(bound, &Datum::Real(nan))
                    .unwrap()
                    .to_bits(),
                nan.to_bits()
            );
            let narrowed = eval_cast_float_in(bound, &Datum::Float32(nan)).unwrap();
            assert!(narrowed.is_nan());
            assert!(narrowed.is_sign_negative());
            assert_eq!(
                eval_cast_float_in(bound, &Datum::Real(-0.0))
                    .unwrap()
                    .to_bits(),
                (-0.0f64).to_bits()
            );
            let raw = Datum::Raw(vec![1]);
            assert!(raw.to_f64().is_err());
            assert_eq!(eval_cast_double_in(bound, &raw), Ok(0.0));
            let json_null = Datum::Json(BinaryJSON::parse("null").unwrap());
            assert!(json_null.to_f64().unwrap().event.is_some());
            assert_eq!(eval_cast_float_value(&json_null), 0.0);
            assert!(original.warnings.take().is_empty());
        });
        drop(scope);
        execution.close();
        assert!(std::panic::catch_unwind(|| eval_cast_float_value(&Datum::Null)).is_err());
    }
}

fn input(value: &Datum) -> Input<'_> {
    match value {
        Datum::Null => Input::Null,
        Datum::MinNotNull => Input::MinNotNull,
        Datum::MaxValue => Input::MaxValue,
        Datum::Int(value) => Input::Int(*value),
        Datum::UInt(value) => Input::UInt(*value),
        Datum::Decimal(value) => Input::Decimal(value.as_shared_parse()),
        Datum::Real(value) => Input::Real(*value),
        Datum::Float32(value) => Input::Float32(*value),
        Datum::String(value) => Input::String(value.bytes()),
        Datum::Bytes(value) => Input::Bytes(value),
        Datum::Json(_) => Input::Json,
        _ => Input::Other,
    }
}

fn evaluate(ctx: &dyn Columns, value: &Datum, target: Target) -> Result<f64, EvalError> {
    tidb_query_expr::native_cast_float(
        input(value),
        target,
        || {
            let Datum::Json(value) = value else {
                unreachable!("SDK JSON display request needs an actual JSON datum");
            };
            value.to_string()
        },
        || {
            value
                .to_f64()
                .map(|converted| (converted.value, converted.event))
        },
        |message| ctx.handle_truncate(message),
    )
    .map_err(|error| match error {
        tidb_query_expr::NativeCastFloatError::Child(error) => error,
        tidb_query_expr::NativeCastFloatError::ConstantFloatCastOverflow { value } => {
            EvalError::ConstantFloatCastOverflow { value }
        }
    })
}

pub(crate) fn eval_cast_double_in(ctx: &dyn Columns, value: &Datum) -> Result<f64, EvalError> {
    evaluate(ctx, value, Target::Double)
}

pub(crate) fn eval_cast_float_in(ctx: &dyn Columns, value: &Datum) -> Result<f64, EvalError> {
    evaluate(ctx, value, Target::Float)
}

/// This is the distinct strict-UTF-8 value-only surface, not the ordinary
/// lossy string/JSON cast with a statement warning callback.
pub(crate) fn eval_cast_float_value(value: &Datum) -> f64 {
    tidb_query_expr::native_cast_float_value(input(value), || {
        value
            .to_f64()
            .map(|converted| (converted.value, converted.event))
    })
}
