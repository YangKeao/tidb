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
use tidb_datatype::Decimal;
use tidb_query_expr::NativeCastDecimalInput as Input;

fn input(value: &Datum) -> Input<'_> {
    match value {
        Datum::Decimal(value) => Input::Decimal(value.as_shared_parse()),
        Datum::Int(value) => Input::Int(*value),
        Datum::UInt(value) => Input::UInt(*value),
        Datum::Real(value) => Input::Real(*value),
        Datum::String(value) => Input::String(value.bytes()),
        Datum::Bytes(value) => Input::Bytes(value),
        Datum::Float32(value) => Input::Float32(*value),
        _ => Input::Other,
    }
}

/// The ordinary cast caller retains its original NULL/range/vector guards.
/// Conversion decisions, warning order, error folding and precision policy
/// belong to the SDK; these closures actuate only the original primitives.
pub(crate) fn eval_cast_decimal_in(
    ctx: &dyn Columns,
    value: &Datum,
    flen: u32,
    scale: u32,
) -> Result<Datum, EvalError> {
    let converted = tidb_query_expr::native_cast_decimal(
        input(value),
        flen,
        scale,
        || {
            value
                .to_decimal()
                .map(|converted| (converted.value.into_shared_parse(), converted.event))
        },
        |code, message| ctx.append_warning(code, message),
    );
    Ok(Datum::Decimal(Decimal::from_shared_parse(converted)))
}

/// The existing UNION helper needs only the original input diagnostic. It
/// retains its own separate conversion domain and negative-input handling.
pub(crate) fn report_cast_decimal_input_in(ctx: &dyn Columns, value: &Datum) {
    tidb_query_expr::native_cast_decimal_input_warning(input(value), |code, message| {
        ctx.append_warning(code, message)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy};
    use std::cell::RefCell;
    use tidb_datatype::{BinaryJSON, ConversionFlags, DateModes, SessionTimeZone};

    #[test]
    fn decimal_cast_bridge_keeps_raw_parts_warning_order_default_conversion_and_union_report() {
        #[derive(Default)]
        struct Original {
            warnings: RefCell<Vec<(u16, String)>>,
        }
        impl Columns for Original {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("already-evaluated operand")
            }
            fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
                panic!("original cast appends directly")
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                panic!("default conversion does not consult statement policy")
            }
            fn type_flags(&self) -> ConversionFlags {
                panic!("no statement conversion context")
            }
            fn date_modes(&self) -> DateModes {
                panic!("no new temporal policy")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("no new temporal context")
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        fn decimal(value: Datum) -> Decimal {
            let Datum::Decimal(value) = value else {
                panic!("DECIMAL result");
            };
            value
        }
        let original = Original::default();
        // This preserves the former pure path, not a new admission capability.
        let owner = AsciiPoolOwner::new(
            AsciiPoolPolicy::checked(0, 0, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16).unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        scope.with_columns(&original, |bound| {
            let cast = |value: &Datum, flen, scale| {
                decimal(eval_cast_decimal_in(bound, value, flen, scale).unwrap())
            };
            let unspec = crate::cast::UNSPECIFIED_CAST_SCALE;
            assert_eq!(
                cast(&Datum::new_bytes(b"999.9x".to_vec()), 2, 0),
                Decimal::from_int(99)
            );
            assert_eq!(
                original.warnings.take(),
                vec![
                    (
                        1292,
                        "Truncated incorrect DECIMAL value: '999.9x'".to_owned()
                    ),
                    (1690, "DECIMAL value is out of range in '(2, 0)'".to_owned()),
                ]
            );
            let unicode = Datum::new_bytes("\u{2003}12.5\u{2003}".as_bytes().to_vec());
            assert_eq!(cast(&unicode, 0, unspec), Decimal::from_int(0));
            assert!(original.warnings.take().is_empty());
            // The retained UNION caller parses its Unicode-trimmed input,
            // unlike ordinary CAST's raw String/Bytes conversion above.
            let union =
                crate::func::eval_func_values("cast_string_to_decimal_in_union", &[unicode], bound)
                    .unwrap()
                    .unwrap();
            assert_eq!(decimal(union), Decimal::parse_mysql("12.5").0);
            assert!(original.warnings.take().is_empty());
            let union = crate::func::eval_func_values(
                "cast_string_to_decimal_in_union",
                &[Datum::new_bytes(b"12.5x".to_vec())],
                bound,
            )
            .unwrap()
            .unwrap();
            assert_eq!(decimal(union), Decimal::parse_mysql("12.5").0);
            assert_eq!(
                original.warnings.take(),
                vec![(
                    1292,
                    "Truncated incorrect DECIMAL value: '12.5x'".to_owned()
                ),]
            );
            let invalid_utf8 = Datum::new_bytes(vec![0xff]);
            report_cast_decimal_input_in(bound, &invalid_utf8);
            assert_eq!(cast(&invalid_utf8, 0, unspec), Decimal::from_int(0));
            assert!(original.warnings.take().is_empty());
            assert_eq!(
                cast(&Datum::Real(0.1), 0, unspec),
                Decimal::parse_mysql("0.1").0
            );
            assert_eq!(
                cast(&Datum::Float32(0.1), 0, unspec),
                Decimal::parse_mysql("0.10000000149011612").0
            );
            let unsupported = Datum::Raw(vec![1]);
            assert!(unsupported.to_decimal().is_err());
            assert_eq!(cast(&unsupported, 0, unspec), Decimal::from_int(0));
            let json_null = Datum::Json(BinaryJSON::parse("null").unwrap());
            assert!(json_null.to_decimal().unwrap().event.is_some());
            assert_eq!(cast(&json_null, 0, unspec), Decimal::from_int(0));
            assert!(original.warnings.take().is_empty());
            let source = Decimal::from_raw_parts(true, b"0001234567".to_vec(), 2, 4)
                .with_declared_shape(20, 4);
            let actual = cast(&Datum::Decimal(source.clone()), 2, unspec);
            assert_eq!(actual.is_negative(), source.is_negative());
            assert_eq!(actual.coefficient_bytes(), source.coefficient_bytes());
            assert_eq!(actual.scale(), source.scale());
            assert_eq!(actual.storage_scale(), source.storage_scale());
            assert_eq!(actual.declared_shape(), source.declared_shape());
            assert!(original.warnings.take().is_empty());
        });
        drop(scope);
        execution.close();
    }
}
