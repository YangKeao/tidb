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

//! Go extractFunctionClass and its datetime/duration signatures.

use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;

pub(crate) fn extract(
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_extract_in(ctx, unit, value, source)
}

#[cfg(test)]
#[test]
fn extract_entries_keep_fresh_modes_metadata_and_composite_boundaries() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        CoreTime, DateModes, FieldTypeCode, MySqlDuration, SessionTimeZone, Time, TimeType,
    };
    struct Demand {
        unit: Datum,
        value: Datum,
        modes: [bool; 2],
        mode_reads: Cell<usize>,
        fail_value: bool,
        events: RefCell<Vec<&'static str>>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.events
                .borrow_mut()
                .push(if index == 0 { "unit" } else { "value" });
            if index == 0 {
                return Ok(self.unit.clone());
            }
            if self.fail_value {
                return Err(EvalError::Unsupported("EXTRACT child"));
            }
            Ok(self.value.clone())
        }
        fn date_modes(&self) -> DateModes {
            let index = self.mode_reads.get();
            self.mode_reads.set(index + 1);
            self.events.borrow_mut().push("modes");
            DateModes {
                allow_invalid_dates: self.modes[index],
                ..DateModes::default()
            }
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone");
            SessionTimeZone::utc()
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events.borrow_mut().push("warning");
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("selected fixtures must not request truncation policy")
        }
    }
    let text = |s: &str| Datum::new_string(s);
    let context = |unit, value, modes| Demand {
        unit,
        value,
        modes,
        mode_reads: Cell::new(0),
        fail_value: false,
        events: RefCell::new(Vec::new()),
        warnings: RefCell::new(Vec::new()),
    };
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate =
        |mode, unit: &str, value: &Datum, source: Option<&FieldType>, columns: &dyn Columns| {
            if mode == 0 {
                return extract(unit, value, source, columns);
            }
            if mode == 1 {
                return crate::eval_in(
                    &tidb_ast::Expr::Extract {
                        unit: unit.to_owned(),
                        value: Box::new(tidb_ast::Expr::Column(vec!["1".to_owned()])),
                    },
                    columns,
                );
            }
            let args = [
                FieldType::new(FieldTypeCode::VarString),
                source
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::VarString)),
            ]
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                let mut constant = Constant::new(Datum::Null, field);
                constant.param_marker = Some(ParamMarker {
                    order: i64::try_from(index).unwrap(),
                });
                Expression::Constant(constant)
            })
            .collect();
            ScalarFunction::new(
                tidb_ast::CiString::new("extract"),
                FieldType::new(FieldTypeCode::LongLong),
                args,
            )
            .eval(columns, row.to_row())
        };
    let prefix = |mode| match mode {
        0 => vec![],
        1 => vec!["value"],
        _ => vec!["unit", "value"],
    };
    let invalid_unit = |unit: &str| {
        EvalError::Conversion(tidb_error::terror::TerrorError::compatible(
            tidb_error::terror::TerrorCode::new(1105),
            format!("invalid unit {unit}"),
        ))
    };
    let owner = |slots| {
        crate::ReadyValuePoolOwner::new(
            crate::ReadyValuePoolPolicy::checked(
                slots,
                slots,
                16 * 1024 * 1024,
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                64,
                8,
                4 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap()
    };
    let pool = owner(1);
    let execution = pool.begin_execution().unwrap();
    let unknown = FieldType::new(FieldTypeCode::Unknown(12));
    for mode in 0..3 {
        for (modes, expected) in [
            ([true, false], Some(10_203)),
            ([true, true], Some(29_010_203)),
            ([false, true], None),
        ] {
            let ctx = context(text("DAY_SECOND"), text("2021-02-29 01:02:03"), modes);
            let result = execution.scope().with_columns(&ctx, |cols| {
                evaluate(mode, "DAY_SECOND", &ctx.value, Some(&unknown), cols)
            });
            let mut events = prefix(mode);
            events.push("modes");
            if let Some(value) = expected {
                assert_eq!(result, Ok(Datum::Int(value)));
                events.push("modes");
            } else {
                assert!(matches!(result, Err(EvalError::Conversion(_))));
            }
            assert_eq!(*ctx.events.borrow(), events);
            assert!(ctx.warnings.borrow().is_empty());
        }
        for (unit, value, expected) in [
            ("bad", Datum::Null, Ok(Datum::Null)),
            ("DAY_SECOND", Datum::Null, Ok(Datum::Null)),
            ("HOUR", Datum::Null, Ok(Datum::Null)),
            (
                "DAY_MICROSECOND",
                Datum::Time(Time::from_raw_parts(
                    CoreTime::from_date(2020, 1, 2, 3, 4, 5, 123456),
                    TimeType::Date,
                    0,
                )),
                Ok(Datum::Int(2_030_405_123_456)),
            ),
            (
                "DAY_MICROSECOND",
                Datum::Duration(MySqlDuration::from_raw_parts(-11_045_123_456_000, -2)),
                Ok(Datum::Int(-30_405_123_456)),
            ),
            (
                "bad",
                Datum::Time(Time::from_raw_parts(
                    CoreTime::default(),
                    TimeType::Timestamp,
                    255,
                )),
                Err(invalid_unit(if mode == 2 { "BAD" } else { "bad" })),
            ),
            (
                "DAY_SECOND",
                Datum::new_bytes(vec![255]),
                Err(EvalError::Unsupported("invalid UTF-8 byte datum")),
            ),
        ] {
            let ctx = context(text(unit), value, [false, false]);
            assert_eq!(
                execution.scope().with_columns(&ctx, |cols| evaluate(
                    mode,
                    unit,
                    &ctx.value,
                    Some(&unknown),
                    cols
                )),
                expected
            );
            assert_eq!(*ctx.events.borrow(), prefix(mode));
            assert!(ctx.warnings.borrow().is_empty());
        }
    }
    // Unlike Unknown(12), the actual Datetime variant selects the existing
    // datetime cast, whose one mode read and timezone access remain in place.
    let datetime = FieldType::new(FieldTypeCode::Datetime);
    for mode in [0, 2] {
        let ctx = context(
            text("DAY_SECOND"),
            text("2021-02-29 01:02:03"),
            [true, false],
        );
        assert_eq!(
            execution.scope().with_columns(&ctx, |cols| evaluate(
                mode,
                "DAY_SECOND",
                &ctx.value,
                Some(&datetime),
                cols
            )),
            Ok(Datum::Int(29_010_203))
        );
        let mut events = prefix(mode);
        events.extend(["modes", "zone"]);
        assert_eq!(*ctx.events.borrow(), events);
        assert!(ctx.warnings.borrow().is_empty());
    }
    let ctx = context(Datum::Null, Datum::new_bytes(vec![255]), [false, false]);
    assert_eq!(
        execution
            .scope()
            .with_columns(&ctx, |cols| evaluate(2, "unused", &ctx.value, None, cols)),
        Ok(Datum::Null)
    );
    assert_eq!(*ctx.events.borrow(), vec!["unit", "value"]);
    let mut ctx = context(Datum::Null, Datum::Null, [false, false]);
    ctx.fail_value = true;
    assert_eq!(
        execution
            .scope()
            .with_columns(&ctx, |cols| evaluate(2, "unused", &ctx.value, None, cols)),
        Err(EvalError::Unsupported("EXTRACT child"))
    );
    assert_eq!(*ctx.events.borrow(), vec!["unit", "value"]);
    let ctx = context(
        Datum::new_bytes(vec![255]),
        Datum::Time(Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()),
        [false, false],
    );
    assert_eq!(
        execution
            .scope()
            .with_columns(&ctx, |cols| evaluate(2, "unused", &ctx.value, None, cols)),
        Err(invalid_unit("�"))
    );
    assert_eq!(*ctx.events.borrow(), vec!["unit", "value"]);
    for (unit, input, expected, warned) in [
        ("DAY_SECOND", "1:02", Datum::Int(102), false),
        ("DAY_SECOND", "2020-01-02 bad", Datum::Int(2_000_000), false),
        ("unknown", "1:02", Datum::Int(0), false),
        ("DAY_SECOND", "bad", Datum::Null, true),
    ] {
        let ctx = context(text(unit), text(input), [false, false]);
        assert_eq!(
            execution
                .scope()
                .with_columns(&ctx, |cols| super::calendar::extract_composite(
                    unit,
                    &[ctx.value.clone()],
                    cols
                )),
            Ok(expected)
        );
        assert_eq!(
            *ctx.events.borrow(),
            if warned { vec!["warning"] } else { vec![] }
        );
        assert_eq!(
            *ctx.warnings.borrow(),
            if warned {
                vec![(
                    1292,
                    "Incorrect datetime value: '0000-00-00 00:00:00'".to_owned(),
                )]
            } else {
                vec![]
            }
        );
    }
    let denied_pool = owner(0);
    let denied = denied_pool.begin_execution().unwrap();
    for mode in 0..3 {
        let ctx = context(Datum::Null, Datum::Null, [false, false]);
        assert!(
            matches!(denied.scope().with_columns(&ctx, |cols| evaluate(mode, "DAY_SECOND", &ctx.value, None, cols)),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
        );
        assert_eq!(*ctx.events.borrow(), prefix(mode));
    }
    let ctx = context(Datum::Null, Datum::Null, [false, false]);
    assert!(
        matches!(denied.scope().with_columns(&ctx, |cols| super::calendar::extract_composite("DAY_SECOND", &[Datum::Null], cols)),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
    );
    assert_eq!(
        denied
            .scope()
            .with_columns(&ctx, |cols| super::calendar::extract_composite(
                "DAY_SECOND",
                &[],
                cols
            )),
        Err(EvalError::Unsupported("bad function arity"))
    );
    assert_eq!(
        denied
            .scope()
            .with_columns(&ctx, |cols| super::calendar::extract_composite(
                "DAY_SECOND",
                &[Datum::new_bytes(vec![255])],
                cols
            )),
        Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
    );
    assert!(ctx.events.borrow().is_empty());
}
