// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(feature = "tikv-expr")]

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{
    BinaryJSON, Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration, SessionTimeZone, Time,
    TimeType, VectorFloat32,
};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::evaluator::{EvaluatorProgram, EvaluatorSuite};
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::tikv::{Backend, Context, FallbackReason, TikvExpression};
use tidb_expr::Columns;

#[derive(Default)]
struct TestContext {
    backend: Option<Backend>,
    /// Simulates a resolver built after the native evaluator is deleted.
    engine_required: bool,
    rows: Cell<usize>,
    borrowed: Cell<usize>,
    warnings: RefCell<Vec<u16>>,
    fallbacks: RefCell<Vec<FallbackReason>>,
}
impl Columns for TestContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn tikv_expression_context(&self) -> Option<Context> {
        self.backend.map(|_| Context {
            flags: 482,
            ..Context::default()
        })
    }
    fn tikv_expression_required(&self) -> bool {
        self.engine_required
    }
    fn tikv_expression_backend(&self) -> Backend {
        self.backend.unwrap_or_default()
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.rows.set(self.rows.get() + rows);
    }
    fn record_tikv_borrowed_expression_rows(&self, rows: usize) {
        self.borrowed.set(self.borrowed.get() + rows);
    }
    fn record_tikv_expression_fallback(&self, reason: FallbackReason) {
        self.fallbacks.borrow_mut().push(reason);
    }
    fn append_warning(&self, code: u16, _: &str) {
        self.warnings.borrow_mut().push(code);
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::Fixed {
            name: "UTC".to_owned(),
            offset_secs: 0,
        }
    }
}

fn column(index: usize, ty: &FieldType) -> Expression {
    let mut column = Column::new(index as i64 + 1, ty.clone());
    column.index = index as i64;
    Expression::Column(column)
}
fn literal(value: Datum, ty: &FieldType) -> Expression {
    Expression::Constant(Constant::new(value, ty.clone()))
}
fn call(name: &str, ty: &FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
}
fn int() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
fn real() -> FieldType {
    FieldType::new(FieldTypeCode::Double)
}
fn decimal() -> FieldType {
    FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(20)
        .with_decimal(4)
}
fn bytes() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
        .with_charset_name("binary")
        .with_collation_name("binary")
}
fn text() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
        .with_charset_name("utf8mb4")
        .with_collation_name("utf8mb4_bin")
}
fn datetime(fsp: i64) -> FieldType {
    FieldType::new(FieldTypeCode::Datetime)
        .with_decimal(fsp)
        .with_flen(if fsp == 0 { 19 } else { 19 + fsp + 1 })
}
fn date() -> FieldType {
    FieldType::new(FieldTypeCode::Date)
        .with_decimal(0)
        .with_flen(10)
}
fn duration(fsp: i64) -> FieldType {
    FieldType::new(FieldTypeCode::Duration)
        .with_decimal(fsp)
        .with_flen(if fsp == 0 { 10 } else { 10 + fsp + 1 })
}
fn json() -> FieldType {
    FieldType::new(FieldTypeCode::Json)
}
fn time_value(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    micro: i32,
    fsp: i64,
) -> Datum {
    Datum::Time(
        Time::from_date_checked(
            year,
            month,
            day,
            hour,
            minute,
            second,
            micro,
            TimeType::DateTime,
            fsp,
        )
        .unwrap(),
    )
}
fn json_value(text: &str) -> Datum {
    Datum::Json(BinaryJSON::parse(text).unwrap())
}
fn dec(value: &str) -> Datum {
    let (value, error) = Decimal::parse_mysql(value);
    assert!(error.is_none());
    Datum::Decimal(value)
}
fn string(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

fn fixture(types: &[FieldType], values: &[Vec<Datum>]) -> Chunk {
    let mut input = Chunk::new_with_capacity(types, values.first().map_or(0, Vec::len));
    for (index, column) in values.iter().enumerate() {
        for value in column {
            input.append_datum(index, value);
        }
    }
    input
}

fn equal(left: &Datum, right: &Datum) -> bool {
    match (left, right) {
        (Datum::Real(left), Datum::Real(right)) => {
            left == right || (left - right).abs() <= 1e-12 * left.abs().max(right.abs()).max(1.0)
        }
        _ => left == right,
    }
}

/// A successful row is evidence for this exact fixture, not every shape of its
/// signature. Numeric math uses a 1e-12 relative/absolute comparison tolerance.
fn check(
    label: &str,
    expression: Expression,
    input: &mut Chunk,
    ty: &FieldType,
) -> Result<(), String> {
    let program = TikvExpression::compile(
        &expression,
        Context {
            flags: 482,
            ..Context::default()
        },
    )
    .map_err(|e| format!("{label}: compile error {e:?}"))?
    .ok_or_else(|| format!("{label}: declined {expression:?}"))?;
    let mut baseline = Vec::new();
    let mut baseline_warnings = Vec::new();
    for backend in [None, Some(Backend::Copying), Some(Backend::Borrowed)] {
        let context = TestContext {
            backend,
            ..TestContext::default()
        };
        let suite = EvaluatorSuite::new(vec![expression.clone()], true);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(ty), input.num_rows());
        suite
            .run(&context, input, &mut output)
            .map_err(|e| format!("{label} {backend:?}: {e:?}"))?;
        let expected_count = if backend.is_some() {
            input.num_rows()
        } else {
            0
        };
        if context.rows.get() != expected_count {
            return Err(format!(
                "{label} {backend:?}: native fallback, rows={}",
                context.rows.get()
            ));
        }
        if backend.is_some() && !context.fallbacks.borrow().is_empty() {
            return Err(format!(
                "{label} {backend:?}: engine context still fell back: {:?}",
                context.fallbacks.borrow()
            ));
        }
        if backend != Some(Backend::Borrowed) && context.borrowed.get() != 0 {
            return Err(format!("{label}: unexpected borrowed counter"));
        }
        let values = (0..output.num_rows())
            .map(|row| output.get_row(row).get_datum(0, ty))
            .collect::<Vec<_>>();
        if backend.is_none() {
            baseline = values;
            baseline_warnings = context.warnings.into_inner();
        } else {
            if values.len() != baseline.len()
                || !values
                    .iter()
                    .zip(&baseline)
                    .all(|(right, left)| equal(left, right))
            {
                return Err(format!(
                    "{label} {backend:?}: native={baseline:?}, engine={values:?}"
                ));
            }
            if *context.warnings.borrow() != baseline_warnings {
                return Err(format!(
                    "{label} {backend:?}: warnings native={baseline_warnings:?}, engine={:?}",
                    context.warnings.borrow()
                ));
            }
        }
    }
    eprintln!(
        "TIKV_COVERAGE_PASS\t{label}\t{}\t{}",
        program
            .wire_signatures()
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(","),
        input.num_rows()
    );
    Ok(())
}

fn record(result: Result<(), String>, failures: &mut Vec<String>) {
    if let Err(error) = result {
        failures.push(error);
    }
}

#[test]
fn tikv_coverage_numeric_families_differential() {
    let mut failures = Vec::new();
    for (tag, ty, left, right) in [
        (
            "int",
            int(),
            vec![Datum::Int(4), Datum::Int(9), Datum::Null],
            vec![Datum::Int(2), Datum::Int(3), Datum::Int(1)],
        ),
        (
            "real",
            real(),
            vec![Datum::Real(4.5), Datum::Real(9.25), Datum::Null],
            vec![Datum::Real(2.0), Datum::Real(3.0), Datum::Real(1.0)],
        ),
        (
            "decimal",
            decimal(),
            vec![dec("4.5000"), dec("9.2500"), Datum::Null],
            vec![dec("2.0000"), dec("3.0000"), dec("1.0000")],
        ),
        (
            "uint",
            int().with_flags(1 << 5),
            vec![Datum::UInt(1 << 63), Datum::UInt(9), Datum::Null],
            vec![Datum::UInt(2), Datum::UInt(3), Datum::UInt(1)],
        ),
    ] {
        let mut input = fixture(&[ty.clone(), ty.clone()], &[left, right]);
        input.set_sel(Some(vec![1, 0, 2, 0]));
        for name in [
            "plus", "minus", "mul", "mod", "eq", "ne", "lt", "le", "gt", "ge", "nulleq", "in",
            "greatest", "least",
        ] {
            if tag == "uint" && matches!(name, "mul" | "plus") {
                continue;
            }
            let result = if matches!(
                name,
                "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "nulleq" | "in"
            ) {
                int()
            } else {
                ty.clone()
            };
            record(
                check(
                    &format!("{name}_{tag}"),
                    call(name, &result, vec![column(0, &ty), column(1, &ty)]),
                    &mut input,
                    &result,
                ),
                &mut failures,
            );
        }
        for name in [
            "abs", "isnull", "istrue", "isfalse", "not", "round", "ceil", "floor",
        ] {
            let result = if matches!(name, "isnull" | "istrue" | "isfalse" | "not") {
                int()
            } else {
                ty.clone()
            };
            record(
                check(
                    &format!("{name}_{tag}"),
                    call(name, &result, vec![column(0, &ty)]),
                    &mut input,
                    &result,
                ),
                &mut failures,
            );
        }
        for name in ["round", "truncate"] {
            record(
                check(
                    &format!("{name}_frac_{tag}"),
                    call(
                        name,
                        &ty,
                        vec![column(0, &ty), literal(Datum::Int(1), &int())],
                    ),
                    &mut input,
                    &ty,
                ),
                &mut failures,
            );
        }
        if tag != "uint" {
            let div_type = if tag == "int" { decimal() } else { ty.clone() };
            record(
                check(
                    &format!("div_{tag}"),
                    call("div", &div_type, vec![column(0, &ty), column(1, &ty)]),
                    &mut input,
                    &div_type,
                ),
                &mut failures,
            );
            record(
                check(
                    &format!("intdiv_{tag}"),
                    call("intdiv", &int(), vec![column(0, &ty), column(1, &ty)]),
                    &mut input,
                    &int(),
                ),
                &mut failures,
            );
            record(
                check(
                    &format!("unaryminus_{tag}"),
                    call("unaryminus", &ty, vec![column(0, &ty)]),
                    &mut input,
                    &ty,
                ),
                &mut failures,
            );
        }
    }
    let ty = real();
    let mut input = fixture(
        std::slice::from_ref(&ty),
        &[vec![Datum::Real(0.25), Datum::Real(0.5), Datum::Null]],
    );
    for name in [
        // `cot` is excluded: this port is one ULP from Go while the engine matches Go.
        "sin", "cos", "tan", "asin", "acos", "atan", "sqrt", "exp", "degrees", "radians", "log2",
        "log10", "ln", "log",
    ] {
        record(
            check(name, call(name, &ty, vec![column(0, &ty)]), &mut input, &ty),
            &mut failures,
        );
    }
    for name in ["pow", "power", "log", "atan", "atan2"] {
        record(
            check(
                &format!("{name}_binary"),
                call(
                    name,
                    &ty,
                    vec![column(0, &ty), literal(Datum::Real(2.0), &ty)],
                ),
                &mut input,
                &ty,
            ),
            &mut failures,
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_string_and_misc_families_differential() {
    let mut failures = Vec::new();
    for (tag, ty, values) in [
        (
            "bytes",
            bytes(),
            vec![
                Datum::Bytes(b"Abc 123".to_vec()),
                Datum::Bytes(b" xyz ".to_vec()),
                Datum::Null,
            ],
        ),
        (
            "utf8",
            text(),
            vec![string("中文Abc"), string(" xyz "), Datum::Null],
        ),
    ] {
        let mut input = fixture(std::slice::from_ref(&ty), &[values]);
        input.set_sel(Some(vec![1, 0, 2, 0]));
        for name in [
            "length",
            "octet_length",
            "bit_length",
            "ascii",
            "ord",
            "char_length",
            "character_length",
        ] {
            record(
                check(
                    &format!("{name}_{tag}"),
                    call(name, &int(), vec![column(0, &ty)]),
                    &mut input,
                    &int(),
                ),
                &mut failures,
            );
        }
        for name in [
            "lower", "upper", "lcase", "ucase", "reverse", "ltrim", "rtrim", "quote", "hex", "md5",
            "sha1", "sha",
        ] {
            record(
                check(
                    &format!("{name}_{tag}"),
                    call(name, &ty, vec![column(0, &ty)]),
                    &mut input,
                    &ty,
                ),
                &mut failures,
            );
        }
        for name in ["left", "right", "substring", "substr", "mid"] {
            record(
                check(
                    &format!("{name}_{tag}"),
                    call(
                        name,
                        &ty,
                        vec![column(0, &ty), literal(Datum::Int(2), &int())],
                    ),
                    &mut input,
                    &ty,
                ),
                &mut failures,
            );
        }
        record(
            check(
                &format!("sha2_{tag}"),
                call(
                    "sha2",
                    &ty,
                    vec![column(0, &ty), literal(Datum::Int(256), &int())],
                ),
                &mut input,
                &ty,
            ),
            &mut failures,
        );
        record(
            check(
                &format!("substring3_{tag}"),
                call(
                    "substring",
                    &ty,
                    vec![
                        column(0, &ty),
                        literal(Datum::Int(2), &int()),
                        literal(Datum::Int(3), &int()),
                    ],
                ),
                &mut input,
                &ty,
            ),
            &mut failures,
        );
        // `find_in_set` is excluded: the engine compares bytes, so a non-binary
        // collation would fold case/accents differently.
        for name in ["strcmp", "instr", "locate"] {
            record(
                check(
                    &format!("{name}_{tag}"),
                    call(
                        name,
                        &int(),
                        vec![column(0, &ty), literal(string("x"), &ty)],
                    ),
                    &mut input,
                    &int(),
                ),
                &mut failures,
            );
        }
        // `crc32` is excluded: the engine returns Datum::Int where native
        // returns Datum::UInt, a result-kind difference.
    }
    let ty = int();
    let unsigned = ty.clone().with_flags(1 << 5);
    let mut input = fixture(
        std::slice::from_ref(&ty),
        &[vec![Datum::Int(7), Datum::Int(-1), Datum::Null]],
    );
    for name in ["bitand", "bitor", "bitxor", "leftshift", "rightshift"] {
        record(
            check(
                name,
                call(
                    name,
                    &unsigned,
                    vec![column(0, &ty), literal(Datum::Int(2), &ty)],
                ),
                &mut input,
                &unsigned,
            ),
            &mut failures,
        );
    }
    record(
        check(
            "bitneg",
            call("bitneg", &unsigned, vec![column(0, &ty)]),
            &mut input,
            &unsigned,
        ),
        &mut failures,
    );
    record(
        check(
            "bit_count",
            call("bit_count", &ty, vec![column(0, &ty)]),
            &mut input,
            &ty,
        ),
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn temporal_constant_shapes_execute_without_fallback() {
    struct TemporalContext {
        config: Context,
        backend: Backend,
        rows: Cell<usize>,
    }
    impl Columns for TemporalContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn tikv_expression_context(&self) -> Option<Context> {
            Some(self.config.clone())
        }
        fn tikv_expression_required(&self) -> bool {
            true
        }
        fn tikv_expression_backend(&self) -> Backend {
            self.backend
        }
        fn record_tikv_expression_rows(&self, rows: usize) {
            self.rows.set(self.rows.get() + rows);
        }
        fn append_warning(&self, code: u16, message: &str) {
            panic!("unexpected temporal warning {code}: {message}");
        }
    }
    let make = |y, m, d, h, micro, kind, fsp| {
        Datum::Time(Time::from_date_checked(y, m, d, h, 0, 0, micro, kind, fsp).unwrap())
    };
    let cases = vec![
        (date(), make(2024, 3, 14, 0, 0, TimeType::Date, 0)),
        (date(), make(2024, 3, 14, 12, 0, TimeType::Date, 0)),
        (datetime(0), make(2024, 3, 14, 12, 0, TimeType::DateTime, 0)),
        (
            datetime(3),
            make(2024, 3, 14, 12, 123456, TimeType::DateTime, 3),
        ),
        (
            datetime(6),
            make(2024, 3, 14, 12, 123456, TimeType::DateTime, 6),
        ),
        (
            datetime(0).with_decimal(-1),
            make(2024, 3, 14, 12, 0, TimeType::DateTime, 0),
        ),
        (datetime(0), make(2024, 0, 1, 0, 0, TimeType::DateTime, 0)),
        (datetime(0), make(2024, 2, 31, 0, 0, TimeType::DateTime, 0)),
        (date(), Datum::Null),
        (datetime(6), Datum::Null),
    ];
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    for offset in [-43200, 0, 28800] {
        for sql_mode in [0, u64::MAX] {
            for backend in [Backend::Copying, Backend::Borrowed] {
                let ctx = TemporalContext {
                    config: Context {
                        time_zone_offset: offset,
                        sql_mode,
                        ..Context::default()
                    },
                    backend,
                    rows: Cell::new(0),
                };
                for (ty, value) in &cases {
                    let suite = EvaluatorSuite::new(vec![literal(value.clone(), ty)], true);
                    assert_eq!(
                        suite.eval_selected_for_cast(&ctx, &input, &[0]).unwrap(),
                        vec![value.clone()],
                        "{ty:?} {value:?} offset={offset} sql_mode={sql_mode}"
                    );
                }
                let year = call(
                    "year",
                    &int(),
                    vec![literal(cases[4].1.clone(), &cases[4].0)],
                );
                assert_eq!(
                    EvaluatorSuite::new(vec![year], true)
                        .eval_selected_for_cast(&ctx, &input, &[0])
                        .unwrap(),
                    vec![Datum::Int(2024)]
                );
                assert_eq!(ctx.rows.get(), cases.len() + 1);
            }
        }
    }
}

#[test]
fn temporal_constant_unsafe_shapes_remain_declined() {
    let dt = Time::from_date_checked(2024, 3, 14, 12, 0, 0, 123456, TimeType::DateTime, 6).unwrap();
    let timestamp =
        Time::from_date_checked(2024, 3, 14, 12, 0, 0, 0, TimeType::Timestamp, 0).unwrap();
    let malformed =
        Time::from_date_checked(2024, 13, 14, 0, 0, 0, 0, TimeType::DateTime, 0).unwrap();
    let zero = Time::from_date_checked(0, 0, 0, 0, 0, 0, 0, TimeType::DateTime, 0).unwrap();
    for (value, ty) in [
        (Datum::Time(dt), datetime(0)),
        (Datum::Time(dt), date()),
        (Datum::Time(timestamp), datetime(0)),
        (
            Datum::Time(timestamp),
            FieldType::new(FieldTypeCode::Timestamp).with_decimal(0),
        ),
        (Datum::Time(malformed), datetime(0)),
        (Datum::Time(zero), datetime(0)),
        (Datum::Int(20240314), date()),
    ] {
        assert!(
            TikvExpression::compile(&literal(value, &ty), Context::default())
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn tikv_coverage_temporal_json_vector_families_differential() {
    let mut failures = Vec::new();
    let time = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
    let value =
        Time::from_date_checked(2024, 3, 14, 12, 34, 56, 123456, TimeType::DateTime, 6).unwrap();
    let mut input = fixture(
        std::slice::from_ref(&time),
        &[vec![Datum::Time(value), Datum::Null]],
    );
    for name in [
        "year",
        "month",
        "day",
        "dayofmonth",
        "dayofweek",
        "dayofyear",
        "weekday",
        "weekofyear",
        "yearweek",
        "quarter",
        "to_days",
        "to_seconds",
    ] {
        record(
            check(
                name,
                call(name, &int(), vec![column(0, &time)]),
                &mut input,
                &int(),
            ),
            &mut failures,
        );
    }
    for name in ["monthname", "dayname"] {
        record(
            check(
                name,
                call(name, &text(), vec![column(0, &time)]),
                &mut input,
                &text(),
            ),
            &mut failures,
        );
    }
    record(
        check(
            "week_mode",
            call(
                "week",
                &int(),
                vec![column(0, &time), literal(Datum::Int(3), &int())],
            ),
            &mut input,
            &int(),
        ),
        &mut failures,
    );
    let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let mut input = fixture(
        std::slice::from_ref(&duration),
        &[vec![
            Datum::Duration(MySqlDuration::from_raw_parts(45_296_123_456_000, 6)),
            Datum::Null,
        ]],
    );
    for name in ["hour", "minute", "second", "microsecond", "time_to_sec"] {
        record(
            check(
                name,
                call(name, &int(), vec![column(0, &duration)]),
                &mut input,
                &int(),
            ),
            &mut failures,
        );
    }
    let json = FieldType::new(FieldTypeCode::Json);
    let value = BinaryJSON::parse(r#"{"a":[1,2],"b":true}"#).unwrap();
    let mut input = fixture(
        std::slice::from_ref(&json),
        &[vec![Datum::Json(value), Datum::Null]],
    );
    for name in ["json_depth", "json_length", "json_valid"] {
        record(
            check(
                name,
                call(name, &int(), vec![column(0, &json)]),
                &mut input,
                &int(),
            ),
            &mut failures,
        );
    }
    record(
        check(
            "json_type",
            call("json_type", &text(), vec![column(0, &json)]),
            &mut input,
            &text(),
        ),
        &mut failures,
    );
    record(
        check(
            "json_extract",
            call(
                "json_extract",
                &json,
                vec![column(0, &json), literal(string("$.a"), &text())],
            ),
            &mut input,
            &json,
        ),
        &mut failures,
    );
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let mut input = fixture(
        &[vector.clone(), vector.clone()],
        &[
            vec![
                Datum::VectorFloat32(VectorFloat32::create(vec![1.0_f32, 2.0]).unwrap()),
                Datum::Null,
            ],
            vec![
                Datum::VectorFloat32(VectorFloat32::create(vec![2.0_f32, 3.0]).unwrap()),
                Datum::VectorFloat32(VectorFloat32::create(vec![1.0_f32, 1.0]).unwrap()),
            ],
        ],
    );
    record(
        check(
            "vec_dims",
            call("vec_dims", &int(), vec![column(0, &vector)]),
            &mut input,
            &int(),
        ),
        &mut failures,
    );
    record(
        check(
            "vec_l2_norm",
            call("vec_l2_norm", &real(), vec![column(0, &vector)]),
            &mut input,
            &real(),
        ),
        &mut failures,
    );
    record(
        check(
            "vec_as_text",
            call("vec_as_text", &text(), vec![column(0, &vector)]),
            &mut input,
            &text(),
        ),
        &mut failures,
    );
    for name in [
        "vec_l1_distance",
        "vec_l2_distance",
        "vec_negative_inner_product",
        "vec_cosine_distance",
    ] {
        record(
            check(
                name,
                call(name, &real(), vec![column(0, &vector), column(1, &vector)]),
                &mut input,
                &real(),
            ),
            &mut failures,
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_control_leaves_and_unsafe_branches() {
    let ty = int();
    let mut input = fixture(
        &[ty.clone(), ty.clone()],
        &[
            vec![Datum::Int(0), Datum::Int(1), Datum::Null],
            vec![Datum::Int(7), Datum::Null, Datum::Int(9)],
        ],
    );
    for name in ["ifnull", "coalesce", "and", "or", "xor"] {
        check(
            name,
            call(name, &ty, vec![column(0, &ty), column(1, &ty)]),
            &mut input,
            &ty,
        )
        .unwrap();
    }
    // A NULL constant carries the SQL `Null` type, whose eval family is
    // String, while the kernel is `CoalesceInt`. The adapter retags the leaf
    // to the arm's family instead of inserting a cast, so the engine's
    // argument validator sees `Int` and the arm stays a leaf.
    let null_ty = FieldType::new(FieldTypeCode::Null);
    check(
        "coalesce_null_arm",
        call(
            "coalesce",
            &ty,
            vec![literal(Datum::Null, &null_ty), column(0, &ty)],
        ),
        &mut input,
        &ty,
    )
    .unwrap();
    check(
        "ifnull_null_arm",
        call(
            "ifnull",
            &ty,
            vec![column(0, &ty), literal(Datum::Null, &null_ty)],
        ),
        &mut input,
        &ty,
    )
    .unwrap();
    check(
        "if_leaf",
        call(
            "if",
            &ty,
            vec![column(0, &ty), column(1, &ty), literal(Datum::Int(3), &ty)],
        ),
        &mut input,
        &ty,
    )
    .unwrap();
    let overflow = call(
        "plus",
        &ty,
        vec![
            literal(Datum::Int(i64::MAX), &ty),
            literal(Datum::Int(1), &ty),
        ],
    );
    // Every `If*` signature has a lazy kernel, so a non-leaf dead branch is
    // admitted and the branch is never entered. The same expression is also
    // covered by `tikv_lazy.rs`; here the point is that admission no longer
    // refuses the shape and the engine still does not raise 1690.
    let expression = call(
        "if",
        &ty,
        vec![
            literal(Datum::Int(0), &ty),
            overflow,
            literal(Datum::Int(7), &ty),
        ],
    );
    check("if_dead_overflow", expression.clone(), &mut input, &ty).unwrap();
    let context = TestContext {
        backend: Some(Backend::Borrowed),
        ..TestContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
    suite.run(&context, &mut input, &mut output).unwrap();
    assert!(context.rows.get() > 0, "the engine must run this shape");
    assert!(
        context.fallbacks.borrow().is_empty(),
        "{:?}",
        context.fallbacks.borrow()
    );
    for row in 0..3 {
        assert_eq!(output.get_row(row).get_int64(0), 7);
    }
}

/// After the native evaluator is deleted, a resolver with no engine context
/// must fail with a structured error rather than silently picking the other
/// implementation. While both implementations coexist the same resolver keeps
/// working, so this documents the switch that milestone E flips.
#[test]
fn tikv_coverage_missing_engine_context_is_a_structured_error_when_required() {
    use tidb_expr::evaluator::EvaluatorError;
    use tidb_expr::EvalError;

    let ty = int();
    let expression = call(
        "plus",
        &ty,
        vec![column(0, &ty), literal(Datum::Int(1), &ty)],
    );
    let mut input = fixture(std::slice::from_ref(&ty), &[vec![Datum::Int(41)]]);

    let context = TestContext {
        engine_required: true,
        ..TestContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression.clone()], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    match suite.run(&context, &mut input, &mut output) {
        Err(EvaluatorError::Eval(EvalError::ExternalEngine { code, .. })) => {
            assert_eq!(code, 1105)
        }
        other => panic!("expected a structured missing-engine error, got {other:?}"),
    }

    // The default resolver still has the native evaluator available.
    let context = TestContext::default();
    let suite = EvaluatorSuite::new(vec![expression], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    suite.run(&context, &mut input, &mut output).unwrap();
    assert_eq!(output.get_row(0).get_int64(0), 42);
    assert_eq!(context.rows.get(), 0);
}

/// The same resolver must also refuse to *silently* fall back when the adapter
/// declines the expression: with no native evaluator there is nothing to fall
/// back to, and the fallback reason is what the error has to carry.
#[test]
fn tikv_coverage_declined_expression_is_a_structured_error_when_required() {
    use tidb_expr::evaluator::EvaluatorError;
    use tidb_expr::EvalError;

    let ty = text();
    // `translate` is a permanent native exception: the pinned tipb has no
    // signature for it, so no lowering can ever reach the engine.
    let expression = call(
        "translate",
        &ty,
        vec![
            literal(string("abc"), &ty),
            literal(string("a"), &ty),
            literal(string("b"), &ty),
        ],
    );
    let mut input = fixture(std::slice::from_ref(&ty), &[vec![string("abc")]]);

    let context = TestContext {
        engine_required: true,
        backend: Some(Backend::Copying),
        ..TestContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression.clone()], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    match suite.run(&context, &mut input, &mut output) {
        Err(EvaluatorError::Eval(EvalError::ExternalEngine { code, message })) => {
            assert_eq!(code, 1105);
            assert!(
                message.contains("declined"),
                "the error must name the refusal: {message}"
            );
        }
        other => panic!("expected a structured refusal error, got {other:?}"),
    }
    assert_eq!(
        context.fallbacks.borrow().len(),
        1,
        "the refusal reason must still be recorded"
    );

    // A resolver that still has the native evaluator keeps answering.
    let context = TestContext {
        backend: Some(Backend::Copying),
        ..TestContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    suite.run(&context, &mut input, &mut output).unwrap();
    assert_eq!(context.rows.get(), 0);
}

#[test]
fn tikv_coverage_control_selector_family_differential() {
    let mut failures = Vec::new();
    let ty = int();
    let text_ty = text();
    // case/casewhen: (condition, result)* with an optional trailing else.
    let mut input = fixture(
        &[ty.clone(), ty.clone(), ty.clone(), ty.clone()],
        &[
            vec![Datum::Int(0), Datum::Int(1), Datum::Null, Datum::Int(2)],
            vec![
                Datum::Int(10),
                Datum::Int(20),
                Datum::Int(30),
                Datum::Int(40),
            ],
            vec![Datum::Int(0), Datum::Int(0), Datum::Int(1), Datum::Null],
            vec![
                Datum::Int(100),
                Datum::Int(200),
                Datum::Int(300),
                Datum::Int(400),
            ],
        ],
    );
    for name in ["case"] {
        record(
            check(
                &format!("{name}_pairs"),
                call(
                    name,
                    &ty,
                    vec![
                        column(0, &ty),
                        column(1, &ty),
                        column(2, &ty),
                        column(3, &ty),
                    ],
                ),
                &mut input,
                &ty,
            ),
            &mut failures,
        );
    }
    let mut input = fixture(
        &[ty.clone(), ty.clone(), ty.clone()],
        &[
            vec![Datum::Int(1), Datum::Int(0), Datum::Null],
            vec![Datum::Int(7), Datum::Int(7), Datum::Int(7)],
            vec![Datum::Int(9), Datum::Int(9), Datum::Int(9)],
        ],
    );
    for name in ["case"] {
        record(
            check(
                &format!("{name}_else"),
                call(
                    name,
                    &ty,
                    vec![column(0, &ty), column(1, &ty), column(2, &ty)],
                ),
                &mut input,
                &ty,
            ),
            &mut failures,
        );
    }
    // elt: the first argument is an integer selector, the rest are strings.
    let mut input = fixture(
        &[ty.clone(), text_ty.clone(), text_ty.clone()],
        &[
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(0), Datum::Null],
            vec![string("a"), string("b"), string("c"), string("d")],
            vec![string("x"), string("y"), string("z"), string("w")],
        ],
    );
    record(
        check(
            "elt",
            call(
                "elt",
                &text_ty,
                vec![column(0, &ty), column(1, &text_ty), column(2, &text_ty)],
            ),
            &mut input,
            &text_ty,
        ),
        &mut failures,
    );
    // field: first argument is the needle, the rest are the haystack.
    let mut input = fixture(
        &[ty.clone(), ty.clone(), ty.clone()],
        &[
            vec![Datum::Int(5), Datum::Int(1), Datum::Null, Datum::Int(3)],
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(1), Datum::Int(3)],
            vec![Datum::Int(10), Datum::Int(2), Datum::Int(2), Datum::Int(3)],
        ],
    );
    record(
        check(
            "field",
            call(
                "field",
                &ty,
                vec![column(0, &ty), column(1, &ty), column(2, &ty)],
            ),
            &mut input,
            &ty,
        ),
        &mut failures,
    );
    // interval: first argument is the probe, the rest are ascending thresholds.
    let mut input = fixture(
        &[ty.clone(), ty.clone(), ty.clone(), ty.clone()],
        &[
            vec![Datum::Int(0), Datum::Int(5), Datum::Int(15), Datum::Null],
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1)],
            vec![
                Datum::Int(10),
                Datum::Int(10),
                Datum::Int(10),
                Datum::Int(10),
            ],
            vec![
                Datum::Int(100),
                Datum::Int(100),
                Datum::Int(100),
                Datum::Int(100),
            ],
        ],
    );
    record(
        check(
            "interval",
            call(
                "interval",
                &ty,
                vec![
                    column(0, &ty),
                    column(1, &ty),
                    column(2, &ty),
                    column(3, &ty),
                ],
            ),
            &mut input,
            &ty,
        ),
        &mut failures,
    );
    // IS TRUE / IS FALSE keep NULL, unlike their non-underscore spellings.
    let mut input = fixture(
        std::slice::from_ref(&ty),
        &[vec![
            Datum::Int(0),
            Datum::Int(1),
            Datum::Int(2),
            Datum::Null,
        ]],
    );
    for name in ["istrue_with_null"] {
        record(
            check(name, call(name, &ty, vec![column(0, &ty)]), &mut input, &ty),
            &mut failures,
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_string_misc_extended_differential() {
    let mut failures = Vec::new();
    let ints = int();
    let reals = real();
    let strings = text();
    let binary = bytes();
    let unsigned = ints.clone().with_flags(1 << 5);

    let mut string_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![string("Abc 123"), string(" xyz "), Datum::Null]],
    );
    let mut int_input = fixture(
        std::slice::from_ref(&ints),
        &[vec![Datum::Int(7), Datum::Int(-1), Datum::Null]],
    );
    let mut real_input = fixture(
        std::slice::from_ref(&reals),
        &[vec![Datum::Real(2.5), Datum::Real(-1.25), Datum::Null]],
    );

    // bin/oct render an integer's raw bits.
    // `oct` is excluded: over a binary literal it reads the bit value natively
    // but takes the string path in the engine.
    for name in ["bin"] {
        record(
            check(
                name,
                call(name, &strings, vec![column(0, &ints)]),
                &mut int_input,
                &strings,
            ),
            &mut failures,
        );
    }
    // conv reads (value, from_base, to_base).
    let mut conv_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![string("FF"), string("10"), Datum::Null]],
    );
    record(
        check(
            "conv",
            call(
                "conv",
                &strings,
                vec![
                    column(0, &strings),
                    literal(Datum::Int(16), &ints),
                    literal(Datum::Int(10), &ints),
                ],
            ),
            &mut conv_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "replace",
            call(
                "replace",
                &strings,
                vec![
                    column(0, &strings),
                    literal(string("b"), &strings),
                    literal(string("B"), &strings),
                ],
            ),
            &mut string_input,
            &strings,
        ),
        &mut failures,
    );
    // The rewriter emits the two-argument TRIM form (direction is BOTH).
    record(
        check(
            "trim",
            call(
                "trim",
                &strings,
                vec![column(0, &strings), literal(string(" "), &strings)],
            ),
            &mut string_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "substring_index",
            call(
                "substring_index",
                &strings,
                vec![
                    column(0, &strings),
                    literal(string(" "), &strings),
                    literal(Datum::Int(2), &ints),
                ],
            ),
            &mut string_input,
            &strings,
        ),
        &mut failures,
    );
    // unhex returns the decoded bytes.
    let mut hex_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![string("4D7953514C"), string("126"), Datum::Null]],
    );
    record(
        check(
            "unhex",
            call("unhex", &binary, vec![column(0, &strings)]),
            &mut hex_input,
            &binary,
        ),
        &mut failures,
    );
    // like(value, pattern, escape) -- the three-argument rewriter shape.
    record(
        check(
            "like",
            call(
                "like",
                &ints,
                vec![
                    column(0, &strings),
                    literal(string("Abc%"), &strings),
                    literal(Datum::Int(i64::from(b'\\')), &ints),
                ],
            ),
            &mut string_input,
            &ints,
        ),
        &mut failures,
    );
    for name in ["regexp", "regexp_like"] {
        record(
            check(
                name,
                call(
                    name,
                    &ints,
                    vec![column(0, &strings), literal(string("^A"), &strings)],
                ),
                &mut string_input,
                &ints,
            ),
            &mut failures,
        );
    }
    record(
        check(
            "regexp_substr",
            call(
                "regexp_substr",
                &strings,
                vec![column(0, &strings), literal(string("A."), &strings)],
            ),
            &mut string_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "regexp_instr",
            call(
                "regexp_instr",
                &ints,
                vec![column(0, &strings), literal(string("A"), &strings)],
            ),
            &mut string_input,
            &ints,
        ),
        &mut failures,
    );
    record(
        check(
            "regexp_replace",
            call(
                "regexp_replace",
                &strings,
                vec![
                    column(0, &strings),
                    literal(string("A"), &strings),
                    literal(string("X"), &strings),
                ],
            ),
            &mut string_input,
            &strings,
        ),
        &mut failures,
    );
    // inet_aton returns UNSIGNED; inet_ntoa takes the integer.
    let mut ip_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![
            string("1.2.3.4"),
            string("255.255.255.255"),
            Datum::Null,
        ]],
    );
    record(
        check(
            "inet_aton",
            call("inet_aton", &unsigned, vec![column(0, &strings)]),
            &mut ip_input,
            &unsigned,
        ),
        &mut failures,
    );
    record(
        check(
            "inet_ntoa",
            call("inet_ntoa", &strings, vec![column(0, &ints)]),
            &mut int_input,
            &strings,
        ),
        &mut failures,
    );
    let mut ip6_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![string("1.2.3.4"), string("2001:db8::1"), Datum::Null]],
    );
    record(
        check(
            "inet6_aton",
            call("inet6_aton", &binary, vec![column(0, &strings)]),
            &mut ip6_input,
            &binary,
        ),
        &mut failures,
    );
    let mut raw_ip_input = fixture(
        std::slice::from_ref(&binary),
        &[vec![
            Datum::Bytes(vec![1, 2, 3, 4]),
            Datum::Bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            Datum::Null,
        ]],
    );
    record(
        check(
            "inet6_ntoa",
            call("inet6_ntoa", &strings, vec![column(0, &binary)]),
            &mut raw_ip_input,
            &strings,
        ),
        &mut failures,
    );
    // is_ipv4/is_ipv6/is_ipv4_compat/is_ipv4_mapped are deliberately not
    // fixtured here: the engine returns 0 for a NULL argument where the native
    // evaluator (and Go) returns NULL. See the coverage report.
    record(
        check(
            "to_binary",
            call("to_binary", &binary, vec![column(0, &strings)]),
            &mut string_input,
            &binary,
        ),
        &mut failures,
    );
    let mut utf8_bytes_input = fixture(
        std::slice::from_ref(&binary),
        &[vec![
            Datum::Bytes(b"hello".to_vec()),
            Datum::Bytes("\u{4e2d}\u{6587}".as_bytes().to_vec()),
            Datum::Null,
        ]],
    );
    record(
        check(
            "from_binary",
            call("from_binary", &strings, vec![column(0, &binary)]),
            &mut utf8_bytes_input,
            &strings,
        ),
        &mut failures,
    );
    // compress/uncompress use the four-byte length frame plus a zlib stream.
    record(
        check(
            "compress",
            call("compress", &binary, vec![column(0, &strings)]),
            &mut string_input,
            &binary,
        ),
        &mut failures,
    );
    let framed = Datum::Bytes(vec![
        11, 0, 0, 0, 120, 156, 203, 72, 205, 201, 201, 87, 40, 207, 47, 202, 73, 1, 0, 26, 11, 4,
        93,
    ]);
    let mut compressed_input = fixture(std::slice::from_ref(&binary), &[vec![framed, Datum::Null]]);
    record(
        check(
            "uncompress",
            call("uncompress", &binary, vec![column(0, &binary)]),
            &mut compressed_input,
            &binary,
        ),
        &mut failures,
    );
    record(
        check(
            "uncompressed_length",
            call("uncompressed_length", &ints, vec![column(0, &binary)]),
            &mut compressed_input,
            &ints,
        ),
        &mut failures,
    );
    record(
        check(
            "any_value",
            call("any_value", &ints, vec![column(0, &ints)]),
            &mut int_input,
            &ints,
        ),
        &mut failures,
    );
    record(
        check(
            "sign",
            call("sign", &ints, vec![column(0, &ints)]),
            &mut int_input,
            &ints,
        ),
        &mut failures,
    );
    record(
        check("pi", call("pi", &reals, vec![]), &mut real_input, &reals),
        &mut failures,
    );
    record(
        check(
            "ceiling",
            call("ceiling", &reals, vec![column(0, &reals)]),
            &mut real_input,
            &reals,
        ),
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_temporal_extended_differential() {
    let mut failures = Vec::new();
    let ints = int();
    let reals = real();
    let strings = text();
    let dt0 = datetime(0);
    let dt6 = datetime(6);
    let dur0 = duration(0);
    let dur6 = duration(6);
    let date_ty = date();

    let mut dt6_input = fixture(
        std::slice::from_ref(&dt6),
        &[vec![
            time_value(2024, 3, 14, 12, 34, 56, 123456, 6),
            time_value(2023, 2, 28, 0, 0, 0, 0, 6),
            Datum::Null,
        ]],
    );
    let mut date_input = fixture(
        std::slice::from_ref(&date_ty),
        &[vec![
            Datum::Time(
                Time::from_date_checked(2024, 3, 14, 0, 0, 0, 0, TimeType::Date, 0).unwrap(),
            ),
            Datum::Time(
                Time::from_date_checked(2023, 2, 28, 0, 0, 0, 0, TimeType::Date, 0).unwrap(),
            ),
            Datum::Null,
        ]],
    );
    record(
        check(
            "date",
            call("date", &date_ty, vec![column(0, &dt6)]),
            &mut dt6_input,
            &date_ty,
        ),
        &mut failures,
    );
    // `last_day` returns a DATE, but the engine kernel is typed `DateTime`
    // internally and keeps a midnight time part. The bridge rebuilds the
    // declared DATE, the way Go's DATE decoder drops the time part, so this
    // runs in the engine instead of falling back to native.
    record(
        check(
            "last_day",
            call("last_day", &date_ty, vec![column(0, &dt6)]),
            &mut dt6_input,
            &date_ty,
        ),
        &mut failures,
    );
    // The temporal spellings of an explicit CAST land on `CastTimeAsTime` and
    // friends; the declared fractional precision is carried onto the value by
    // the bridge, so both a DATE and a DATETIME target have to run in the
    // engine over a DATETIME column.
    record(
        check(
            "cast_date_datetime",
            call("cast_date", &date_ty, vec![column(0, &dt6)]),
            &mut dt6_input,
            &date_ty,
        ),
        &mut failures,
    );
    record(
        check(
            "cast_datetime_datetime",
            call("cast_datetime", &dt6, vec![column(0, &dt6)]),
            &mut dt6_input,
            &dt6,
        ),
        &mut failures,
    );
    record(
        check(
            "cast_time_datetime",
            call("cast_time", &dur6, vec![column(0, &dt6)]),
            &mut dt6_input,
            &dur6,
        ),
        &mut failures,
    );
    // `EXTRACT(unit FROM x)` and `TIMESTAMP(x)` are lowered as the unit
    // function and the temporal cast respectively, so both must run in the
    // engine over a column rather than only over a constant.
    record(
        check(
            "extract_year",
            call(
                "extract",
                &int(),
                vec![literal(string("YEAR"), &strings), column(0, &dt6)],
            ),
            &mut dt6_input,
            &int(),
        ),
        &mut failures,
    );
    record(
        check(
            "timestamp_of_datetime",
            call("timestamp", &dt6, vec![column(0, &dt6)]),
            &mut dt6_input,
            &dt6,
        ),
        &mut failures,
    );
    record(
        check(
            "date_format",
            call(
                "date_format",
                &strings,
                vec![
                    column(0, &dt6),
                    literal(string("%Y-%m-%d %H:%i:%s"), &strings),
                ],
            ),
            &mut dt6_input,
            &strings,
        ),
        &mut failures,
    );
    let mut dt_pair = fixture(
        &[dt6.clone(), dt6.clone()],
        &[
            vec![
                time_value(2024, 3, 14, 12, 34, 56, 123456, 6),
                time_value(2024, 3, 1, 0, 0, 0, 0, 6),
                Datum::Null,
            ],
            vec![
                time_value(2024, 3, 1, 0, 0, 0, 0, 6),
                time_value(2024, 3, 14, 12, 34, 56, 123456, 6),
                time_value(2024, 1, 1, 0, 0, 0, 0, 6),
            ],
        ],
    );
    record(
        check(
            "datediff",
            call("datediff", &ints, vec![column(0, &dt6), column(1, &dt6)]),
            &mut dt_pair,
            &ints,
        ),
        &mut failures,
    );
    record(
        check(
            "timestampdiff",
            call(
                "timestampdiff",
                &ints,
                vec![
                    literal(string("DAY"), &strings),
                    column(0, &dt6),
                    column(1, &dt6),
                ],
            ),
            &mut dt_pair,
            &ints,
        ),
        &mut failures,
    );
    let mut str_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![
            string("2024-03-14 12:34:56"),
            string("1999-01-02"),
            Datum::Null,
        ]],
    );
    record(
        check(
            "str_to_date",
            call(
                "str_to_date",
                &dt0,
                vec![
                    column(0, &strings),
                    literal(string("%Y-%m-%d %H:%i:%s"), &strings),
                ],
            ),
            &mut str_input,
            &dt0,
        ),
        &mut failures,
    );
    // from_days is deliberately not fixtured here: the engine answers
    // FROM_DAYS(1) with the zero date where the native evaluator (and Go)
    // returns NULL. See the coverage report.
    let mut make_date_input = fixture(
        &[ints.clone(), ints.clone()],
        &[
            vec![Datum::Int(2024), Datum::Int(2023), Datum::Null],
            vec![Datum::Int(60), Datum::Int(1), Datum::Int(1)],
        ],
    );
    record(
        check(
            "makedate",
            call(
                "makedate",
                &date_ty,
                vec![column(0, &ints), column(1, &ints)],
            ),
            &mut make_date_input,
            &date_ty,
        ),
        &mut failures,
    );
    let mut make_time_input = fixture(
        &[ints.clone(), ints.clone(), reals.clone()],
        &[
            vec![Datum::Int(12), Datum::Int(0), Datum::Null],
            vec![Datum::Int(34), Datum::Int(59), Datum::Int(0)],
            vec![Datum::Real(56.5), Datum::Real(0.25), Datum::Real(1.0)],
        ],
    );
    record(
        check(
            "maketime",
            call(
                "maketime",
                &dur6,
                vec![column(0, &ints), column(1, &ints), column(2, &reals)],
            ),
            &mut make_time_input,
            &dur6,
        ),
        &mut failures,
    );
    let mut period_add_input = fixture(
        &[ints.clone(), ints.clone()],
        &[
            vec![Datum::Int(202401), Datum::Int(202312), Datum::Null],
            vec![Datum::Int(2), Datum::Int(3), Datum::Int(1)],
        ],
    );
    record(
        check(
            "period_add",
            call(
                "period_add",
                &ints,
                vec![column(0, &ints), column(1, &ints)],
            ),
            &mut period_add_input,
            &ints,
        ),
        &mut failures,
    );
    let mut period_diff_input = fixture(
        &[ints.clone(), ints.clone()],
        &[
            vec![Datum::Int(202402), Datum::Int(202301), Datum::Null],
            vec![Datum::Int(202401), Datum::Int(202312), Datum::Int(202401)],
        ],
    );
    record(
        check(
            "period_diff",
            call(
                "period_diff",
                &ints,
                vec![column(0, &ints), column(1, &ints)],
            ),
            &mut period_diff_input,
            &ints,
        ),
        &mut failures,
    );
    let mut dt0_input = fixture(
        std::slice::from_ref(&dt0),
        &[vec![
            time_value(2024, 3, 14, 12, 34, 56, 0, 0),
            time_value(1970, 1, 1, 0, 0, 0, 0, 0),
            Datum::Null,
        ]],
    );
    record(
        check(
            "unix_timestamp",
            call("unix_timestamp", &ints, vec![column(0, &dt0)]),
            &mut dt0_input,
            &ints,
        ),
        &mut failures,
    );
    let mut epoch_input = fixture(
        std::slice::from_ref(&ints),
        &[vec![Datum::Int(1_710_419_696), Datum::Int(0), Datum::Null]],
    );
    record(
        check(
            "from_unixtime",
            call("from_unixtime", &dt6, vec![column(0, &ints)]),
            &mut epoch_input,
            &dt6,
        ),
        &mut failures,
    );
    let mut add_input = fixture(
        &[dt6.clone(), dur0.clone()],
        &[
            vec![
                time_value(2024, 3, 14, 12, 0, 0, 0, 6),
                time_value(2024, 1, 1, 0, 0, 0, 0, 6),
                Datum::Null,
            ],
            vec![
                Datum::Duration(MySqlDuration::from_raw_parts(3_600_000_000_000, 0)),
                Datum::Duration(MySqlDuration::from_raw_parts(90_000_000_000, 0)),
                Datum::Duration(MySqlDuration::from_raw_parts(0, 0)),
            ],
        ],
    );
    record(
        check(
            "addtime",
            call("addtime", &dt6, vec![column(0, &dt6), column(1, &dur0)]),
            &mut add_input,
            &dt6,
        ),
        &mut failures,
    );
    record(
        check(
            "subtime",
            call("subtime", &dt6, vec![column(0, &dt6), column(1, &dur0)]),
            &mut add_input,
            &dt6,
        ),
        &mut failures,
    );
    let mut diff_input = fixture(
        &[dur0.clone(), dur0.clone()],
        &[
            vec![
                Datum::Duration(MySqlDuration::from_raw_parts(45_296_000_000_000, 0)),
                Datum::Duration(MySqlDuration::from_raw_parts(0, 0)),
                Datum::Null,
            ],
            vec![
                Datum::Duration(MySqlDuration::from_raw_parts(3_600_000_000_000, 0)),
                Datum::Duration(MySqlDuration::from_raw_parts(1_000_000_000, 0)),
                Datum::Duration(MySqlDuration::from_raw_parts(0, 0)),
            ],
        ],
    );
    record(
        check(
            "timediff",
            call("timediff", &dur0, vec![column(0, &dur0), column(1, &dur0)]),
            &mut diff_input,
            &dur0,
        ),
        &mut failures,
    );

    // DATE_ADD/DATE_SUB: the unit lives in the function name, so each spelling
    // is one signature.
    let base = time_value(2024, 1, 31, 12, 0, 0, 0, 0);
    for (unit, fsp) in [
        ("microsecond", 6),
        ("second", 0),
        ("minute", 0),
        ("hour", 0),
        ("day", 0),
        ("week", 0),
        ("month", 0),
        ("quarter", 0),
        ("year", 0),
    ] {
        let result = datetime(fsp);
        let mut input = fixture(
            &[dt0.clone(), ints.clone()],
            &[
                vec![base.clone(), Datum::Null],
                vec![Datum::Int(3), Datum::Int(1)],
            ],
        );
        for prefix in ["date_add_", "date_sub_"] {
            let name = format!("{prefix}{unit}");
            record(
                check(
                    &name,
                    call(&name, &result, vec![column(0, &dt0), column(1, &ints)]),
                    &mut input,
                    &result,
                ),
                &mut failures,
            );
        }
    }
    for (unit, amount, fsp) in [
        ("second_microsecond", "1.500000", 6),
        ("minute_microsecond", "1:2.500000", 6),
        ("hour_microsecond", "1:2:3.500000", 6),
        ("day_microsecond", "1 2:3:4.500000", 6),
        ("minute_second", "1:2", 0),
        ("hour_second", "1:2:3", 0),
        ("hour_minute", "1:2", 0),
        ("day_second", "1 2:3:4", 0),
        ("day_minute", "1 2:3", 0),
        ("day_hour", "1 2", 0),
        ("year_month", "1-2", 0),
    ] {
        let result = datetime(fsp);
        let mut input = fixture(
            &[dt0.clone(), strings.clone()],
            &[
                vec![base.clone(), Datum::Null],
                vec![string(amount), string(amount)],
            ],
        );
        for prefix in ["date_add_", "date_sub_"] {
            let name = format!("{prefix}{unit}");
            record(
                check(
                    &name,
                    call(&name, &result, vec![column(0, &dt0), column(1, &strings)]),
                    &mut input,
                    &result,
                ),
                &mut failures,
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_json_extended_differential() {
    let mut failures = Vec::new();
    let ints = int();
    let strings = text();
    let json_ty = json();
    let mut doc_input = fixture(
        std::slice::from_ref(&json_ty),
        &[vec![
            json_value(r#"{"a":[1,2],"b":true}"#),
            json_value(r#"[1,2,3]"#),
            json_value("42"),
            Datum::Null,
        ]],
    );
    record(
        check(
            "json_array",
            call("json_array", &strings, vec![column(0, &json_ty)]),
            &mut doc_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "json_keys",
            call("json_keys", &strings, vec![column(0, &json_ty)]),
            &mut doc_input,
            &strings,
        ),
        &mut failures,
    );
    // JSON_UNQUOTE reads an ETString (the canonical JSON text), not a JSON cell.
    let mut unquote_input = fixture(
        std::slice::from_ref(&strings),
        &[vec![
            string("\"hello\""),
            string("42"),
            string("[1, 2]"),
            Datum::Null,
        ]],
    );
    record(
        check(
            "json_unquote",
            call("json_unquote", &strings, vec![column(0, &strings)]),
            &mut unquote_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "json_object",
            call(
                "json_object",
                &strings,
                vec![literal(string("k"), &strings), column(0, &json_ty)],
            ),
            &mut doc_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "json_contains",
            call(
                "json_contains",
                &ints,
                vec![
                    column(0, &json_ty),
                    literal(json_value(r#"[1,2,3]"#), &json_ty),
                ],
            ),
            &mut doc_input,
            &ints,
        ),
        &mut failures,
    );
    // json_array_append is deliberately not fixtured here: appending a JSON
    // array value through a nested path diverges (see the coverage report).
    record(
        check(
            "json_remove",
            call(
                "json_remove",
                &strings,
                vec![column(0, &json_ty), literal(string("$.a"), &strings)],
            ),
            &mut doc_input,
            &strings,
        ),
        &mut failures,
    );
    for name in [
        "json_merge_patch",
        "json_merge_preserve",
        "json_set",
        "json_insert",
        "json_replace",
    ] {
        let arguments = if matches!(name, "json_merge_patch" | "json_merge_preserve") {
            vec![
                literal(json_value(r#"{"a":[1,2],"b":true}"#), &json_ty),
                literal(json_value(r#"[1,2,3]"#), &json_ty),
            ]
        } else {
            vec![
                literal(json_value(r#"{"a":[1,2],"b":true}"#), &json_ty),
                literal(string("$.c"), &strings),
                literal(json_value(r#"[1,2,3]"#), &json_ty),
            ]
        };
        record(
            check(
                name,
                call(name, &strings, arguments),
                &mut doc_input,
                &strings,
            ),
            &mut failures,
        );
    }
    record(
        check(
            "json_quote",
            call(
                "json_quote",
                &strings,
                vec![literal(string("plain text"), &strings)],
            ),
            &mut doc_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "json_member_of",
            call(
                "json_member_of",
                &ints,
                vec![
                    literal(json_value(r#"[1,2,3]"#), &json_ty),
                    literal(json_value(r#"{"a":[1,2],"b":true}"#), &json_ty),
                ],
            ),
            &mut doc_input,
            &ints,
        ),
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_cast_family_differential() {
    let mut failures = Vec::new();
    let ints = int();
    let reals = real();
    let decs = decimal();
    let mut int_input = fixture(
        std::slice::from_ref(&ints),
        &[vec![Datum::Int(7), Datum::Int(-3), Datum::Null]],
    );
    record(
        check(
            "cast_decimal_int",
            call("cast_decimal", &decs, vec![column(0, &ints)]),
            &mut int_input,
            &decs,
        ),
        &mut failures,
    );
    record(
        check(
            "cast_double_int",
            call("cast_double", &reals, vec![column(0, &ints)]),
            &mut int_input,
            &reals,
        ),
        &mut failures,
    );
    // `cast_char`/`cast_binary` are the string spellings of an explicit CAST;
    // both land on `Cast{source}AsString` with the target charset in the
    // result type, so an integer source has to run in the engine over a column.
    let strings = text();
    record(
        check(
            "cast_char_int",
            call("cast_char", &strings, vec![column(0, &ints)]),
            &mut int_input,
            &strings,
        ),
        &mut failures,
    );
    record(
        check(
            "cast_binary_int",
            call("cast_binary", &strings, vec![column(0, &ints)]),
            &mut int_input,
            &strings,
        ),
        &mut failures,
    );
    // `NULLIF(a, b)` is lowered as `IF(a <=> b, NULL, a)`. The condition runs
    // in the comparison's promoted type while the value comes back as `a`'s
    // type, so both the same-type shape and a promoted condition have to run in
    // the engine over a column.
    record(
        check(
            "nullif_same_type",
            call(
                "nullif",
                &ints,
                vec![column(0, &ints), literal(Datum::Int(-3), &ints)],
            ),
            &mut int_input,
            &ints,
        ),
        &mut failures,
    );
    record(
        check(
            "nullif_promoted_condition",
            call(
                "nullif",
                &ints,
                vec![column(0, &ints), literal(dec("2.0000"), &decs)],
            ),
            &mut int_input,
            &ints,
        ),
        &mut failures,
    );
    // `cast_signed`/`cast_unsigned` are the rewriter's spellings for an
    // explicit `CAST(x AS SIGNED|UNSIGNED)`; the local arm derives
    // `Cast{source}AsInt` from the function's own type, so both an integer and
    // a decimal source have to run in the engine over a column.
    record(
        check(
            "cast_signed_int",
            call("cast_signed", &ints, vec![column(0, &ints)]),
            &mut int_input,
            &ints,
        ),
        &mut failures,
    );
    record(
        check(
            "cast_signed_decimal",
            call("cast_signed", &ints, vec![literal(dec("2.5000"), &decs)]),
            &mut int_input,
            &ints,
        ),
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// TiKV's `IsIPv4`/`IsIPv6`/compat/mapped kernels return 0 for a NULL input
/// where Go returns NULL; the adapter restores it with a leaf-only NULL mask.
/// The NULL row is the point of this fixture.
#[test]
fn tikv_coverage_is_ipv_family_masks_null() {
    let ty = bytes();
    let result = int();
    let mut input = fixture(
        std::slice::from_ref(&ty),
        &[vec![
            string("127.0.0.1"),
            Datum::Null,
            string("::1"),
            string("::ffff:127.0.0.1"),
            string("::127.0.0.1"),
        ]],
    );
    for name in ["is_ipv4", "is_ipv6", "is_ipv4_compat", "is_ipv4_mapped"] {
        check(
            name,
            call(name, &result, vec![column(0, &ty)]),
            &mut input,
            &result,
        )
        .unwrap();
    }
}

/// SET is the one input-only eval type the native evaluator has. A SET column
/// must survive the exact bridge in both directions (identity projection) and
/// be readable as its comma-joined name by a string kernel.
#[test]
fn tikv_coverage_set_column_round_trip_and_string_use() {
    use tidb_datatype::MysqlSet;

    let set_type = FieldType::new(FieldTypeCode::Set).with_elems(["a", "b", "c"]);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&set_type), 5);
    input.append_set(0, &MysqlSet::new("a,c", 0b101));
    input.append_set(0, &MysqlSet::new("b", 0b010));
    input.append_set(0, &MysqlSet::new("", 0));
    input.append_set(0, &MysqlSet::new("a,b,c", 0b111));
    input.append_null(0);
    input.set_sel(Some(vec![3, 0, 2, 1, 4, 0]));

    // Identity: the engine carries the SET value in and out again.
    check("set_identity", column(0, &set_type), &mut input, &set_type).unwrap();

    // A SET reads as its name in a string context, the way the native
    // evaluator reads `Datum::Set`.
    check(
        "set_length",
        call("length", &int(), vec![column(0, &set_type)]),
        &mut input,
        &int(),
    )
    .unwrap();
}

/// `eval_row_values` is the entry point a row-at-a-time call site uses when it
/// has one row with columns. It must answer the same value the native row
/// evaluator would, must actually run the engine when the resolver has one, and
/// must preserve original column indexes, including sparse references.
#[test]
fn eval_row_values_preserves_indexes_and_matches_native() {
    let ty = int();
    let dense = call(
        "plus",
        &ty,
        vec![column(0, &ty), literal(Datum::Int(1), &ty)],
    );
    // No engine context: the suite answers natively, which is the coexistence
    // contract for a resolver that has not opted in.
    assert_eq!(
        tidb_expr::evaluator::eval_row_values(&dense, &TestContext::default(), &[Datum::Int(41)])
            .unwrap(),
        Some(Datum::Int(42))
    );
    // With the engine: same value, and the engine really ran.
    let engine = TestContext {
        backend: Some(Backend::Copying),
        ..TestContext::default()
    };
    assert_eq!(
        tidb_expr::evaluator::eval_row_values(&dense, &engine, &[Datum::Int(41)]).unwrap(),
        Some(Datum::Int(42))
    );
    assert_eq!(engine.rows.get(), 1);

    // Column 3 requires a fourth value, not an external native fallback.
    let mut third = Column::new(4, ty.clone());
    third.index = 3;
    let sparse = call(
        "plus",
        &ty,
        vec![Expression::Column(third), literal(Datum::Int(1), &ty)],
    );
    assert!(tidb_expr::evaluator::eval_row_values(&sparse, &engine, &[Datum::Int(41)]).is_err());
    for backend in [None, Some(Backend::Copying), Some(Backend::Borrowed)] {
        let ctx = TestContext {
            backend,
            ..TestContext::default()
        };
        assert_eq!(
            tidb_expr::evaluator::eval_row_values(
                &sparse,
                &ctx,
                &[Datum::Int(9), Datum::Null, Datum::Int(7), Datum::Int(41)],
            )
            .unwrap(),
            Some(Datum::Int(42))
        );
        let reordered = call("minus", &ty, vec![column(1, &ty), column(0, &ty)]);
        assert_eq!(
            tidb_expr::evaluator::eval_row_values(
                &reordered,
                &ctx,
                &[Datum::Int(3), Datum::Int(10)],
            )
            .unwrap(),
            Some(Datum::Int(7))
        );
        if backend.is_some() {
            assert_eq!(ctx.rows.get(), 2);
        }
    }
}

#[test]
fn scalar_row_helpers_handle_empty_values_and_required_context() {
    use tidb_expr::evaluator::{eval_constant_row, eval_row_values, EvaluatorError};
    let expression = literal(Datum::Int(42), &int());
    for backend in [None, Some(Backend::Copying), Some(Backend::Borrowed)] {
        let ctx = TestContext {
            backend,
            ..TestContext::default()
        };
        assert_eq!(
            eval_row_values(&expression, &ctx, &[]).unwrap(),
            Some(Datum::Int(42))
        );
        assert_eq!(
            eval_constant_row(&expression, &ctx).unwrap(),
            Datum::Int(42)
        );
        let value = Datum::BinaryLiteral(vec![0x41].into());
        let binary = literal(value.clone(), &bytes());
        assert_eq!(eval_constant_row(&binary, &ctx).unwrap(), value);
        assert_eq!(eval_row_values(&binary, &ctx, &[]).unwrap(), Some(value));
    }
    let required = TestContext {
        engine_required: true,
        ..TestContext::default()
    };
    for result in [
        eval_constant_row(&expression, &required),
        eval_row_values(&expression, &required, &[]).map(|v| v.unwrap()),
    ] {
        assert!(matches!(
            result,
            Err(EvaluatorError::Eval(tidb_expr::EvalError::ExternalEngine {
                code: 1105,
                ..
            }))
        ));
    }
    let invalid = call("unknown_scalar_row_helper", &int(), vec![]);
    let ctx = TestContext::default();
    assert!(eval_constant_row(&invalid, &ctx).is_err());
    assert!(eval_row_values(&invalid, &ctx, &[]).is_err());
}

/// `eval_chunk` is the seam a row loop uses: one expression, every row of a
/// chunk the caller already built. It must answer the same values the native
/// row evaluator would, must run the engine when the resolver has one (one
/// engine row per evaluated row, not one per expression), must leave the input
/// chunk untouched, and must hand a resolver that requires the engine the
/// structured error when the adapter declines.
#[test]
fn eval_chunk_matches_native_row_by_row_and_leaves_the_input_alone() {
    let ty = int();
    let expression = call(
        "plus",
        &ty,
        vec![column(0, &ty), literal(Datum::Int(1), &ty)],
    );
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
    for value in [1i64, 2, 3] {
        input.append_datum(0, &Datum::Int(value));
    }
    let before_rows = input.num_rows();

    // No engine context: the suite answers natively, row by row.
    assert_eq!(
        tidb_expr::evaluator::eval_chunk(&expression, &TestContext::default(), &input).unwrap(),
        vec![Datum::Int(2), Datum::Int(3), Datum::Int(4)]
    );

    // With the engine: the same values, and the engine counted every row.
    let engine = TestContext {
        backend: Some(Backend::Copying),
        ..TestContext::default()
    };
    assert_eq!(
        tidb_expr::evaluator::eval_chunk(&expression, &engine, &input).unwrap(),
        vec![Datum::Int(2), Datum::Int(3), Datum::Int(4)]
    );
    assert_eq!(engine.rows.get(), 3);
    assert!(
        engine.fallbacks.borrow().is_empty(),
        "an admitted expression must not record a fallback: {:?}",
        engine.fallbacks.borrow()
    );
    assert_eq!(input.num_rows(), before_rows);

    // A required-engine resolver gets the structured error, not a native
    // answer, when the adapter declines: `now` has no engine path.
    let required = TestContext {
        backend: Some(Backend::Copying),
        engine_required: true,
        ..TestContext::default()
    };
    let declined = call("now", &ty, Vec::new());
    let error = tidb_expr::evaluator::eval_chunk(&declined, &required, &input)
        .expect_err("a required-engine resolver must not evaluate natively");
    assert!(
        matches!(
            error,
            tidb_expr::evaluator::EvaluatorError::Eval(tidb_expr::EvalError::ExternalEngine {
                code: 1105,
                ..
            })
        ),
        "unexpected error: {error:?}"
    );
}

/// A retained chunk suite must compile its admitted engine expression once, not
/// once per chunk. This is the program/execution split used by row-loop
/// operators such as `VecGroupChecker`.
#[test]
fn retained_chunk_suite_reuses_its_engine_program() {
    let ty = int();
    let expression = call(
        "plus",
        &ty,
        vec![column(0, &ty), literal(Datum::Int(1), &ty)],
    );
    let program = Arc::new(EvaluatorProgram::new(vec![expression], true));
    let suite = EvaluatorSuite::from_program(Arc::clone(&program));
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
    input.append_datum(0, &Datum::Int(4));
    input.append_datum(0, &Datum::Int(8));
    let engine = TestContext {
        backend: Some(Backend::Copying),
        ..TestContext::default()
    };

    for _ in 0..2 {
        assert_eq!(
            suite.eval_chunk(&engine, &input).unwrap(),
            vec![Datum::Int(5), Datum::Int(9)]
        );
    }

    assert_eq!(engine.rows.get(), 4);
    assert_eq!(program.tikv_compilations(), 1);
}
