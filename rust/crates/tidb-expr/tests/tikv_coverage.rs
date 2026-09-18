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

use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{
    BinaryJSON, Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration, SessionTimeZone, Time,
    TimeType, VectorFloat32,
};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::evaluator::EvaluatorSuite;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::tikv::{Backend, Context, TikvExpression};
use tidb_expr::Columns;

#[derive(Default)]
struct TestContext {
    backend: Option<Backend>,
    rows: Cell<usize>,
    borrowed: Cell<usize>,
    warnings: RefCell<Vec<u16>>,
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
    fn tikv_expression_backend(&self) -> Backend {
        self.backend.unwrap_or_default()
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.rows.set(self.rows.get() + rows);
    }
    fn record_tikv_borrowed_expression_rows(&self, rows: usize) {
        self.borrowed.set(self.borrowed.get() + rows);
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
        "sin", "cos", "tan", "cot", "asin", "acos", "atan", "sqrt", "exp", "degrees", "radians",
        "log2", "log10", "ln", "log",
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
        for name in ["strcmp", "instr", "locate", "find_in_set"] {
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
        record(
            check(
                &format!("crc32_{tag}"),
                call("crc32", &int().with_flags(1 << 5), vec![column(0, &ty)]),
                &mut input,
                &int().with_flags(1 << 5),
            ),
            &mut failures,
        );
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
    let expression = call(
        "if",
        &ty,
        vec![
            literal(Datum::Int(0), &ty),
            overflow,
            literal(Datum::Int(7), &ty),
        ],
    );
    assert!(TikvExpression::compile(&expression, Context::default())
        .unwrap()
        .is_none());
    let context = TestContext {
        backend: Some(Backend::Borrowed),
        ..TestContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
    suite.run(&context, &mut input, &mut output).unwrap();
    assert_eq!(context.rows.get(), 0);
    for row in 0..3 {
        assert_eq!(output.get_row(row).get_int64(0), 7);
    }
}
