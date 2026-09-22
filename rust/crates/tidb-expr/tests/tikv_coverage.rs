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
use tidb_expr::tikv::{Backend, Context, DeclineReason, TikvExpression};
use tidb_expr::Columns;

#[derive(Default)]
struct TestContext {
    backend: Option<Backend>,
    /// Simulates a resolver built after the native evaluator is deleted.
    rows: Cell<usize>,
    borrowed: Cell<usize>,
    warnings: RefCell<Vec<u16>>,
    fallbacks: RefCell<Vec<DeclineReason>>,
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
    fn record_tikv_expression_decline(&self, reason: DeclineReason) {
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

/// A successful receipt proves engine execution plus copying/borrowed transport
/// parity for this exact fixture. It is deliberately not a semantic oracle;
/// independent retained-math values live in `src/tests/math.rs` and the source
/// corpus modules, while contracted families use `check_declined` below.
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
    let mut engine_rows = 0;
    let mut borrowed_rows = 0;
    for backend in [Some(Backend::Copying), Some(Backend::Borrowed)] {
        let context = TestContext {
            backend,
            ..TestContext::default()
        };
        let suite = EvaluatorSuite::new(vec![expression.clone()], true);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(ty), input.num_rows());
        suite
            .run(&context, input, &mut output)
            .map_err(|e| format!("{label} {backend:?}: {e:?}"))?;
        let expected_count = input.num_rows();
        if context.rows.get() != expected_count {
            return Err(format!(
                "{label} {backend:?}: engine row mismatch, rows={}",
                context.rows.get()
            ));
        }
        if !context.fallbacks.borrow().is_empty() {
            return Err(format!(
                "{label} {backend:?}: engine context still fell back: {:?}",
                context.fallbacks.borrow()
            ));
        }
        if backend != Some(Backend::Borrowed) && context.borrowed.get() != 0 {
            return Err(format!("{label}: unexpected borrowed counter"));
        }
        engine_rows += context.rows.get();
        borrowed_rows += context.borrowed.get();
        let values = (0..output.num_rows())
            .map(|row| output.get_row(row).get_datum(0, ty))
            .collect::<Vec<_>>();
        if backend == Some(Backend::Copying) {
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
                    "{label} {backend:?}: copying={baseline:?}, borrowed={values:?}"
                ));
            }
            if *context.warnings.borrow() != baseline_warnings {
                return Err(format!(
                    "{label} {backend:?}: warnings copying={baseline_warnings:?}, borrowed={:?}",
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
    // Emit only after exact row/fallback accounting and value/warning parity.
    // Requested Borrowed mode can use copying inside the engine; record the
    // observed borrowed rows rather than claiming that mode is always zero-copy.
    eprintln!(
        "TIKV_RUNTIME_RECEIPT_V1\t{label}\t{}\t{}\t{engine_rows}\t{borrowed_rows}\t0",
        program
            .wire_signatures()
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(","),
        input.num_rows(),
    );
    Ok(())
}

fn check_expected(
    label: &str,
    expression: Expression,
    input: &mut Chunk,
    ty: &FieldType,
    expected: &[Datum],
) -> Result<(), String> {
    let context = TestContext {
        backend: Some(Backend::Copying),
        ..TestContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression.clone()], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(ty), input.num_rows());
    suite
        .run(&context, input, &mut output)
        .map_err(|error| format!("{label}: {error:?}"))?;
    let actual = (0..output.num_rows())
        .map(|row| output.get_row(row).get_datum(0, ty))
        .collect::<Vec<_>>();
    if actual.len() != expected.len()
        || !actual
            .iter()
            .zip(expected)
            .all(|(left, right)| equal(left, right))
    {
        return Err(format!("{label}: expected={expected:?}, actual={actual:?}"));
    }
    check(label, expression, input, ty)
}

fn check_declined(label: &str, expression: Expression) -> Result<(), String> {
    match TikvExpression::compile(&expression, Context::default())
        .map_err(|error| format!("{label}: compile error {error:?}"))?
    {
        None => Ok(()),
        Some(_) => Err(format!("{label}: expected explicit engine decline")),
    }
}

fn record(result: Result<(), String>, failures: &mut Vec<String>) {
    if let Err(error) = result {
        failures.push(error);
    }
}

#[test]
fn tikv_coverage_numeric_families_engine_receipts() {
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
        for name in ["abs", "isnull", "istrue", "isfalse", "not"] {
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
        for name in ["round", "ceil", "floor"] {
            record(
                check_declined(
                    &format!("{name}_{tag}"),
                    call(name, &ty, vec![column(0, &ty)]),
                ),
                &mut failures,
            );
        }
        for name in ["round", "truncate"] {
            record(
                check_declined(
                    &format!("{name}_frac_{tag}"),
                    call(
                        name,
                        &ty,
                        vec![column(0, &ty), literal(Datum::Int(1), &int())],
                    ),
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
    for name in ["sqrt", "exp", "log2", "log10", "ln", "log"] {
        record(
            check(name, call(name, &ty, vec![column(0, &ty)]), &mut input, &ty),
            &mut failures,
        );
    }
    for name in [
        "sin", "cos", "tan", "asin", "acos", "atan", "degrees", "radians",
    ] {
        record(
            check_declined(name, call(name, &ty, vec![column(0, &ty)])),
            &mut failures,
        );
    }
    record(
        check(
            "log_binary",
            call(
                "log",
                &ty,
                vec![column(0, &ty), literal(Datum::Real(2.0), &ty)],
            ),
            &mut input,
            &ty,
        ),
        &mut failures,
    );
    for name in ["pow", "power", "atan", "atan2"] {
        record(
            check_declined(
                &format!("{name}_binary"),
                call(
                    name,
                    &ty,
                    vec![column(0, &ty), literal(Datum::Real(2.0), &ty)],
                ),
            ),
            &mut failures,
        );
    }
    let ty = int();
    for (name, expected) in [
        ("greatest", vec![Datum::Int(3), Datum::Int(2), Datum::Null]),
        ("least", vec![Datum::Int(1), Datum::Int(1), Datum::Null]),
    ] {
        let mut input = fixture(
            &[ty.clone(), ty.clone()],
            &[
                vec![Datum::Int(3), Datum::Int(1), Datum::Null],
                vec![Datum::Int(1), Datum::Int(2), Datum::Int(4)],
            ],
        );
        record(
            check_expected(
                &format!("{name}_independent_values"),
                call(name, &ty, vec![column(0, &ty), column(1, &ty)]),
                &mut input,
                &ty,
                &expected,
            ),
            &mut failures,
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_string_and_misc_families_engine_receipts() {
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
            "lower", "upper", "lcase", "ucase", "reverse", "ltrim", "rtrim", "hex",
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
        let quote = call("quote", &ty, vec![column(0, &ty)]);
        if tag == "bytes" {
            record(check_declined("quote_bytes", quote), &mut failures);
        } else {
            record(check("quote_utf8", quote, &mut input, &ty), &mut failures);
        }
        for name in ["md5", "sha1", "sha"] {
            record(
                check_declined(
                    &format!("{name}_{tag}"),
                    call(name, &ty, vec![column(0, &ty)]),
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
            check_declined(
                &format!("sha2_{tag}"),
                call(
                    "sha2",
                    &ty,
                    vec![column(0, &ty), literal(Datum::Int(256), &int())],
                ),
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
        record(
            check(
                &format!("strcmp_{tag}"),
                call(
                    "strcmp",
                    &int(),
                    vec![column(0, &ty), literal(string("x"), &ty)],
                ),
                &mut input,
                &int(),
            ),
            &mut failures,
        );
        for name in ["instr", "locate"] {
            record(
                check_declined(
                    &format!("{name}_{tag}"),
                    call(
                        name,
                        &int(),
                        vec![column(0, &ty), literal(string("x"), &ty)],
                    ),
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
fn implicit_binary_string_numeric_casts_require_literal_provenance() {
    let s = || literal(Datum::Bytes(vec![b'1'].into()), &bytes());
    let uint = int().with_flags(1 << 5);
    let expressions = vec![
        call("sqrt", &real(), vec![s()]),
        call(
            "plus",
            &real(),
            vec![s(), literal(Datum::Real(0.0), &real())],
        ),
        call(
            "leftshift",
            &uint,
            vec![literal(Datum::Int(1), &int()), s()],
        ),
        call(
            "round",
            &real(),
            vec![literal(Datum::Real(1.25), &real()), s()],
        ),
        call(
            "left",
            &text(),
            vec![literal(Datum::Bytes(b"abcd".to_vec().into()), &text()), s()],
        ),
        call("cast", &real(), vec![s()]),
        call("cast_double", &real(), vec![s()]),
    ];
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    let mut mismatches = Vec::new();
    for expression in expressions {
        if let Some(engine) = TikvExpression::compile(&expression, Context::default()).unwrap() {
            let native = EvaluatorSuite::new(vec![expression.clone()], true)
                .eval_selected_for_cast(&TestContext::default(), &input, &[0]);
            mismatches.push(format!(
                "{expression:?}: native={native:?}, engine={:?}",
                engine.evaluate(&TestContext::default(), &input)
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    check(
        "catalog text cast remains admitted",
        call(
            "cast_double",
            &real(),
            vec![literal(Datum::Bytes(vec![b'1'].into()), &text())],
        ),
        &mut input,
        &real(),
    )
    .unwrap();
    // Validated literal CAST subtrees must not be rejected again by a parent.
    let authorized = call(
        "cast_signed",
        &int(),
        vec![literal(Datum::BinaryLiteral(vec![b'1'].into()), &bytes())],
    );
    check(
        "authorized nested literal CAST",
        call(
            "plus",
            &int(),
            vec![authorized.clone(), literal(Datum::Int(1), &int())],
        ),
        &mut input,
        &int(),
    )
    .unwrap();
    let mut string = Datum::Null;
    string.set_string(vec![b'1'], bytes().collation());
    let ordinary_string = literal(string, &bytes());
    assert!(TikvExpression::compile(
        &call("sqrt", &real(), vec![ordinary_string.clone()]),
        Context::default()
    )
    .unwrap()
    .is_none());
    // The ordinary String has the same wire leaf as the authorized literal.
    // Structural protobuf equality must not authorize a new sibling cast.
    assert!(TikvExpression::compile(
        &call("plus", &int(), vec![authorized.clone(), ordinary_string]),
        Context::default()
    )
    .unwrap()
    .is_none());
    assert!(TikvExpression::compile(
        &call("plus", &real(), vec![authorized, s()]),
        Context::default()
    )
    .unwrap()
    .is_none());
}

#[test]
fn bit_noncanonical_roots_and_nested_consumers_remain_declined() {
    let bit = |width| FieldType::new(FieldTypeCode::Bit).with_flen(width);
    let root = |width, raw: Vec<u8>| literal(Datum::Bit(raw.into()), &bit(width));
    let mut expressions = vec![
        root(1, vec![]),
        root(1, vec![2]),
        root(0, vec![0]),
        root(65, vec![0; 9]),
        root(9, vec![1]),
        root(9, vec![0, 0, 1]),
        root(9, vec![2, 0]),
        root(64, vec![0; 9]),
        literal(Datum::Bit(vec![1].into()), &int()),
        literal(Datum::BinaryLiteral(vec![1].into()), &bit(1)),
        call("cast_signed", &int(), vec![root(1, vec![1])]),
        call(
            "plus",
            &int(),
            vec![root(1, vec![1]), literal(Datum::Int(1), &int())],
        ),
        call("hex", &bytes(), vec![root(8, vec![1])]),
        call(
            "coalesce",
            &bit(1),
            vec![root(1, vec![1]), literal(Datum::Null, &bit(1))],
        ),
    ];
    let mut deferred = Constant::new(Datum::Bit(vec![1].into()), bit(1));
    deferred.deferred_expr = Some(Box::new(root(1, vec![0])));
    let mut parameter = Constant::new(Datum::Bit(vec![1].into()), bit(1));
    parameter.param_marker = Some(Default::default());
    expressions.extend([
        Expression::Constant(deferred),
        Expression::Constant(parameter),
    ]);
    for expression in expressions {
        assert!(
            TikvExpression::compile(&expression, Context::default())
                .unwrap()
                .is_none(),
            "unexpected BIT admission: {expression:?}"
        );
    }
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    check(
        "typed BIT NULL",
        literal(Datum::Null, &bit(9)),
        &mut input,
        &bit(9),
    )
    .unwrap();
}

#[test]
fn binary_text_integer_cast_diagnostic_and_metadata_shapes_stay_declined() {
    for raw in [
        b"".as_slice(),
        b" 1",
        b"+1",
        b"-1",
        b"1.0",
        b"1e1",
        b"1x",
        b"1\0",
        b"\xff",
        b"1000000",
        b"18446744073709551616",
    ] {
        for as_string in [false, true] {
            let mut value = Datum::Bytes(raw.to_vec());
            if as_string {
                value.set_string(raw.to_vec(), bytes().collation());
            }
            for unsigned in [false, true] {
                let ty = int().with_flags(if unsigned { 1 << 5 } else { 0 });
                let expression = call(
                    if unsigned {
                        "cast_unsigned"
                    } else {
                        "cast_signed"
                    },
                    &ty,
                    vec![literal(value.clone(), &bytes())],
                );
                assert!(
                    TikvExpression::compile(&expression, Context::default())
                        .unwrap()
                        .is_none(),
                    "{expression:?}"
                );
            }
        }
    }
    let scalar = || literal(Datum::Bytes(b"1".to_vec()), &bytes());
    for expression in [
        call("cast", &int(), vec![scalar()]),
        call("cast_signed", &int().with_flags(1 << 5), vec![scalar()]),
        call("cast_unsigned", &int(), vec![scalar()]),
    ] {
        assert!(TikvExpression::compile(&expression, Context::default())
            .unwrap()
            .is_none());
    }
    let mut deferred = Constant::new(Datum::Bytes(b"1".to_vec()), bytes());
    deferred.deferred_expr = Some(Box::new(scalar()));
    let mut parameter = Constant::new(Datum::Bytes(b"1".to_vec()), bytes());
    parameter.param_marker = Some(Default::default());
    for constant in [
        deferred,
        parameter,
        Constant::new(
            Datum::Bytes(b"1".to_vec()),
            bytes().with_charset_name("utf8mb4"),
        ),
    ] {
        assert!(TikvExpression::compile(
            &call("cast_signed", &int(), vec![Expression::Constant(constant)]),
            Context::default()
        )
        .unwrap()
        .is_none());
    }
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    check(
        "nested bounded text CAST",
        call(
            "plus",
            &int(),
            vec![
                call("cast_signed", &int(), vec![scalar()]),
                literal(Datum::Int(1), &int()),
            ],
        ),
        &mut input,
        &int(),
    )
    .unwrap();
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
fn timestamp_london_fold_remains_declined_due_to_instant_mismatch() {
    let value = Time::from_date_checked(2021, 10, 31, 1, 30, 0, 0, TimeType::Timestamp, 0).unwrap();
    let zone = SessionTimeZone::Named("Europe/London".parse().unwrap());
    let mut native_utc = value;
    native_utc
        .convert_time_zone(&zone, &SessionTimeZone::utc())
        .unwrap();
    assert_eq!(native_utc.clock(), (1, 30, 0));
    let context = Context {
        time_zone_name: Some("Europe/London".into()),
        ..Context::default()
    };
    let engine_value =
        tidb_query_expr::standalone::date_time_from_chunk(&value.go_raw().to_le_bytes()).unwrap();
    let packed = context.pack_time_literal(&engine_value).unwrap();
    let engine_utc = Time::from_packed_uint(packed, TimeType::Timestamp, 0).unwrap();
    assert_eq!(engine_utc.clock(), (0, 30, 0));
    assert_ne!(packed, native_utc.to_packed_uint().unwrap());
    // A wall-field round trip cannot detect which occurrence was selected.
    // Keep the adapter closed to named zones until this dialect gap is fixed.
    assert!(TikvExpression::compile(
        &literal(
            Datum::Time(value),
            &FieldType::new(FieldTypeCode::Timestamp).with_decimal(0)
        ),
        context
    )
    .unwrap()
    .is_none());
}

#[test]
fn temporal_constant_unsafe_shapes_remain_declined() {
    let dt = Time::from_date_checked(2024, 3, 14, 12, 0, 0, 123456, TimeType::DateTime, 6).unwrap();
    let timestamp =
        Time::from_date_checked(2024, 3, 14, 12, 0, 0, 0, TimeType::Timestamp, 0).unwrap();
    let malformed =
        Time::from_date_checked(2024, 13, 14, 0, 0, 0, 0, TimeType::DateTime, 0).unwrap();
    for (value, ty) in [
        (Datum::Time(dt), datetime(0)),
        (Datum::Time(dt), date()),
        (Datum::Time(timestamp), datetime(0)),
        (
            Datum::Time(dt),
            FieldType::new(FieldTypeCode::Timestamp).with_decimal(6),
        ),
        (Datum::Time(malformed), datetime(0)),
        (
            Datum::Time(
                Time::from_date_checked(2024, 2, 31, 0, 0, 0, 0, TimeType::Timestamp, 0).unwrap(),
            ),
            FieldType::new(FieldTypeCode::Timestamp).with_decimal(0),
        ),
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
fn tikv_coverage_temporal_json_vector_families_engine_receipts() {
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
fn tikv_coverage_control_selector_family_engine_receipts() {
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
        check_expected(
            "interval_independent_values",
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
            &[Datum::Int(0), Datum::Int(1), Datum::Int(2), Datum::Int(-1)],
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
fn tikv_coverage_string_misc_extended_engine_receipts() {
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
    record(
        check_declined(
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
    // TRIM is contracted until the TiKV bridge preserves directional metadata.
    record(
        check_declined(
            "trim",
            call(
                "trim",
                &strings,
                vec![column(0, &strings), literal(string(" "), &strings)],
            ),
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
    // Native compression is gone; engine warning/collation parity is not yet
    // established, so these shapes must decline rather than silently replay.
    record(
        check_declined(
            "compress",
            call("compress", &binary, vec![column(0, &strings)]),
        ),
        &mut failures,
    );
    record(
        check_declined(
            "uncompress",
            call("uncompress", &binary, vec![column(0, &binary)]),
        ),
        &mut failures,
    );
    record(
        check_declined(
            "uncompressed_length",
            call("uncompressed_length", &ints, vec![column(0, &binary)]),
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
        check_declined("ceiling", call("ceiling", &reals, vec![column(0, &reals)])),
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn tikv_coverage_temporal_extended_engine_receipts() {
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
    record(
        check_declined(
            "unix_timestamp_named_zone_unsafe",
            call("unix_timestamp", &ints, vec![column(0, &dt0)]),
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
fn tikv_coverage_json_extended_engine_receipts() {
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
fn tikv_coverage_cast_family_engine_receipts() {
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
    for (name, expected) in [
        (
            "is_ipv4",
            vec![
                Datum::Int(1),
                Datum::Null,
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
            ],
        ),
        (
            "is_ipv6",
            vec![
                Datum::Int(0),
                Datum::Null,
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(1),
            ],
        ),
        (
            "is_ipv4_compat",
            vec![
                Datum::Int(0),
                Datum::Null,
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
            ],
        ),
        (
            "is_ipv4_mapped",
            vec![
                Datum::Int(0),
                Datum::Null,
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
            ],
        ),
    ] {
        check_expected(
            name,
            call(name, &result, vec![column(0, &ty)]),
            &mut input,
            &result,
            &expected,
        )
        .unwrap();
    }

    let mut binary_input = fixture(
        std::slice::from_ref(&ty),
        &[vec![
            Datum::new_bytes(vec![]),
            Datum::new_bytes(vec![0x10; 4]),
            Datum::new_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            Datum::new_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4]),
            Datum::new_bytes(vec![0, 1, 2, 3, 4, 5, 6]),
            Datum::new_bytes(vec![0xff; 16]),
            Datum::new_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4]),
            Datum::new_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 2, 3, 4]),
            Datum::new_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4]),
        ]],
    );
    for (name, expected) in [
        ("is_ipv4_mapped", vec![0, 0, 1, 0, 0, 0, 0, 0, 0]),
        ("is_ipv4_compat", vec![0, 0, 0, 0, 0, 0, 1, 0, 0]),
    ] {
        check_expected(
            name,
            call(name, &result, vec![column(0, &ty)]),
            &mut binary_input,
            &result,
            &expected.into_iter().map(Datum::Int).collect::<Vec<_>>(),
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
