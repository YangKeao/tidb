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

// Included under scalar_function so the actual private vector-mode consumer is
// tested without widening its production API or constructing a different plan.
#[test]
fn numeric_argument_consumers_keep_json_and_vectorized_diagnostic_domains() {
    use super::{cast_numeric_argument, cast_numeric_argument_in_mode};
    use crate::expression::Expression;
    use crate::{constant::Constant, Columns, Datum, ErrorLevel, EvalError};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        BinaryJSON, Decimal, EvalType, FieldType, FieldTypeCode, SessionTimeZone, ERR_TRUNCATED,
        JSON_TYPE_CODE_STRING,
    };

    struct Session {
        level: Cell<ErrorLevel>,
        reads: Cell<usize>,
        veto: Cell<bool>,
        messages: RefCell<Vec<String>>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("actual operand already supplied")
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("unspecified scale must not enter final conversion")
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.reads.set(self.reads.get() + 1);
            self.level.get()
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.messages.borrow_mut().push(message.to_owned());
            if self.veto.get() {
                Err(EvalError::Unsupported("numeric argument veto"))
            } else {
                Ok(())
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }
    let ctx = Session {
        level: Cell::new(ErrorLevel::Error),
        reads: Cell::new(0),
        veto: Cell::new(false),
        messages: RefCell::new(Vec::new()),
        warnings: RefCell::new(Vec::new()),
    };
    let expression = |value: &Datum, code| {
        Expression::Constant(Constant::new(
            value.clone(),
            FieldType::new(code).with_flen(-1).with_decimal(-1),
        ))
    };
    let string = Datum::new_string(" \u{2003}-12x ");
    let string_expression = expression(&string, FieldTypeCode::VarString);
    assert_eq!(
        cast_numeric_argument(&string_expression, string.clone(), EvalType::Decimal, &ctx),
        Ok(Datum::Decimal(Decimal::from_int(-12)))
    );
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DECIMAL value: '-12x'".to_owned()]
    );
    assert_eq!(ctx.reads.get(), 0);
    for level in [ErrorLevel::Warn, ErrorLevel::Ignore, ErrorLevel::Error] {
        ctx.level.set(level);
        ctx.reads.set(0);
        let result = cast_numeric_argument_in_mode(
            &string_expression,
            string.clone(),
            EvalType::Decimal,
            &ctx,
            true,
        );
        assert_eq!(ctx.reads.get(), 1);
        assert!(ctx.messages.borrow().is_empty());
        match level {
            ErrorLevel::Error => {
                let Err(EvalError::Conversion(error)) = result else {
                    panic!("original vector truncation error")
                };
                assert_eq!(error.identity(), ERR_TRUNCATED.identity());
                assert_eq!(error.message(), ERR_TRUNCATED.message());
                assert!(ctx.warnings.borrow().is_empty());
            }
            ErrorLevel::Warn => {
                assert_eq!(result, Ok(Datum::Decimal(Decimal::from_int(-12))));
                let warning = ERR_TRUNCATED.to_sql_error();
                assert_eq!(ctx.warnings.take(), [(warning.code, warning.message)]);
            }
            ErrorLevel::Ignore => {
                assert_eq!(result, Ok(Datum::Decimal(Decimal::from_int(-12))));
                assert!(ctx.warnings.borrow().is_empty());
            }
        }
    }
    let bytes = Datum::new_bytes([b'1', b'2', 0xff]);
    assert_eq!(
        cast_numeric_argument(
            &expression(&bytes, FieldTypeCode::VarString),
            bytes,
            EvalType::Real,
            &ctx
        ),
        Ok(Datum::Real(12.0))
    );
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DOUBLE value: '12�'".to_owned()]
    );
    let json = Datum::Json(BinaryJSON::from_encoded_parts(
        JSON_TYPE_CODE_STRING,
        vec![3, b'1', b'2', 0xff],
    ));
    assert_eq!(
        cast_numeric_argument(
            &expression(&json, FieldTypeCode::Json),
            json,
            EvalType::Real,
            &ctx
        ),
        Ok(Datum::Real(0.0))
    );
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DOUBLE value: '12�'".to_owned()]
    );
    let json = Datum::Json(BinaryJSON::parse("[]").unwrap());
    assert_eq!(
        cast_numeric_argument(
            &expression(&json, FieldTypeCode::Json),
            json,
            EvalType::Real,
            &ctx
        ),
        Ok(Datum::Real(0.0))
    );
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect FLOAT value: '[]'".to_owned()]
    );
    let json = Datum::Json(BinaryJSON::parse("\"2.5\"").unwrap());
    assert_eq!(
        cast_numeric_argument(
            &expression(&json, FieldTypeCode::Json),
            json.clone(),
            EvalType::Real,
            &ctx
        ),
        Ok(Datum::Real(2.5))
    );
    assert!(ctx.messages.borrow().is_empty());
    assert_eq!(crate::tikv::eval_cast_double_in(&ctx, &json), Ok(0.0));
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect FLOAT value: '\"2.5\"'".to_owned()]
    );
    let empty = Datum::new_string("");
    assert_eq!(
        cast_numeric_argument(
            &expression(&empty, FieldTypeCode::VarString),
            empty,
            EvalType::Real,
            &ctx
        ),
        Ok(Datum::Real(0.0))
    );
    assert!(ctx.messages.borrow().is_empty());
    ctx.veto.set(true);
    ctx.reads.set(0);
    assert_eq!(
        cast_numeric_argument(&string_expression, string, EvalType::Decimal, &ctx),
        Err(EvalError::Unsupported("numeric argument veto"))
    );
    assert_eq!(ctx.reads.get(), 0);
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DECIMAL value: '-12x'".to_owned()]
    );
    assert!(ctx.warnings.borrow().is_empty());
}

#[test]
fn numeric_argument_shape_and_json_integer_keep_effective_types_and_utc_independence() {
    use super::{cast_numeric_argument, numeric_decimal_cast_type};
    use crate::{
        constant::Constant, expression::Expression, Columns, Datum, ErrorLevel, EvalError,
    };
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        BinaryJSON, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone,
    };

    // The integral domain ignores source width/scale; BIT and integer-mode
    // hybrids use the old default width rather than a display-name width.
    for (code, width) in [
        (FieldTypeCode::Tiny, 3),
        (FieldTypeCode::Short, 5),
        (FieldTypeCode::Int24, 8),
        (FieldTypeCode::Long, 10),
        (FieldTypeCode::LongLong, 20),
        (FieldTypeCode::Year, 4),
        (FieldTypeCode::Bit, 20),
    ] {
        let source = FieldType::new(code).with_flen(100).with_decimal(42);
        let target = numeric_decimal_cast_type(&source);
        assert_eq!(target.code(), FieldTypeCode::NewDecimal);
        assert_eq!((target.flen(), target.decimal()), (width, 0));
    }
    for code in [FieldTypeCode::Enum, FieldTypeCode::Set] {
        let source = FieldType::new(code)
            .with_raw_flags(u64::from(FieldTypeFlags::ENUM_SET_AS_INT))
            .with_flen(100)
            .with_decimal(42);
        let target = numeric_decimal_cast_type(&source);
        assert_eq!((target.flen(), target.decimal()), (20, 0));
    }
    for (source, expected) in [
        (
            FieldType::new(FieldTypeCode::VarString)
                .with_flen(-2)
                .with_decimal(-7),
            (65, -7),
        ),
        (
            FieldType::new(FieldTypeCode::Double)
                .with_flen(0)
                .with_decimal(42),
            (0, 30),
        ),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(3)
                .with_decimal(4),
            (3, 4),
        ),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(i64::MAX)
                .with_decimal(i64::MAX),
            (65, 30),
        ),
        // Unknown(1) must not normalize to the known TINY code.
        (
            FieldType::new(FieldTypeCode::Unknown(1))
                .with_flen(100)
                .with_decimal(42),
            (65, 30),
        ),
        (
            FieldType::new(FieldTypeCode::Tiny)
                .with_flen(17)
                .with_decimal(5)
                .with_array(true),
            (17, 5),
        ),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(100)
                .with_decimal(42)
                .with_array(true),
            (65, 30),
        ),
    ] {
        let target = numeric_decimal_cast_type(&source);
        assert_eq!(target.code(), FieldTypeCode::NewDecimal);
        assert!(!target.is_array());
        assert_eq!((target.flen(), target.decimal()), expected);
    }

    #[derive(Default)]
    struct Session {
        messages: RefCell<Vec<String>>,
        veto: Cell<bool>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("actual JSON operand already supplied")
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("JSON integer uses value-only UTC, not the session zone")
        }
        fn truncate_level(&self) -> ErrorLevel {
            panic!("only the actual truncate callback is requested")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("no full integer CAST advisory")
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.messages.borrow_mut().push(message.to_owned());
            if self.veto.get() {
                Err(EvalError::Unsupported("JSON integer veto"))
            } else {
                Ok(())
            }
        }
    }
    let ctx = Session::default();
    for (document, integer, warning_subject) in [
        ("3", 3, None),
        ("-7", -7, None),
        ("1.5", 1, Some("1.5")),
        ("\"12\"", 0, Some("\"12\"")),
        ("{}", 0, Some("{}")),
        ("null", 0, Some("null")),
    ] {
        let value = Datum::Json(BinaryJSON::parse(document).unwrap());
        let expression = Expression::Constant(Constant::new(
            value.clone(),
            FieldType::new(FieldTypeCode::Json),
        ));
        assert_eq!(
            cast_numeric_argument(&expression, value, EvalType::Int, &ctx),
            Ok(Datum::Int(integer))
        );
        let expected: Vec<_> = warning_subject
            .into_iter()
            .map(|subject| format!("Truncated incorrect INTEGER value: '{subject}'"))
            .collect();
        assert_eq!(ctx.messages.take(), expected);
    }
    ctx.veto.set(true);
    let value = Datum::Json(BinaryJSON::parse("\"12\"").unwrap());
    let expression = Expression::Constant(Constant::new(
        value.clone(),
        FieldType::new(FieldTypeCode::Json),
    ));
    assert_eq!(
        cast_numeric_argument(&expression, value, EvalType::Int, &ctx),
        Err(EvalError::Unsupported("JSON integer veto"))
    );
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect INTEGER value: '\"12\"'".to_owned()]
    );
}

#[test]
fn real_decimal_argument_keeps_lazy_subject_error_precedence_and_final_storage() {
    use super::cast_numeric_argument;
    use crate::{
        constant::{Constant, ParamMarker},
        expression::Expression,
        Columns, Datum, ErrorLevel, EvalError,
    };
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        Collation, ConversionFlags, Decimal, EvalType, FieldType, FieldTypeCode, FieldTypeFlags,
        MysqlEnum, SessionTimeZone, ERR_BAD_NUMBER, ERR_OVERFLOW,
    };

    struct Session {
        level: Cell<ErrorLevel>,
        veto: Cell<bool>,
        subject_error: Cell<bool>,
        calls: RefCell<Vec<&'static str>>,
        messages: RefCell<Vec<String>>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("actual value already supplied")
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.calls.borrow_mut().push("level");
            self.level.get()
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            assert_eq!(order, 0);
            self.calls.borrow_mut().push("subject");
            if self.subject_error.get() {
                Err(EvalError::Unsupported("subject unavailable"))
            } else {
                Ok(Datum::Real(1e100))
            }
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.calls.borrow_mut().push("truncate");
            self.messages.borrow_mut().push(message.to_owned());
            if self.veto.get() {
                Err(EvalError::Unsupported("real decimal veto"))
            } else {
                Ok(())
            }
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.calls.borrow_mut().push("zone");
            crate::context::NoColumns.time_zone()
        }
        fn type_flags(&self) -> ConversionFlags {
            self.calls.borrow_mut().push("flags");
            ConversionFlags::default()
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("no unexpected final-fit warning")
        }
    }
    let ctx = Session {
        level: Cell::new(ErrorLevel::Error),
        veto: Cell::new(false),
        subject_error: Cell::new(false),
        calls: RefCell::new(Vec::new()),
        messages: RefCell::new(Vec::new()),
    };
    let field = FieldType::new(FieldTypeCode::Double)
        .with_flen(-1)
        .with_decimal(-1);
    for (real, expected) in [
        (0.0, "0"),
        (-0.0, "0"),
        (-9_007_199_254_740_992.0, "-9007199254740992"),
        (9_007_199_254_740_992.0, "9007199254740992"),
        (9_007_199_254_740_994.0, "9007199254740994"),
        (1.25, "1.25"),
    ] {
        let value = Datum::Real(real);
        let expression = Expression::Constant(Constant::new(value.clone(), field.clone()));
        let Datum::Decimal(decimal) =
            cast_numeric_argument(&expression, value, EvalType::Decimal, &ctx).unwrap()
        else {
            panic!("decimal result")
        };
        assert_eq!(decimal.to_string(), expected);
        if real == 0.0 {
            assert!(!decimal.is_negative());
        }
        assert_eq!(decimal.declared_shape(), None);
        assert!(ctx.calls.borrow().is_empty());
    }
    let nan = Datum::Real(f64::NAN);
    let expression = Expression::Constant(Constant::new(nan.clone(), field.clone()));
    let Err(EvalError::Conversion(error)) =
        cast_numeric_argument(&expression, nan, EvalType::Decimal, &ctx)
    else {
        panic!("bad number error")
    };
    assert_eq!(error.identity(), ERR_BAD_NUMBER.identity());
    assert_eq!(error.message(), ERR_BAD_NUMBER.message());
    assert!(ctx.calls.borrow().is_empty());

    let mut parameter = Constant::new(Datum::Real(1e100), field.clone());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let expression = Expression::Constant(parameter);
    let Err(EvalError::Conversion(error)) =
        cast_numeric_argument(&expression, Datum::Real(1e100), EvalType::Decimal, &ctx)
    else {
        panic!("strict overflow error")
    };
    assert_eq!(error.identity(), ERR_OVERFLOW.identity());
    assert_eq!(error.message(), ERR_OVERFLOW.message());
    assert_eq!(ctx.calls.take(), ["level"]);
    assert!(ctx.messages.borrow().is_empty());
    for (level, unavailable) in [
        (ErrorLevel::Warn, false),
        (ErrorLevel::Ignore, false),
        (ErrorLevel::Warn, true),
    ] {
        ctx.level.set(level);
        ctx.subject_error.set(unavailable);
        let Datum::Decimal(decimal) =
            cast_numeric_argument(&expression, Datum::Real(1e100), EvalType::Decimal, &ctx)
                .unwrap()
        else {
            panic!("clamped decimal result")
        };
        assert_eq!(decimal.to_string(), "9".repeat(81));
        assert_eq!(ctx.calls.take(), ["level", "subject", "truncate"]);
        assert_eq!(
            ctx.messages.take(),
            ["Truncated incorrect DECIMAL value: '1e+100'".to_owned()]
        );
    }
    ctx.subject_error.set(false);
    ctx.veto.set(true);
    assert_eq!(
        cast_numeric_argument(&expression, Datum::Real(1e100), EvalType::Decimal, &ctx),
        Err(EvalError::Unsupported("real decimal veto"))
    );
    assert_eq!(ctx.calls.take(), ["level", "subject", "truncate"]);
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DECIMAL value: '1e+100'".to_owned()]
    );
    ctx.veto.set(false);

    // The integral fast path rejoins the SAME final target fit and context reads.
    let value = Datum::Real(12.0);
    let expression = Expression::Constant(Constant::new(
        value.clone(),
        FieldType::new(FieldTypeCode::Double)
            .with_flen(5)
            .with_decimal(2),
    ));
    let Datum::Decimal(decimal) =
        cast_numeric_argument(&expression, value, EvalType::Decimal, &ctx).unwrap()
    else {
        panic!("fitted decimal")
    };
    assert_eq!(decimal.to_string(), "12.00");
    assert_eq!(decimal.declared_shape(), Some((5, 2)));
    assert_eq!(ctx.calls.take(), ["zone", "flags"]);

    // EvalInt carries the unsigned hybrid ordinal as i64 bits (eval_numeric_row);
    // the consumer restores unsignedness before the unspecified-scale conversion.
    let field = FieldType::new(FieldTypeCode::Enum)
        .with_raw_flags(u64::from(FieldTypeFlags::UNSIGNED))
        .with_decimal(-1);
    let expression = Expression::Constant(Constant::new(
        Datum::Enum(MysqlEnum::new("ordinal", u64::MAX), Collation::DEFAULT),
        field,
    ));
    let Datum::Decimal(decimal) =
        cast_numeric_argument(&expression, Datum::Int(-1), EvalType::Decimal, &ctx).unwrap()
    else {
        panic!("unsigned decimal")
    };
    assert_eq!(decimal.to_string(), "18446744073709551615");
    assert_eq!(decimal.declared_shape(), None);
    assert!(ctx.calls.borrow().is_empty());
    let stored =
        Decimal::from_raw_parts(true, b"0001234567".to_vec(), 2, 4).with_declared_shape(20, 4);
    let value = Datum::Decimal(stored.clone());
    let expression = Expression::Constant(Constant::new(
        value.clone(),
        FieldType::new(FieldTypeCode::NewDecimal),
    ));
    let Datum::Decimal(actual) =
        cast_numeric_argument(&expression, value, EvalType::Decimal, &ctx).unwrap()
    else {
        panic!("unchanged decimal")
    };
    assert_eq!(actual.coefficient_bytes(), stored.coefficient_bytes());
    assert_eq!(actual.is_negative(), stored.is_negative());
    assert_eq!(actual.scale(), stored.scale());
    assert_eq!(actual.storage_scale(), stored.storage_scale());
    assert_eq!(actual.declared_shape(), stored.declared_shape());
    assert!(ctx.calls.borrow().is_empty());
}

#[test]
fn numeric_argument_routes_keep_preserve_precedence_normalization_and_effect_domains() {
    use super::cast_numeric_argument;
    use crate::{
        constant::Constant, expression::Expression, Columns, Datum, ErrorLevel, EvalError,
    };
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, Collation, ConversionFlags, Decimal, EvalType, FieldType, FieldTypeCode,
        FieldTypeFlags, MySqlDuration, MysqlEnum, SessionTimeZone,
    };

    #[derive(Default)]
    struct Session {
        calls: RefCell<Vec<&'static str>>,
        messages: RefCell<Vec<String>>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("already evaluated operand")
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.calls.borrow_mut().push("zone");
            crate::context::NoColumns.time_zone()
        }
        fn type_flags(&self) -> ConversionFlags {
            self.calls.borrow_mut().push("flags");
            ConversionFlags::default()
        }
        fn truncate_level(&self) -> ErrorLevel {
            panic!("no level demand for these exact decimal values")
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.calls.borrow_mut().push("truncate");
            self.messages.borrow_mut().push(message.to_owned());
            Ok(())
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("no final-fit diagnostic expected")
        }
    }
    let ctx = Session::default();
    let field = |code| FieldType::new(code).with_flen(-1).with_decimal(-1);
    let cast = |value: Datum, source: FieldType, target| {
        let expression = Expression::Constant(Constant::new(value.clone(), source));
        cast_numeric_argument(&expression, value, target, &ctx)
    };
    // These metadata mismatches deliberately lock the old early-return ordering:
    // neither target admission nor normalization precedes NULL/same-domain.
    for (value, source, target) in [
        (
            Datum::Null,
            field(FieldTypeCode::VarString),
            EvalType::VectorFloat32,
        ),
        (
            Datum::Float32(1e-50),
            field(FieldTypeCode::Double),
            EvalType::Real,
        ),
        (
            Datum::Raw(b"raw".to_vec()),
            field(FieldTypeCode::Json),
            EvalType::Json,
        ),
        (
            Datum::Int(-1),
            field(FieldTypeCode::VarString).with_raw_flags(u64::from(FieldTypeFlags::UNSIGNED)),
            EvalType::String,
        ),
    ] {
        assert_eq!(cast(value.clone(), source, target), Ok(value));
    }
    assert!(ctx.calls.borrow().is_empty());
    let mut missing = Constant::new(Datum::Null, field(FieldTypeCode::Json));
    missing.ret_type = None;
    let missing = Expression::Constant(missing);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cast_numeric_argument(&missing, Datum::Null, EvalType::Json, &ctx)
    }))
    .is_err());
    for (value, source, target, message) in [
        (
            Datum::Json(BinaryJSON::parse("3").unwrap()),
            field(FieldTypeCode::VarString),
            EvalType::Json,
            "numeric argument cast domain",
        ),
        (
            Datum::Json(BinaryJSON::parse("3").unwrap()),
            field(FieldTypeCode::VarString),
            EvalType::Real,
            "string arithmetic argument domain",
        ),
        (
            Datum::Real(1.25),
            field(FieldTypeCode::VarString),
            EvalType::Decimal,
            "string arithmetic argument domain",
        ),
        (
            Datum::Int(-1),
            field(FieldTypeCode::Double),
            EvalType::VectorFloat32,
            "numeric argument cast domain",
        ),
    ] {
        assert_eq!(
            cast(value, source, target),
            Err(EvalError::Unsupported(message))
        );
    }
    assert!(ctx.calls.borrow().is_empty());
    for (value, source, expected) in [
        (Datum::Float32(1e-50), field(FieldTypeCode::Double), "0"),
        (
            Datum::Float32(16_777_217.0),
            field(FieldTypeCode::Double),
            "16777216",
        ),
        (
            Datum::Int(-1),
            field(FieldTypeCode::Double).with_raw_flags(u64::from(FieldTypeFlags::UNSIGNED)),
            "18446744073709551615",
        ),
        (Datum::Real(1.25), field(FieldTypeCode::Enum), "1.25"),
    ] {
        let Datum::Decimal(decimal) = cast(value, source, EvalType::Decimal).unwrap() else {
            panic!("decimal route")
        };
        assert_eq!(decimal.to_string(), expected);
        assert!(ctx.calls.borrow().is_empty());
    }
    assert_eq!(
        cast(
            Datum::new_bytes(b"12x".to_vec()),
            field(FieldTypeCode::VarString),
            EvalType::Real
        ),
        Ok(Datum::Real(12.0))
    );
    assert_eq!(ctx.calls.take(), ["truncate"]);
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DOUBLE value: '12x'".to_owned()]
    );
    assert_eq!(
        cast(
            Datum::Json(BinaryJSON::parse("\"2.5\"").unwrap()),
            field(FieldTypeCode::Json),
            EvalType::Real
        ),
        Ok(Datum::Real(2.5))
    );
    assert!(ctx.calls.borrow().is_empty());
    assert_eq!(
        cast(
            Datum::Json(BinaryJSON::parse("\"9\"").unwrap()),
            field(FieldTypeCode::Json),
            EvalType::Int
        ),
        Ok(Datum::Int(0))
    );
    assert_eq!(ctx.calls.take(), ["truncate"]);
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect INTEGER value: '\"9\"'".to_owned()]
    );
    for (value, source, expected) in [
        (
            Datum::Duration(MySqlDuration::new(1, 2, 3, 0, 0).unwrap()),
            field(FieldTypeCode::Duration),
            Decimal::from_int(10203),
        ),
        (
            Datum::Json(BinaryJSON::parse("12").unwrap()),
            field(FieldTypeCode::Json),
            Decimal::from_int(12),
        ),
    ] {
        assert_eq!(
            cast(value, source, EvalType::Decimal),
            Ok(Datum::Decimal(expected))
        );
        assert_eq!(ctx.calls.take(), ["zone", "flags"]);
    }
    let hybrid = Datum::Enum(MysqlEnum::new("seventeen", 17), Collation::DEFAULT);
    assert_eq!(
        cast(hybrid, field(FieldTypeCode::Enum), EvalType::Real),
        Ok(Datum::Real(17.0))
    );
    assert_eq!(ctx.calls.take(), ["zone", "flags"]);
    assert_eq!(
        cast(
            Datum::Real(12.0),
            field(FieldTypeCode::Double),
            EvalType::Int
        ),
        Ok(Datum::Int(12))
    );
    assert_eq!(ctx.calls.take(), ["zone", "flags"]);
    assert!(ctx.messages.borrow().is_empty());
}

#[test]
fn numeric_argument_completion_keeps_default_metadata_string_integer_and_result_errors() {
    use super::{cast_numeric_argument, numeric_argument_target_field};
    use crate::{constant::Constant, expression::Expression, Columns, Datum, EvalError};
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, ConversionFlags, EvalType, FieldType, FieldTypeCode, SessionTimeZone,
        ERR_OVERFLOW, ERR_TRUNCATED_WRONG_VALUE,
    };

    #[derive(Default)]
    struct Session(RefCell<Vec<&'static str>>);
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("operand already supplied")
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.0.borrow_mut().push("zone");
            crate::context::NoColumns.time_zone()
        }
        fn type_flags(&self) -> ConversionFlags {
            self.0.borrow_mut().push("flags");
            ConversionFlags::default()
                .with_ignore_truncate_err(false)
                .with_truncate_as_warning(false)
        }
        fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
            panic!("no parsing truncation in these fixtures")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("strict conversion returns its typed error")
        }
    }
    let ctx = Session::default();
    let field = |code, flen, decimal| FieldType::new(code).with_flen(flen).with_decimal(decimal);
    let cast = |value: Datum, source: FieldType, target| {
        let expression = Expression::Constant(Constant::new(value.clone(), source));
        cast_numeric_argument(&expression, value, target, &ctx)
    };
    let (real_target, skip) = numeric_argument_target_field(
        &field(FieldTypeCode::LongLong, 20, 0),
        FieldTypeCode::Double.mysql_type(),
    );
    assert_eq!(real_target, FieldType::new(FieldTypeCode::Double));
    assert!(real_target.decimal() < 0);
    assert!(!skip); // Default negative scale does NOT skip a non-decimal conversion.
    assert_eq!(
        cast(
            Datum::Int(17),
            field(FieldTypeCode::LongLong, 20, 0),
            EvalType::Real
        ),
        Ok(Datum::Real(17.0))
    );
    assert_eq!(ctx.0.take(), ["zone", "flags"]);

    // The legacy String/Bytes argument path constructs Decimal even for target Int.
    for (target, scale) in [(EvalType::Int, -1), (EvalType::Decimal, -7)] {
        let Datum::Decimal(value) = cast(
            Datum::new_string("12.5"),
            field(FieldTypeCode::VarString, 0, scale),
            target,
        )
        .unwrap() else {
            panic!("string integer path still returns decimal")
        };
        assert_eq!(value.to_string(), "12.5");
        assert_eq!(value.declared_shape(), None);
        assert!(ctx.0.borrow().is_empty());
    }
    let Datum::Decimal(value) = cast(
        Datum::new_bytes(b"12.5".to_vec()),
        field(FieldTypeCode::VarString, 5, 1),
        EvalType::Int,
    )
    .unwrap() else {
        panic!("fitted string integer path")
    };
    assert_eq!(value.to_string(), "12.5");
    assert_eq!(value.declared_shape(), Some((5, 1)));
    assert_eq!(ctx.0.take(), ["zone", "flags"]);
    let Datum::Decimal(value) = cast(
        Datum::Real(12.0),
        field(FieldTypeCode::Double, 5, 2),
        EvalType::Decimal,
    )
    .unwrap() else {
        panic!("fitted real path")
    };
    assert_eq!(value.to_string(), "12.00");
    assert_eq!(value.declared_shape(), Some((5, 2)));
    assert_eq!(ctx.0.take(), ["zone", "flags"]);

    assert_eq!(
        cast(
            Datum::Raw(b"12".to_vec()),
            field(FieldTypeCode::LongLong, 20, 0),
            EvalType::Real
        ),
        Err(EvalError::Unsupported("numeric argument conversion failed"))
    );
    assert_eq!(ctx.0.take(), ["zone", "flags"]);
    let Err(EvalError::Conversion(error)) = cast(
        Datum::Real(999.0),
        field(FieldTypeCode::Double, 2, 0),
        EvalType::Decimal,
    ) else {
        panic!("final precision fitting returns typed overflow")
    };
    assert_eq!(error.identity(), ERR_OVERFLOW.identity());
    assert_eq!(error.message(), "DECIMAL value is out of range in '(2, 0)'");
    assert_eq!(ctx.0.take(), ["zone", "flags"]);
    let Err(EvalError::Conversion(error)) = cast(
        Datum::Json(BinaryJSON::parse("{}").unwrap()),
        field(FieldTypeCode::Json, 5, 1),
        EvalType::Decimal,
    ) else {
        panic!("context decimal error precedes final fitting")
    };
    assert_eq!(error.identity(), ERR_TRUNCATED_WRONG_VALUE.identity());
    assert_eq!(error.message(), "Truncated incorrect DECIMAL value: '{}'");
    assert_eq!(ctx.0.take(), ["zone", "flags"]);
}

#[test]
fn numeric_batch_value_boundary_preserves_domains_diagnostics_and_lazy_subjects() {
    use super::{
        cast_numeric_argument_in_mode, native_batch_integer_bits, native_batch_integer_value,
        native_numeric_batch_cast, native_numeric_batch_source,
    };
    use crate::{
        constant::Constant, expression::Expression, Columns, Datum, ErrorLevel, EvalError,
    };
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, Decimal, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone,
    };
    #[derive(Default)]
    struct Trace(RefCell<Vec<String>>);
    impl Columns for Trace {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("value-only boundary")
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.0.borrow_mut().push("zone".into());
            crate::NoColumns.time_zone()
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.0.borrow_mut().push("level".into());
            ErrorLevel::Warn
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.0.borrow_mut().push(format!("truncate:{message}"));
            Ok(())
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0
                .borrow_mut()
                .push(format!("warning:{code}:{message}"));
        }
    }
    // Already source-typed inputs: the boundary must not replay their owner.
    let values = vec![
        (FieldTypeCode::LongLong, Datum::Int(-17)),
        (FieldTypeCode::LongLong, Datum::UInt(u64::MAX)),
        (FieldTypeCode::Double, Datum::Real(1.25)),
        (FieldTypeCode::Double, Datum::Real(f64::MAX)),
        (
            FieldTypeCode::NewDecimal,
            Datum::Decimal(Decimal::from_int(-19)),
        ),
        (FieldTypeCode::VarString, Datum::new_string("12.5")),
        (
            FieldTypeCode::VarString,
            Datum::new_string(" \u{2003}-12x "),
        ),
        (FieldTypeCode::VarString, Datum::new_string("")),
        (
            FieldTypeCode::Json,
            Datum::Json(BinaryJSON::parse("{\"a\":1}").unwrap()),
        ),
        (
            FieldTypeCode::Json,
            Datum::Json(BinaryJSON::parse("2.5").unwrap()),
        ),
    ];
    for (code, raw) in values {
        for unsigned in [false, true] {
            let field = FieldType::new(code)
                .with_flen(-1)
                .with_decimal(-1)
                .with_raw_flags(if unsigned {
                    u64::from(FieldTypeFlags::UNSIGNED)
                } else {
                    0
                });
            for target in [EvalType::Int, EvalType::Real, EvalType::Decimal] {
                for value in [Datum::Null, raw.clone()] {
                    let source = native_numeric_batch_source(&field, false, target);
                    let expression =
                        Expression::Constant(Constant::new(value.clone(), field.clone()));
                    let subject = super::static_numeric_expression_text(&expression, true);
                    let native = Trace::default();
                    let result = native_numeric_batch_cast(
                        &field,
                        source,
                        target,
                        value.clone(),
                        &native,
                        subject.as_deref(),
                        None,
                    );
                    if source == target {
                        assert_eq!(result, Ok(value));
                        assert!(native.0.borrow().is_empty());
                    } else {
                        let old = Trace::default();
                        let expression =
                            Expression::Constant(Constant::new(value.clone(), field.clone()));
                        let expected =
                            cast_numeric_argument_in_mode(&expression, value, target, &old, true);
                        assert_eq!(result, expected, "{field:?} -> {target:?}");
                        assert_eq!(native.0.take(), old.0.take(), "{field:?} -> {target:?}");
                    }
                }
            }
        }
    }
    // No matching-domain coercion, context read, or diagnostic render, even
    // with an actual native formatter failure retained in immutable metadata.
    let same = Trace::default();
    let field = FieldType::new(FieldTypeCode::Double);
    assert_eq!(
        native_numeric_batch_cast(
            &field,
            EvalType::Real,
            EvalType::Real,
            Datum::Float32(2.0),
            &same,
            None,
            Some(f64::NAN.to_le_bytes())
        ),
        Ok(Datum::Float32(2.0))
    );
    assert!(same.0.borrow().is_empty());
    for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Set] {
        let field = FieldType::new(code);
        for target in [EvalType::Int, EvalType::Real, EvalType::Decimal] {
            assert_eq!(native_numeric_batch_source(&field, true, target), target);
            let expected = if field.is_hybrid() {
                EvalType::Int
            } else {
                field.eval_type()
            };
            assert_eq!(native_numeric_batch_source(&field, false, target), expected);
        }
    }
    for bits in [i64::MIN, -1, 0, 1, i64::MAX] {
        for unsigned in [false, true] {
            let value = native_batch_integer_value(Some(bits), unsigned);
            assert_eq!(native_batch_integer_bits(value), Ok(Some(bits)));
        }
    }
    assert_eq!(native_batch_integer_bits(Datum::Null), Ok(None));
    assert_eq!(native_batch_integer_value(None, true), Datum::Null);
    for value in [
        Datum::Real(1.0),
        Datum::new_string("1"),
        Datum::Decimal(Decimal::from_int(1)),
    ] {
        assert_eq!(
            native_batch_integer_bits(value),
            Err(EvalError::Unsupported(
                "integer batch value is outside its typed domain"
            ))
        );
    }
}

mod staged_values {
    use super::super::*;
    use std::cell::{Cell, RefCell};
    struct Session {
        level: crate::ErrorLevel,
        veto: bool,
        events: RefCell<Vec<String>>,
        calls: Cell<usize>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("numeric operand already supplied")
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            self.events.borrow_mut().push("zone".into());
            tidb_datatype::SessionTimeZone::utc()
        }
        fn type_flags(&self) -> tidb_datatype::ConversionFlags {
            self.events.borrow_mut().push("flags".into());
            tidb_datatype::DEFAULT_STATEMENT_FLAGS
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.events
                .borrow_mut()
                .push(format!("level:{:?}", self.level));
            self.level
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.events.borrow_mut().push(format!("truncate:{message}"));
            if self.veto {
                Err(EvalError::Unsupported("staged numeric warning veto"))
            } else {
                Ok(())
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events
                .borrow_mut()
                .push(format!("warning:{code}:{message}"));
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.calls.set(self.calls.get() + 1);
            self.events.borrow_mut().push(format!("param:{order}"));
            if self.veto {
                Err(EvalError::Unsupported("subject unavailable"))
            } else {
                Ok(Datum::new_string("runtime diagnostic"))
            }
        }
    }
    fn subject(ctx: &Session, kind: usize) -> Option<String> {
        ctx.events.borrow_mut().push(format!("subject:{kind}"));
        match kind {
            0 => None,
            1 => Some(String::new()),
            2 => Some("source expression".into()),
            3 => ctx
                .param_value(0)
                .ok()
                .and_then(|v| v.sql_bytes().ok())
                .map(|v| String::from_utf8_lossy(&v).into_owned()),
            _ => unreachable!(),
        }
    }
    fn outcome(ctx: &Session, body: impl FnOnce() -> Result<Datum, EvalError>) -> String {
        let value = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
            Ok(Ok(v)) => format!("VALUE:{}", crate::tikv::dynamic_leaf_tests::frame(&v)),
            Ok(Err(e)) => format!("ERROR:{e:?}"),
            Err(p) => format!(
                "PANIC:{:?}",
                p.downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| p.downcast_ref::<&str>().copied())
            ),
        };
        format!("{value}|{:?}|{}", ctx.events.borrow(), ctx.calls.get())
    }
    fn corpus(independent: bool) -> (usize, u64) {
        let mut fields = crate::tikv::eager_scalar_value_tests::types();
        for code in [
            tidb_datatype::FieldTypeCode::Bit,
            tidb_datatype::FieldTypeCode::Enum,
            tidb_datatype::FieldTypeCode::Set,
        ] {
            for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
                fields.push(FieldType::new(code).with_flags(flags));
            }
        }
        let mut values = crate::tikv::session_read_tests::values();
        values.extend([
            Datum::Real(1e100),
            Datum::Real(-1e100),
            Datum::Real(-0.0),
            Datum::Real(1.25),
            Datum::Real(f64::NAN),
        ]);
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for (fi, field) in fields.iter().enumerate() {
            for (vi, value) in values.iter().enumerate() {
                for target in [EvalType::Int, EvalType::Real, EvalType::Decimal] {
                    for vectorized in [false, true] {
                        for level in [
                            crate::ErrorLevel::Error,
                            crate::ErrorLevel::Warn,
                            crate::ErrorLevel::Ignore,
                        ] {
                            for veto in [false, true] {
                                for kind in 0..4 {
                                    let old = Session {
                                        level,
                                        veto,
                                        events: RefCell::new(vec![]),
                                        calls: Cell::new(0),
                                    };
                                    let want = outcome(&old, || {
                                        cast_numeric_argument_value(
                                            field,
                                            value.clone(),
                                            target,
                                            &old,
                                            vectorized,
                                            || subject(&old, kind),
                                        )
                                    });
                                    let label = format!(
                                        "{fi}|{vi}|{target:?}|{vectorized}|{level:?}|{veto}|{kind}"
                                    );
                                    if independent {
                                        let actual = Session {
                                            level,
                                            veto,
                                            events: RefCell::new(vec![]),
                                            calls: Cell::new(0),
                                        };
                                        let got = outcome(&actual, || {
                                            match numeric_argument_value::prepare(
                                                field,
                                                value.clone(),
                                                target,
                                                &actual,
                                                vectorized,
                                            )? {
                                                numeric_argument_value::Step::Ready(v) => Ok(v),
                                                numeric_argument_value::Step::Subject(pending) => {
                                                    let subject = subject(&actual, kind);
                                                    pending.finish(
                                                        field,
                                                        subject.as_deref(),
                                                        &actual,
                                                    )
                                                }
                                            }
                                        });
                                        assert_eq!(got, want, "shared staged conversion {label}");
                                    }
                                    for b in format!("{label}|{want}").bytes() {
                                        hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                                    }
                                    count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (58752, 0xEA48CF6E3E46CF89));
        (count, hash)
    }
    fn isolated(name: &str) -> bool {
        const KEY: &str = "TIDB_NUMERIC_STAGED_PROOF";
        if std::env::var(KEY).as_deref() == Ok(name) {
            return false;
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("scalar_function::numeric_argument_tests::staged_values::{name}"),
                "--nocapture",
            ])
            .env(KEY, name)
            .env("TZ", "UTC")
            .output()
            .unwrap();
        eprint!("{}", String::from_utf8_lossy(&result.stderr));
        eprintln!("{}", String::from_utf8_lossy(&result.stdout));
        assert!(result.status.success());
        true
    }
    #[test]
    fn numeric_argument_original_staged_values() {
        if isolated("numeric_argument_original_staged_values") {
            return;
        }
        let (count, hash) = corpus(false);
        eprintln!("NUMERIC ARGUMENT ORIGINAL STAGED {count} {hash:016X}");
    }
    #[test]
    fn numeric_argument_shared_staged_values_match_frozen() {
        if isolated("numeric_argument_shared_staged_values_match_frozen") {
            return;
        }
        let (count, hash) = corpus(true);
        eprintln!("NUMERIC ARGUMENT SHARED STAGED {count} {hash:016X}");
    }
    fn saved_facts_corpus(independent: bool) -> (usize, u64) {
        use tidb_datatype::{BinaryLiteral, Decimal, FieldTypeCode};
        let mut fields = crate::tikv::eager_scalar_value_tests::types()
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Set] {
            for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
                fields.push(Some(FieldType::new(code).with_flags(flags)));
            }
        }
        fields.push(None);
        let saved = [
            Datum::BinaryLiteral(BinaryLiteral::from(vec![255, 1])),
            Datum::Bit(BinaryLiteral::from(vec![255, 1])),
            Datum::Null,
            Datum::Int(7),
            Datum::Decimal(Decimal::from_literal("12.50")),
        ];
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for (fi, field) in fields.iter().enumerate() {
            for (si, saved) in saved.iter().enumerate() {
                for (vi, value) in crate::tikv::session_read_tests::values()
                    .into_iter()
                    .enumerate()
                {
                    for name in ["plus", "minus", "mul", "div", "intdiv", "mod"] {
                        for code in [
                            FieldTypeCode::LongLong,
                            FieldTypeCode::Double,
                            FieldTypeCode::NewDecimal,
                        ] {
                            for float in [false, true] {
                                for decimal in [None, Some(Decimal::from_literal("123.450"))] {
                                    for profile in [0, 2] {
                                        let make = || {
                                            let mut c = crate::constant::Constant::new(
                                                saved.clone(),
                                                FieldType::new(FieldTypeCode::LongLong),
                                            );
                                            c.ret_type = field.clone();
                                            c.param_marker =
                                                Some(crate::constant::ParamMarker { order: 0 });
                                            let expression = Expression::Constant(c);
                                            ScalarFunction::new(
                                                tidb_ast::CiString::new(name),
                                                FieldType::new(code),
                                                vec![
                                                    expression,
                                                    Expression::Constant(
                                                        crate::constant::Constant::new(
                                                            Datum::Int(1),
                                                            FieldType::new(FieldTypeCode::LongLong),
                                                        ),
                                                    ),
                                                ],
                                            )
                                        };
                                        let function = make();
                                        let expression = &function.args[0];
                                        let ctx = crate::tikv::typed_dynamic_tests::Context::new(
                                            profile,
                                            vec![value.clone()],
                                        );
                                        let result = match std::panic::catch_unwind(
                                            std::panic::AssertUnwindSafe(|| {
                                                let sources = [
                                                    EvalType::Int,
                                                    EvalType::Real,
                                                    EvalType::Decimal,
                                                ]
                                                .map(|t| numeric_argument_source(expression, t));
                                                let binary = is_numeric_binary_literal(expression);
                                                let text = numeric_argument_text(
                                                    &function,
                                                    expression,
                                                    float,
                                                    &ctx,
                                                    decimal.as_ref(),
                                                );
                                                format!("{sources:?}|{binary}|{text:?}")
                                            }),
                                        ) {
                                            Ok(v) => v,
                                            Err(p) => format!(
                                                "PANIC:{:?}",
                                                p.downcast_ref::<String>()
                                                    .map(String::as_str)
                                                    .or_else(|| p.downcast_ref::<&str>().copied())
                                            ),
                                        };

                                        if independent {
                                            use crate::scalar_function::dynamic_diagnostic::{
                                                ArgumentDiagnostic, ArgumentSourceFacts,
                                                DiagnosticStep,
                                            };
                                            let candidate = make();
                                            let expression = &candidate.args[0];
                                            let actual =
                                                crate::tikv::typed_dynamic_tests::Context::new(
                                                    profile,
                                                    vec![value.clone()],
                                                );
                                            let got = match std::panic::catch_unwind(
                                                std::panic::AssertUnwindSafe(|| {
                                                    let facts =
                                                        ArgumentSourceFacts::capture(expression);
                                                    let recipe = ArgumentDiagnostic::survey(
                                                        &candidate, expression, float, 128, 65536,
                                                    )
                                                    .unwrap();
                                                    assert!(recipe.bytes() <= 65536);
                                                    let (plan, bindings) = recipe.freeze().unwrap();
                                                    let mut state = plan.state();
                                                    let mut step = plan
                                                        .step(&mut state, decimal.as_ref())
                                                        .unwrap();
                                                    let text = loop {
                                                        match step {
                                                            DiagnosticStep::Done(text) => {
                                                                break text
                                                            }
                                                            DiagnosticStep::NeedConstant(index) => {
                                                                let Expression::Constant(c) =
                                                                    bindings[index]
                                                                else {
                                                                    panic!("diagnostic demands actual Constant")
                                                                };
                                                                let value = c.eval_in(&actual).ok();
                                                                step = plan
                                                                    .resume(
                                                                        &mut state,
                                                                        index,
                                                                        value.as_ref(),
                                                                        decimal.as_ref(),
                                                                    )
                                                                    .unwrap();
                                                            }
                                                        }
                                                    };
                                                    let sources = [
                                                        EvalType::Int,
                                                        EvalType::Real,
                                                        EvalType::Decimal,
                                                    ]
                                                    .map(|t| facts.source(t));
                                                    let binary = facts.binary();
                                                    format!("{sources:?}|{binary}|{text:?}")
                                                }),
                                            ) {
                                                Ok(v) => v,
                                                Err(p) => format!(
                                                    "PANIC:{:?}",
                                                    p.downcast_ref::<String>()
                                                        .map(String::as_str)
                                                        .or_else(|| p
                                                            .downcast_ref::<&str>()
                                                            .copied())
                                                ),
                                            };
                                            assert_eq!(got,result,"argument diagnostic {fi}/{si}/{vi}/{name}/{code:?}/{float}/{decimal:?}/{profile}");
                                            assert_eq!(actual.snapshot(),ctx.snapshot(),"argument diagnostic effects {fi}/{si}/{vi}/{name}/{code:?}/{float}/{decimal:?}/{profile}");
                                        }
                                        for b in format!("{fi}|{si}|{vi}|{name}|{code:?}|{float}|{decimal:?}|{profile}|{result}|{}",ctx.snapshot()).bytes() {hash=(hash^u64::from(b)).wrapping_mul(0x100000001b3);}
                                        count += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (246240, 0xD589F55424DB5FF5));
        (count, hash)
    }
    #[test]
    fn numeric_argument_original_saved_binary_facts_and_text() {
        if isolated("numeric_argument_original_saved_binary_facts_and_text") {
            return;
        }
        let (count, hash) = saved_facts_corpus(false);
        eprintln!("NUMERIC ARGUMENT ORIGINAL SAVED FACTS {count} {hash:016X}");
    }
    #[test]
    fn numeric_argument_independent_argument_diagnostics_before_retirement() {
        if isolated("numeric_argument_independent_argument_diagnostics_before_retirement") {
            return;
        }
        let (count, hash) = saved_facts_corpus(true);
        eprintln!("NUMERIC ARGUMENT INDEPENDENT ARGUMENT DIAGNOSTICS {count} {hash:016X}");
    }
    fn diagnostic_text_outcome(body: impl FnOnce() -> Option<String>) -> String {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
            Ok(text) => format!("{text:?}"),
            Err(p) => format!(
                "PANIC:{:?}",
                p.downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| p.downcast_ref::<&str>().copied())
            ),
        }
    }
    fn structure_corpus(independent: bool) -> (usize, u64) {
        use crate::scalar_function::dynamic_diagnostic::{ArgumentDiagnostic, DiagnosticStep};
        use crate::{
            column::{Column, CorrelatedColumn},
            constant::Constant,
        };
        use tidb_datatype::{BinaryLiteral, Decimal, FieldTypeCode};
        let mut fields = crate::tikv::eager_scalar_value_tests::types()
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Set] {
            for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
                fields.push(Some(FieldType::new(code).with_flags(flags)));
            }
        }
        fields.push(None);
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for (fi, field) in fields.iter().enumerate() {
            for form in 0..6 {
                for name in ["plus", "minus", "mul", "div", "intdiv", "mod"] {
                    for code in [
                        FieldTypeCode::LongLong,
                        FieldTypeCode::Double,
                        FieldTypeCode::NewDecimal,
                    ] {
                        for float in [false, true] {
                            for (di,decimal) in [None,Some(Decimal::from_literal("123.450")),Some(Decimal::from_literal("123456789012345678901234567890123456789012345678901234567890123456789")),Some(Decimal::from_raw_parts(false,vec![b'1'],100000,100000))].into_iter().enumerate() {
                                let make=|| {
                                    let lit=||Expression::Constant(Constant::new(Datum::Int(7),FieldType::new(FieldTypeCode::LongLong)));
                                    let plus=||Expression::ScalarFunction(ScalarFunction::new(tidb_ast::CiString::new("plus"),FieldType::new(FieldTypeCode::Double),vec![lit(),lit()]));
                                    let mut expression=match form {
                                        0=>Expression::Constant(Constant::new(Datum::BinaryLiteral(BinaryLiteral::from(vec![255,1])),FieldType::new(FieldTypeCode::LongLong))),
                                        1=>{let mut c=Column::new(1,FieldType::new(FieldTypeCode::LongLong));c.orig_name="t.source".into();Expression::Column(c)},
                                        2=>{let mut c=Column::new(1,FieldType::new(FieldTypeCode::LongLong));c.is_hidden=true;c.virtual_expr=Some(Box::new(plus()));Expression::Column(c)},
                                        3=>plus(),
                                        4=>Expression::ScalarFunction(ScalarFunction::new(tidb_ast::CiString::new("cast_json"),FieldType::new(FieldTypeCode::Json),vec![lit()])),
                                        5=>{let mut c=Column::new(1,FieldType::new(FieldTypeCode::LongLong));c.orig_name="t.outer".into();Expression::CorrelatedColumn(CorrelatedColumn {column:c,data:None})},
                                        _=>unreachable!(),
                                    };
                                    match &mut expression {Expression::Constant(c)=>c.ret_type=field.clone(),Expression::Column(c)=>c.ret_type=field.clone(),Expression::CorrelatedColumn(c)=>c.column.ret_type=field.clone(),Expression::ScalarFunction(f)=>f.ret_type=field.clone()};
                                    ScalarFunction::new(tidb_ast::CiString::new(name),FieldType::new(code),vec![expression,lit()])
                                };
                                let function=make();
                                let ctx=crate::tikv::typed_dynamic_tests::Context::new(0,vec![Datum::Int(99)]);
                                let want=diagnostic_text_outcome(||numeric_argument_text(&function,&function.args[0],float,&ctx,decimal.as_ref()));
                                if independent {
                                    let candidate=make();let actual=crate::tikv::typed_dynamic_tests::Context::new(0,vec![Datum::Int(99)]);
                                    let recipe=ArgumentDiagnostic::survey(&candidate,&candidate.args[0],float,128,65536).unwrap();
                                    let (plan,bindings)=recipe.freeze().unwrap();assert!(bindings.is_empty());
                                    let mut state=plan.state();
                                    let got=diagnostic_text_outcome(|| {let DiagnosticStep::Done(text)=plan.step(&mut state,decimal.as_ref()).unwrap() else {panic!("static source facts cannot demand runtime SQL")};text});
                                    assert_eq!(got,want,"structure {fi}/{form}/{name}/{code:?}/{float}/{di}");
                                    assert_eq!(actual.snapshot(),ctx.snapshot());
                                }
                                for b in format!("{fi}|{form}|{name}|{code:?}|{float}|{di}|{want:?}|{}",ctx.snapshot()).bytes() {hash=(hash^u64::from(b)).wrapping_mul(0x100000001b3);}
                                count+=1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (15552, 0x1765DBC05E4A2D95));
        (count, hash)
    }
    #[test]
    fn numeric_argument_original_structure_and_decimal_refinement() {
        if isolated("numeric_argument_original_structure_and_decimal_refinement") {
            return;
        }
        let (count, hash) = structure_corpus(false);
        eprintln!("NUMERIC ARGUMENT ORIGINAL STRUCTURE {count} {hash:016X}");
    }
    #[test]
    fn numeric_argument_independent_structure_before_retirement() {
        if isolated("numeric_argument_independent_structure_before_retirement") {
            return;
        }
        let (count, hash) = structure_corpus(true);
        eprintln!("NUMERIC ARGUMENT INDEPENDENT STRUCTURE {count} {hash:016X}");
    }
}
