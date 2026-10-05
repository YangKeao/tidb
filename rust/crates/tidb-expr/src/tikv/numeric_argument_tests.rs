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
