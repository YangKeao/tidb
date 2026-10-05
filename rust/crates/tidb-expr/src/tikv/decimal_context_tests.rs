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

#[test]
fn decimal_context_keeps_policy_terror_identity_raw_sources_and_value_only_differences() {
    use crate::{context::NoColumns, Datum};
    use std::cell::RefCell;
    use tidb_ast::CastType;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, ConversionContext, ConversionFlags, ConversionLocation,
        ConversionWarningAppender, DatumValueError, ScalarConversionEvent, ERR_OVERFLOW,
        ERR_TRUNCATED, ERR_TRUNCATED_WRONG_VALUE, JSON_TYPE_CODE_LITERAL,
    };
    use tidb_error::terror::{TerrorClass, TerrorError};

    #[derive(Default)]
    struct Warnings(RefCell<Vec<TerrorError>>);
    impl ConversionWarningAppender for Warnings {
        fn append_conversion_warning(&self, error: TerrorError) {
            self.0.borrow_mut().push(error);
        }
    }
    let warnings = Warnings::default();
    let strict_flags = ConversionFlags::default()
        .with_ignore_truncate_err(false)
        .with_truncate_as_warning(false);
    let literal = BinaryLiteral::from(vec![1; 9]);
    assert_eq!(literal.to_string(), "0x010101010101010101");
    assert_eq!(BinaryLiteral::ZERO.to_string(), "");
    for (mode, flags) in [
        ("strict", strict_flags),
        ("warn", strict_flags.with_truncate_as_warning(true)),
        ("ignore", strict_flags.with_ignore_truncate_err(true)),
    ] {
        let context = ConversionContext::new(flags, ConversionLocation::UTC, &warnings);
        // MyDecimal keeps a numeric prefix with generic Truncated; only a
        // no-digit input enters the old mapper's TruncatedWrongValue renderer.
        for (value, expected_value, expected_error) in [
            (Datum::new_string(" \t-12x"), "-12", ERR_TRUNCATED.clone()),
            (
                Datum::new_string(vec![b'1', b'2', 0xff]),
                "12",
                ERR_TRUNCATED.clone(),
            ),
            (
                Datum::new_string(" \t-x"),
                "0",
                ERR_TRUNCATED_WRONG_VALUE.generate("Truncated incorrect DECIMAL value: 'x'"),
            ),
            (
                Datum::new_string(vec![0xff]),
                "0",
                ERR_TRUNCATED_WRONG_VALUE.generate("Truncated incorrect DECIMAL value: '�'"),
            ),
            (
                Datum::Bit(literal.clone()),
                "18446744073709551615",
                ERR_TRUNCATED_WRONG_VALUE
                    .generate("Truncated incorrect BINARY value: '0x010101010101010101'"),
            ),
            (
                Datum::Json(BinaryJSON::parse("{}").unwrap()),
                "0",
                ERR_TRUNCATED_WRONG_VALUE.generate("Truncated incorrect DECIMAL value: '{}'"),
            ),
        ] {
            warnings.0.borrow_mut().clear();
            let (decimal, error) = value.to_decimal_with_context(&context).unwrap();
            assert_eq!(decimal.to_string(), expected_value);
            let recorded = warnings.0.borrow();
            let diagnostic = match mode {
                "strict" => {
                    assert!(recorded.is_empty());
                    assert!(error.is_some());
                    error.as_ref()
                }
                "warn" => {
                    assert!(error.is_none());
                    assert_eq!(recorded.len(), 1);
                    recorded.first()
                }
                _ => {
                    assert!(error.is_none());
                    assert!(recorded.is_empty());
                    None
                }
            };
            if let Some(error) = diagnostic {
                assert_eq!(error.identity(), expected_error.identity());
                assert_eq!(error.class(), TerrorClass::Types);
                assert_eq!(error.message(), expected_error.message());
                assert_eq!(
                    error.to_sql_error().code,
                    expected_error.to_sql_error().code
                );
                assert_eq!(error.to_sql_error().message, expected_error.message());
            }
        }
        // REAL/FLOAT32 diagnostics bypass handle_truncate even in ignore mode.
        warnings.0.borrow_mut().clear();
        let (_, error) = Datum::Real(1e100)
            .to_decimal_with_context(&context)
            .unwrap();
        let error = error.unwrap();
        assert_eq!(error.identity(), ERR_OVERFLOW.identity());
        assert_eq!(error.message(), ERR_OVERFLOW.message());
        let (_, error) = Datum::Float32(f64::MAX)
            .to_decimal_with_context(&context)
            .unwrap();
        let error = error.unwrap();
        assert_eq!(error.identity(), ERR_TRUNCATED_WRONG_VALUE.identity());
        assert_eq!(error.message(), "Truncated incorrect DECIMAL value: 'Inf'");
        assert!(warnings.0.borrow().is_empty());
        for value in [
            Datum::new_string("12"),
            Datum::Bit(BinaryLiteral::from(vec![12])),
            Datum::Json(BinaryJSON::parse("12").unwrap()),
        ] {
            let (decimal, error) = value.to_decimal_with_context(&context).unwrap();
            assert_eq!(decimal.to_string(), "12");
            assert!(error.is_none());
        }
        assert!(warnings.0.borrow().is_empty());
        let bytes = Datum::new_bytes(b"12x".to_vec());
        assert_eq!(bytes.to_decimal().unwrap().value.to_string(), "12");
        assert!(matches!(bytes.to_decimal_with_context(&context),
            Err(DatumValueError::Unsupported(kind, "decimal")) if kind == bytes.kind()));
    }
    let plain = Datum::Bit(literal.clone()).to_decimal().unwrap();
    assert_eq!(plain.value.to_string(), "18446744073709551615");
    assert_eq!(plain.event, Some(ScalarConversionEvent::Truncated));
    assert_eq!(
        crate::cast::eval_cast(&CastType::Unsigned, Datum::Bit(literal), None, &NoColumns),
        Ok(Datum::UInt(u64::MAX)),
    );
    let empty_literal = Datum::Json(BinaryJSON::from_encoded_parts(
        JSON_TYPE_CODE_LITERAL,
        vec![],
    ));
    assert_eq!(empty_literal.to_decimal().unwrap().value.to_string(), "0");
    let strict = ConversionContext::new(strict_flags, ConversionLocation::UTC, &warnings);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        empty_literal.to_decimal_with_context(&strict)
    }))
    .is_err());
}
