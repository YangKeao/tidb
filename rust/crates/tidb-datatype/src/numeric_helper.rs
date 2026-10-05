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

/// Best-effort integer parsing failure from `types.strToInt`.
pub use tidb_query_datatype::codec::native_integer_convert::NativeStringToIntError as StringToIntError;

/// Overflow returned while narrowing a floating SQL value.
pub use tidb_query_datatype::codec::native_float_convert::NativeFloatOverflow as FloatOverflow;

/// Rounds to the nearest even integer, matching Go `math.RoundToEven`.
pub fn round_float(value: f64) -> f64 {
    tidb_query_datatype::codec::native_float_convert::native_round_float(value)
}

/// Rounds `value` to `decimal` decimal places.
pub fn round(value: f64, decimal: i32) -> f64 {
    tidb_query_datatype::codec::native_float_convert::native_round(value, decimal)
}

/// Truncates `value` to `decimal` decimal places.
pub fn truncate(value: f64, decimal: i32) -> f64 {
    tidb_query_datatype::codec::native_float_convert::native_truncate(value, decimal)
}

/// Returns the largest magnitude admitted by a `(flen, decimal)` float.
pub fn get_max_float(flen: i32, decimal: i32) -> f64 {
    tidb_query_datatype::codec::native_float_convert::native_get_max_float(flen, decimal)
}

/// Rounds and clamps a float to a MySQL `(flen, decimal)` domain.
pub fn truncate_float(value: f64, flen: i32, decimal: i32) -> Result<f64, (f64, FloatOverflow)> {
    tidb_query_datatype::codec::native_float_convert::native_truncate_float(value, flen, decimal)
}

/// Truncates and renders without an exponent, matching Go format `'f', -1`.
pub fn truncate_float_to_string(value: f64, decimal: i32) -> String {
    tidb_query_datatype::codec::native_float_convert::native_truncate_float_to_string(
        value, decimal,
    )
}

/// Parses a signed integer in TiDB's best-effort mode.
pub fn string_to_int(value: &str) -> Result<i64, (i64, StringToIntError)> {
    tidb_query_datatype::codec::native_integer_convert::native_string_to_int(value)
}

/// Converts display length to decimal precision.
pub const fn decimal_length_to_precision(length: i32, scale: i32, unsigned: bool) -> i32 {
    tidb_query_datatype::codec::native_decimal_convert::native_decimal_length_to_precision(
        length, scale, unsigned,
    )
}

/// Converts decimal precision to display length without truncation.
pub const fn precision_to_length_no_truncation(length: i32, scale: i32, unsigned: bool) -> i32 {
    tidb_query_datatype::codec::native_decimal_convert::native_precision_to_length_no_truncation(
        length, scale, unsigned,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_to_int() {
        for (input, output, error) in [
            ("9223372036854775806", 9_223_372_036_854_775_806, None),
            ("9223372036854775807", i64::MAX, None),
            (
                "9223372036854775808",
                i64::MAX,
                Some(StringToIntError::BadNumber),
            ),
            ("-9223372036854775807", -9_223_372_036_854_775_807, None),
            ("-9223372036854775808", i64::MIN, None),
            (
                "-9223372036854775809",
                i64::MIN,
                Some(StringToIntError::BadNumber),
            ),
        ] {
            match (string_to_int(input), error) {
                (Ok(actual), None) => assert_eq!(actual, output),
                (Err((actual, actual_error)), Some(expected_error)) => {
                    assert_eq!(actual, output);
                    assert_eq!(actual_error, expected_error);
                }
                (actual, expected) => panic!("{input}: {actual:?} != {expected:?}"),
            }
        }
    }

    #[test]
    fn test_round_float() {
        for (input, expected) in [
            (2.5, 2.0),
            (1.5, 2.0),
            (0.5, 0.0),
            (0.499_999_999_999_999_97, 0.0),
            (0.0, 0.0),
            (-0.499_999_999_999_999_97, -0.0),
            (-0.5, -0.0),
            (-2.5, -2.0),
            (-1.5, -2.0),
        ] {
            assert_eq!(round_float(input), expected);
        }
    }

    #[test]
    fn test_round() {
        for (input, decimal, expected) in [
            (-1.23, 0, -1.0),
            (-1.58, 0, -2.0),
            (1.58, 0, 2.0),
            (1.298, 1, 1.3),
            (1.298, 0, 1.0),
            (23.298, -1, 20.0),
        ] {
            assert_eq!(round(input, decimal), expected);
        }
    }

    #[test]
    fn test_truncate() {
        for (input, decimal, expected) in [
            (123.45, 0, 123.0),
            (123.45, 1, 123.4),
            (123.45, 2, 123.45),
            (123.45, 3, 123.450),
            (123.45, -400, 0.0),
            (123.45, 400, 123.45),
        ] {
            assert_eq!(truncate(input, decimal), expected);
        }
    }

    #[test]
    fn test_max_float() {
        assert_eq!(get_max_float(3, 2), 9.99);
        assert_eq!(get_max_float(5, 2), 999.99);
        assert_eq!(get_max_float(10, 1), 999_999_999.9);
        assert_eq!(get_max_float(5, 5), 0.99999);
    }

    #[test]
    fn test_truncate_float() {
        assert_eq!(truncate_float(100.114, 10, 2), Ok(100.11));
        assert_eq!(truncate_float(100.115, 10, 2), Ok(100.12));
        assert_eq!(truncate_float(100.1156, 10, 3), Ok(100.116));
        assert_eq!(truncate_float(100.1156, 3, 1), Err((99.9, FloatOverflow)));
        assert_eq!(truncate_float(1.36, 10, 2), Ok(1.36));
    }

    #[test]
    fn test_truncate_float_to_string() {
        for (input, decimal, expected) in [
            (12.13, -1, "10"),
            (13.15, 0, "13"),
            (0.0, 2, "0"),
            (0.001, 2, "0"),
            (0.539, 2, "0.53"),
            (0.9951, 2, "0.99"),
            (1.0, 2, "1"),
            (-0.456, 2, "-0.45"),
        ] {
            assert_eq!(truncate_float_to_string(input, decimal), expected);
        }
    }

    #[test]
    fn shared_numeric_text_keeps_parser_precedence_float_spelling_and_const_length_quirks() {
        use StringToIntError::{BadNumber, Truncated};
        for (input, expected) in [
            ("\u{2003}\u{00a0}+42\u{202f}", Ok(42)),
            ("9223372036854775807", Ok(i64::MAX)),
            ("-9223372036854775808", Ok(i64::MIN)),
            ("+", Err((0, Truncated))),
            ("-", Err((0, Truncated))),
            ("１２", Err((0, Truncated))),
            ("\u{200b}12", Err((0, Truncated))),
            ("12\0tail", Err((12, Truncated))),
            ("-0x", Err((0, Truncated))),
            ("-9223372036854775808x", Err((i64::MIN, Truncated))),
            ("9223372036854775808\0", Err((i64::MAX, BadNumber))),
            ("18446744073709551615x", Err((i64::MAX, BadNumber))),
            ("18446744073709551616x", Err((i64::MAX, BadNumber))),
            ("-18446744073709551616x", Err((i64::MIN, BadNumber))),
            ("12x18446744073709551616", Err((12, Truncated))),
        ] {
            assert_eq!(string_to_int(input), expected, "{input:?}");
        }
        assert_eq!(Truncated.to_string(), "truncated");
        assert_eq!(format!("{Truncated:?}"), "Truncated");
        let error: &dyn std::error::Error = &BadNumber;
        assert_eq!(error.to_string(), "bad number");
        assert!(error.source().is_none());
        assert_eq!(format!("{BadNumber:?}"), "BadNumber");
        for (input, decimal, expected) in [
            (f64::NAN, 0, "NaN"),
            (f64::INFINITY, i32::MIN, "inf"),
            (f64::NEG_INFINITY, i32::MAX, "-inf"),
            (-0.0, 0, "-0"),
            (-0.0, i32::MAX, "-0"),
            (-0.0, i32::MIN, "0"),
            (1.25, i32::MAX, "1.25"),
            (-1.25, i32::MIN, "0"),
        ] {
            assert_eq!(truncate_float_to_string(input, decimal), expected);
        }
        assert_eq!(
            truncate_float_to_string(f64::MAX, 400),
            format!("17976931348623157{}", "0".repeat(292)),
        );
        assert_eq!(
            truncate_float_to_string(f64::from_bits(1), 400),
            format!("0.{}5", "0".repeat(323)),
        );
        const QUIRKS: [(i32, i32); 5] = [
            (
                decimal_length_to_precision(0, 0, false),
                precision_to_length_no_truncation(0, 0, false),
            ),
            (
                decimal_length_to_precision(0, 0, true),
                precision_to_length_no_truncation(0, 0, true),
            ),
            (
                decimal_length_to_precision(0, 1, false),
                precision_to_length_no_truncation(0, 1, false),
            ),
            (
                decimal_length_to_precision(-1, 1, true),
                precision_to_length_no_truncation(-1, 1, true),
            ),
            (
                decimal_length_to_precision(i32::MAX, 0, false),
                precision_to_length_no_truncation(i32::MIN, 0, false),
            ),
        ];
        assert_eq!(
            QUIRKS,
            [(0, 0), (-1, 1), (-1, 2), (-3, 1), (i32::MAX - 1, i32::MIN)]
        );
    }
}
