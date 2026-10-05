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

//! The native 40-byte Go MyDecimal storage carrier. Algorithms live in the
//! shared datatype owner; this facade preserves the original private layout,
//! derived traits and value-plus-error API, including partial mutation on unwind.

use crate::decimal::DecimalCodecError;
use smallvec::SmallVec;
use tidb_query_datatype::codec::mysql::NativeMyDecimal as SharedMyDecimal;
pub use tidb_query_datatype::codec::mysql::{
    NativeMyDecimalError as DecimalError, NativeMyDecimalRoundMode as RoundMode,
    NATIVE_MYDECIMAL_MAX_WORD_BUF_LEN as MAX_WORD_BUF_LEN,
    NATIVE_MYDECIMAL_STRUCT_SIZE as MYDECIMAL_STRUCT_SIZE,
};

#[cfg(test)]
const DIGITS_PER_WORD: i32 = 9;

/// Go `MyDecimal`: sign + digit counts + base-1e9 word buffer, in Go's exact
/// field order and layout (40 bytes).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MyDecimal {
    /// Go `digitsInt`: decimal digits before the point.
    digits_int: i8,
    /// Go `digitsFrac`: decimal digits after the point.
    digits_frac: i8,
    /// Go `resultFrac`: result fraction digits.
    result_frac: i8,
    /// Go `negative`.
    negative: bool,
    /// Go `wordBuf`: base-1e9 words (`0 <= word < wordBase`).
    word_buf: [i32; MAX_WORD_BUF_LEN],
}

const _: () = assert!(std::mem::size_of::<MyDecimal>() == MYDECIMAL_STRUCT_SIZE);

#[cfg(test)]
fn digits_to_words(digits: i32) -> i32 {
    tidb_query_datatype::codec::mysql::native_mydecimal_digits_to_words(digits)
}

pub fn format_float_g_shortest(f: f64) -> String {
    tidb_query_datatype::codec::mysql::Decimal::native_format_float_g_shortest(f)
}

// Write the complete original storage back on both return and unwinding. The
// projection is infallible and does not validate malformed source cell fields.
struct SharedWriteBack<'a> {
    target: &'a mut MyDecimal,
    value: SharedMyDecimal,
}

impl Drop for SharedWriteBack<'_> {
    fn drop(&mut self) {
        *self.target = MyDecimal::from_shared(self.value);
    }
}

impl MyDecimal {
    fn as_shared(&self) -> SharedMyDecimal {
        SharedMyDecimal::from_raw_parts((
            self.digits_int,
            self.digits_frac,
            self.result_frac,
            self.negative,
            self.word_buf,
        ))
    }

    fn from_shared(value: SharedMyDecimal) -> Self {
        let (digits_int, digits_frac, result_frac, negative, word_buf) = value.raw_parts();
        Self {
            digits_int,
            digits_frac,
            result_frac,
            negative,
            word_buf,
        }
    }

    fn with_shared_mut<R>(&mut self, operation: impl FnOnce(&mut SharedMyDecimal) -> R) -> R {
        let value = self.as_shared();
        let mut guard = SharedWriteBack {
            target: self,
            value,
        };
        operation(&mut guard.value)
    }

    pub(crate) fn from_decimal_parts(
        negative: bool,
        coefficient: &str,
        storage_scale: u32,
        result_frac: u32,
        minimum_integer_digit: bool,
    ) -> Result<MyDecimal, DecimalError> {
        SharedMyDecimal::from_decimal_parts(
            negative,
            coefficient,
            storage_scale,
            result_frac,
            minimum_integer_digit,
        )
        .map(Self::from_shared)
    }

    #[must_use]
    pub fn from_scaled_i128(
        value: i128,
        storage_scale: u32,
        result_frac: u32,
    ) -> Option<MyDecimal> {
        SharedMyDecimal::from_scaled_i128(value, storage_scale, result_frac).map(Self::from_shared)
    }

    #[cfg(test)]
    fn digit_bounds(&self) -> (i32, i32) {
        self.as_shared().digit_bounds()
    }

    pub fn round(
        &self,
        to: &mut MyDecimal,
        frac: i32,
        round_mode: RoundMode,
    ) -> Option<DecimalError> {
        let same = std::ptr::eq(self, to);
        debug_assert!(!same, "use round_in_place for the aliasing case");
        let source = self.as_shared();
        to.with_shared_mut(|value| source.round(value, frac, round_mode))
    }

    pub fn round_in_place(&mut self, frac: i32, round_mode: RoundMode) -> Option<DecimalError> {
        self.with_shared_mut(|value| value.round_in_place(frac, round_mode))
    }

    pub fn shift(&mut self, shift: i32) -> Option<DecimalError> {
        self.with_shared_mut(|value| value.shift(shift))
    }

    pub fn from_string(str: &[u8]) -> (MyDecimal, Option<DecimalError>) {
        let (value, error) = SharedMyDecimal::from_string(str);
        (Self::from_shared(value), error)
    }

    #[must_use]
    pub fn from_int(val: i64) -> MyDecimal {
        Self::from_shared(SharedMyDecimal::from_int(val))
    }

    #[must_use]
    pub fn from_uint(val: u64) -> MyDecimal {
        Self::from_shared(SharedMyDecimal::from_uint(val))
    }

    #[must_use]
    pub fn is_negative(&self) -> bool {
        self.as_shared().is_negative()
    }

    #[must_use]
    pub fn to_int(&self) -> (i64, Option<DecimalError>) {
        self.as_shared().to_int()
    }

    #[must_use]
    pub fn to_uint(&self) -> (u64, Option<DecimalError>) {
        self.as_shared().to_uint()
    }

    pub fn from_float64(f: f64) -> (MyDecimal, Option<DecimalError>) {
        let (value, error) = SharedMyDecimal::from_float64(f);
        (Self::from_shared(value), error)
    }

    #[must_use]
    pub fn to_float64(&self) -> (f64, Option<DecimalError>) {
        self.as_shared().to_float64()
    }

    #[must_use]
    pub fn compare(&self, other: &MyDecimal) -> std::cmp::Ordering {
        self.as_shared().compare(&other.as_shared())
    }

    #[must_use]
    pub fn digits_frac(&self) -> i8 {
        self.as_shared().digits_frac()
    }

    #[must_use]
    pub fn coefficient_i128(&self) -> Option<(i128, u32)> {
        self.as_shared().coefficient_i128()
    }

    #[must_use]
    pub fn result_frac(&self) -> i8 {
        self.as_shared().result_frac()
    }

    pub(crate) fn set_result_frac(&mut self, result_frac: i8) {
        self.with_shared_mut(|value| value.set_result_frac(result_frac));
    }

    pub fn to_hash_key(&self) -> Result<SmallVec<[u8; 64]>, DecimalCodecError> {
        self.as_shared().to_hash_key()
    }

    #[must_use]
    pub fn to_string_bytes(&self) -> Vec<u8> {
        self.as_shared().to_string_bytes()
    }

    pub fn append_to_string_bytes(&self, output: &mut Vec<u8>) {
        self.as_shared().append_to_string_bytes(output);
    }

    #[must_use]
    pub fn to_result_string_bytes(&self) -> Vec<u8> {
        self.as_shared().to_result_string_bytes()
    }

    pub fn append_result_string_bytes(&self, output: &mut Vec<u8>) {
        self.as_shared().append_result_string_bytes(output);
    }

    pub(crate) fn to_decimal_parts(self) -> (bool, SmallVec<[u8; 24]>, u32, u32) {
        self.as_shared().to_decimal_parts()
    }

    pub fn to_i128_scaled(&self) -> Option<(i128, u32)> {
        self.as_shared().to_i128_scaled()
    }

    pub fn i128_scaled_from_raw_bytes(bytes: &[u8]) -> Option<(i128, u32)> {
        SharedMyDecimal::i128_scaled_from_raw_bytes(bytes)
    }

    #[must_use]
    pub fn to_raw_bytes(&self) -> [u8; MYDECIMAL_STRUCT_SIZE] {
        self.as_shared().to_raw_bytes()
    }

    pub fn from_raw_bytes(bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> Result<MyDecimal, &'static str> {
        SharedMyDecimal::from_raw_bytes(bytes).map(Self::from_shared)
    }

    #[must_use]
    pub fn from_raw_bytes_like_go(bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> MyDecimal {
        Self::from_shared(SharedMyDecimal::from_raw_bytes_like_go(bytes))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn from_scaled_i128_matches_from_decimal_parts() {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut cases: Vec<(i128, u32, u32)> = vec![
            (0, 0, 0),
            (0, 2, 2),
            (5, 2, 2),
            (-5, 2, 0),
            (123_456_789_012_345_678, 2, 2),
            (999_999_999_999_999_999, 4, 2),
            (-1_000_000_000, 0, 0),
            (1_000_000_000_000_000_000_000_000, 9, 9),
            (i128::MAX, 30, 4),
            (i128::MIN + 1, 0, 0),
            // The 64-bit boundary of the machine-word path, at its widest
            // scale and just past it.
            (u64::MAX as i128, 2, 2),
            (u64::MAX as i128, 18, 18),
            (u64::MAX as i128, 19, 19),
            (u64::MAX as i128 + 1, 2, 2),
            (-(u64::MAX as i128), 18, 0),
            (999_999_999_999_999_999, 18, 18),
            (1_000_000_000_000_000_000, 18, 18),
            (1_000_000_000_000_000_000, 0, 0),
            (999_999_999, 9, 9),
            (1_000_000_000, 9, 9),
            (-1, 18, 18),
        ];
        for _ in 0..2000 {
            let bits = next();
            let magnitude = i128::from_ne_bytes(
                [next().to_ne_bytes(), next().to_ne_bytes()]
                    .concat()
                    .try_into()
                    .unwrap(),
            )
            .unsigned_abs()
                >> (bits % 120);
            let value = if bits & 1 == 0 {
                magnitude as i128
            } else {
                -(magnitude as i128)
            };
            let storage_scale = (bits >> 8) as u32 % 31;
            let result_frac = ((bits >> 16) as u32 % 31).min(storage_scale);
            cases.push((value, storage_scale, result_frac));
        }
        for (value, storage_scale, result_frac) in cases {
            let coefficient = value.unsigned_abs().to_string();
            let coefficient = format!(
                "{:0>width$}",
                coefficient,
                width = storage_scale as usize + 1
            );
            let expected = super::MyDecimal::from_decimal_parts(
                value < 0,
                &coefficient,
                storage_scale,
                result_frac,
                true,
            )
            .ok();
            assert_eq!(
                super::MyDecimal::from_scaled_i128(value, storage_scale, result_frac),
                expected,
                "{value} / {storage_scale} / {result_frac}"
            );
        }
    }

    use super::*;

    /// `coefficient_i128` agrees with the Decimal round-trip on common
    /// aggregate shapes, and refuses what i128 cannot hold.
    #[test]
    fn coefficient_i128_matches_decimal_round_trip() {
        let cases = [
            "0",
            "1",
            "-1",
            "123.45",
            "-123.45",
            "37734107.00",
            "0.0000012345",
            "99999999999999999999999999999999999999", // 38 digits: fits
        ];
        for text in cases {
            let (parsed, error) = MyDecimal::from_string(text.as_bytes());
            assert!(error.is_none(), "{text}");
            let (coefficient, scale) = parsed
                .coefficient_i128()
                .unwrap_or_else(|| panic!("{text} must fit"));
            let rebuilt = crate::decimal::Decimal::from_scaled_i128(coefficient, scale);
            assert_eq!(rebuilt.to_string(), text, "{text}");
        }
        // 39 digits cannot fit.
        let (huge, error) = MyDecimal::from_string(b"999999999999999999999999999999999999999");
        if error.is_none() {
            assert!(huge.coefficient_i128().is_none());
        }
    }

    /// Differential fixture: every expectation is `types.MyDecimal.FromString`
    /// output captured from the Go implementation in this repository
    /// (input, `String()`, error, `digitsInt`, `digitsFrac`, `negative`).
    #[test]
    fn from_string_matches_go() {
        type Case = (
            &'static str,
            &'static str,
            Option<DecimalError>,
            i8,
            i8,
            bool,
        );
        let cases: &[Case] = &[
            ("0", "0", None, 1, 0, false),
            ("1", "1", None, 1, 0, false),
            ("-1", "-1", None, 1, 0, true),
            ("12345", "12345", None, 5, 0, false),
            ("1.5", "1.5", None, 1, 1, false),
            ("-1.50", "-1.50", None, 1, 2, true),
            ("0.000001", "0.000001", None, 1, 6, false),
            (
                "123456789012345678901234567890",
                "123456789012345678901234567890",
                None,
                30,
                0,
                false,
            ),
            ("  42  ", "42", None, 2, 0, false),
            ("+3.14", "3.14", None, 1, 2, false),
            (".5", "0.5", None, 0, 1, false),
            ("5.", "5", None, 1, 0, false),
            ("1e3", "1000", None, 4, 0, false),
            ("1E-3", "0.001", None, 0, 3, false),
            ("1.5e10", "15000000000", None, 11, 0, false),
            ("-1.5e-10", "-0.00000000015", None, 0, 11, true),
            (
                "1e100",
                "999999999999999999999999999999999999999999999999999999999999999999999999999999999",
                Some(DecimalError::Overflow),
                81,
                0,
                false,
            ),
            ("1e-100", "0", Some(DecimalError::Truncated), 0, 0, false),
            (
                "1e1000000000000",
                "999999999999999999999999999999999999999999999999999999999999999999999999999999999",
                Some(DecimalError::Overflow),
                81,
                0,
                false,
            ),
            (
                "1e-1000000000000",
                "0",
                Some(DecimalError::Truncated),
                0,
                0,
                false,
            ),
            ("1.23e5", "123000", None, 6, 0, false),
            (
                "999999999999999999999999999999.9999999999999999999999999999999999",
                "999999999999999999999999999999.9999999999999999999999999999999999",
                None,
                30,
                34,
                false,
            ),
            (
                "abc",
                "0",
                Some(DecimalError::TruncatedWrongValue),
                0,
                0,
                false,
            ),
            (
                "",
                "0",
                Some(DecimalError::TruncatedWrongValue),
                0,
                0,
                false,
            ),
            (
                "   ",
                "0",
                Some(DecimalError::TruncatedWrongValue),
                0,
                0,
                false,
            ),
            ("1x", "1", Some(DecimalError::Truncated), 1, 0, false),
            ("1.2.3", "1.2", Some(DecimalError::Truncated), 1, 1, false),
            ("1e", "1", Some(DecimalError::Truncated), 1, 0, false),
            ("1e+", "1", Some(DecimalError::Truncated), 1, 0, false),
            ("0.0", "0.0", None, 1, 1, false),
            ("-0.0", "0.0", None, 1, 1, false),
            ("-0", "0", None, 1, 0, false),
            (
                "12345678901234567890.12345678901234567890",
                "12345678901234567890.12345678901234567890",
                None,
                20,
                20,
                false,
            ),
            ("1e9", "1000000000", None, 10, 0, false),
            ("1e-9", "0.000000001", None, 0, 9, false),
            ("123.456e-2", "1.23456", None, 1, 5, false),
            (
                "0.1e-80",
                "0.000000000000000000000000000000000000000000000000000000000000000000000000000000001",
                None,
                0,
                81,
                false,
            ),
            (
                "9e81",
                "999999999999999999999999999999999999999999999999999999999999999999999999999999999",
                Some(DecimalError::Overflow),
                81,
                0,
                false,
            ),
        ];
        for (input, text, want_err, digits_int, digits_frac, negative) in cases {
            let (d, err) = MyDecimal::from_string(input.as_bytes());
            assert_eq!(err, *want_err, "error for {input:?}");
            assert_eq!(
                String::from_utf8(d.to_string_bytes()).unwrap(),
                *text,
                "text for {input:?}"
            );
            assert_eq!(d.digits_int, *digits_int, "digits_int for {input:?}");
            assert_eq!(d.digits_frac, *digits_frac, "digits_frac for {input:?}");
            assert_eq!(d.negative, *negative, "negative for {input:?}");
            assert_eq!(d.result_frac, d.digits_frac, "result_frac for {input:?}");
        }
    }

    /// Go distinguishes a no-digit `ErrTruncatedWrongVal("DECIMAL", ...)`
    /// from the `ErrBadNumber` returned by an exponent that cannot be parsed.
    #[test]
    fn from_string_preserves_no_digit_error_identity() {
        for input in [
            b"abc".as_slice(),
            b"".as_slice(),
            b"-".as_slice(),
            b".".as_slice(),
        ] {
            let (value, error) = MyDecimal::from_string(input);
            assert_eq!(value.to_string_bytes(), b"0");
            assert_eq!(error, Some(DecimalError::TruncatedWrongValue));
        }

        let (_, error) = MyDecimal::from_string(b"1e18446744073709551620");
        assert_eq!(error, Some(DecimalError::BadNumber));
    }

    /// Go's `strings.TrimSpace` removes Unicode whitespace around the
    /// exponent and trailing suffix, while the input remains a byte string.
    #[test]
    fn from_string_trims_unicode_whitespace_like_go() {
        let (trailing, trailing_error) = MyDecimal::from_string("1\u{00a0}".as_bytes());
        assert_eq!(trailing.to_string_bytes(), b"1");
        assert_eq!(trailing_error, None);

        let (exponent, exponent_error) = MyDecimal::from_string("1e\u{00a0}5".as_bytes());
        assert_eq!(exponent.to_string_bytes(), b"100000");
        assert_eq!(exponent_error, None);
    }

    /// The word-native hash key is byte-identical to the digit-string
    /// `Decimal::to_hash_key`, and its Go contract holds: equal values with
    /// different written scales share a key, different values never do.
    #[test]
    fn to_hash_key_matches_the_decimal_hash_key() {
        let inputs = [
            "0",
            "-0",
            "0.000",
            "-0.0",
            "1",
            "1.0",
            "1.00",
            "0001.000",
            "-1",
            "-1.0",
            "0.1",
            "0.10",
            ".1",
            "-0.1",
            "0.000000001",
            "0.0000000010",
            "0.00000000012345",
            "123456789",
            "123456789.000000000",
            "1234567890",
            "1234567890.123456789",
            "1234567890.1234567890",
            "-1234567890.1234567890",
            "999999999.999999999",
            "1000000000.000000001",
            "1000000000.0000000010",
            "123987654321.123456789000",
            "-213123.000123000",
            "0.001230E-3",
            "12300E-5",
            "99999999999999999999999999999999999999999999999999999999999999999",
            "12345678901234567890123456789012345678901234567890.123456789012345678901234567890",
            "0.123456789012345678901234567890",
            "-0.123456789012345678901234567890",
            "1000.5",
            "1000.50",
            "1000.500000000000",
            "2500.25",
            "2500.250",
        ];
        let mut keys = Vec::new();
        for input in inputs {
            let (value, error) = MyDecimal::from_string(input.as_bytes());
            // The 80-digit input fills all nine words and truncates; the
            // clamped value still has to hash like its text.
            assert!(
                matches!(error, None | Some(DecimalError::Truncated)),
                "FromString({input}): {error:?}"
            );
            let native = value.to_hash_key().expect("legal shape");
            let text = String::from_utf8(value.to_string_bytes()).expect("ascii");
            let (reference, _) = crate::Decimal::from_literal(&text)
                .to_hash_key()
                .expect("legal shape");
            assert_eq!(native.as_slice(), reference.as_slice(), "{input} ({text})");
            keys.push((value, native));
        }
        for (left, left_key) in &keys {
            for (right, right_key) in &keys {
                let is_zero = |value: &MyDecimal| value.word_buf.iter().all(|word| *word == 0);
                let equal = left.compare(right) == std::cmp::Ordering::Equal
                    || (is_zero(left) && is_zero(right));
                assert_eq!(
                    left_key == right_key,
                    equal,
                    "{} vs {}",
                    String::from_utf8_lossy(&left.to_string_bytes()),
                    String::from_utf8_lossy(&right.to_string_bytes())
                );
            }
        }
    }

    /// Exact source rows from `pkg/types/mydecimal_test.go::TestRemoveTrailingZeros`.
    #[test]
    fn test_remove_trailing_zeros() {
        for (input, expected_fraction_digits) in [
            ("0", 0),
            ("0.0", 0),
            (".0", 0),
            (".00000000", 0),
            ("0.0000", 0),
            ("0000", 0),
            ("0000.0", 0),
            ("0000.000", 0),
            ("-0", 0),
            ("-0.0", 0),
            ("-.0", 0),
            ("-.00000000", 0),
            ("-0.0000", 0),
            ("-0000", 0),
            ("-0000.0", 0),
            ("-0000.000", 0),
            ("123123123", 0),
            ("213123.", 0),
            ("21312.000", 0),
            ("21321.123", 3),
            ("213.1230000", 3),
            ("213123.000123000", 6),
            ("-123123123", 0),
            ("-213123.", 0),
            ("-21312.000", 0),
            ("-21321.123", 3),
            ("-213.1230000", 3),
            ("-213123.000123000", 6),
            ("123E5", 0),
            ("12300E-5", 3),
            ("0.00100E1", 2),
            ("0.001230E-3", 8),
            ("123987654321.123456789000", 9),
            ("000000000123", 0),
            ("123456789.987654321", 9),
            ("999.999000", 3),
        ] {
            let (decimal, error) = MyDecimal::from_string(input.as_bytes());
            assert_eq!(error, None, "FromString({input})");

            let (_, end) = decimal.digit_bounds();
            let fraction_start = digits_to_words(i32::from(decimal.digits_int)) * DIGITS_PER_WORD;
            let fraction_digits = (end - fraction_start).max(0);
            assert_eq!(fraction_digits, expected_fraction_digits, "{input}");
        }
    }

    /// `from_string` and `from_int` must agree on integral text, and the
    /// parsed value must survive the raw 40-byte chunk round-trip.
    #[test]
    fn from_string_agrees_with_from_int_and_round_trips() {
        for value in [0i64, 1, -1, 12345, -987654321, i64::MAX, i64::MIN] {
            let text = value.to_string();
            let (parsed, err) = MyDecimal::from_string(text.as_bytes());
            assert_eq!(err, None, "{value} parses cleanly");
            assert_eq!(
                parsed.to_string_bytes(),
                MyDecimal::from_int(value).to_string_bytes(),
                "{value} matches from_int"
            );
            let restored = MyDecimal::from_raw_bytes(parsed.to_raw_bytes()).expect("valid bytes");
            assert_eq!(restored, parsed, "{value} survives the raw round-trip");
        }
    }

    #[test]
    fn scaled_i128_reads_packed_words_without_decimal_text() {
        for (input, coefficient, scale) in [
            ("0", 0_i128, 0_u32),
            ("0.01", 1, 2),
            ("-12.3400", -123_400, 4),
            ("1.5", 15, 1),
            ("0.123456", 123_456, 6),
            ("-7.12345678", -712_345_678, 8),
            ("123456789.987654321", 123_456_789_987_654_321, 9),
            ("42.1234567891", 421_234_567_891, 10),
            (
                "12345678901234567890.123456789012345678",
                12_345_678_901_234_567_890_123_456_789_012_345_678,
                18,
            ),
        ] {
            let (value, error) = MyDecimal::from_string(input.as_bytes());
            assert_eq!(error, None, "{input}");
            assert_eq!(
                value.to_i128_scaled(),
                Some((coefficient, scale)),
                "{input}"
            );
            assert_eq!(
                MyDecimal::i128_scaled_from_raw_bytes(&value.to_raw_bytes()),
                Some((coefficient, scale)),
                "raw chunk cell: {input}"
            );
        }

        let (too_wide, error) = MyDecimal::from_string(
            b"999999999999999999999999999999999999999999999999999999999999999999999999999999999",
        );
        assert_eq!(error, None);
        assert_eq!(too_wide.to_i128_scaled(), None);
        assert_eq!(
            MyDecimal::i128_scaled_from_raw_bytes(&too_wide.to_raw_bytes()),
            None
        );
    }

    #[test]
    fn layout_is_40_bytes() {
        assert_eq!(std::mem::size_of::<MyDecimal>(), 40);
    }

    #[test]
    fn from_int_to_string() {
        for (v, expect) in [
            (0i64, "0"),
            (1, "1"),
            (-1, "-1"),
            (42, "42"),
            (-12345, "-12345"),
            (1_000_000_000, "1000000000"),
            (i64::MAX, "9223372036854775807"),
            (i64::MIN, "-9223372036854775808"),
        ] {
            let d = MyDecimal::from_int(v);
            assert_eq!(
                String::from_utf8(d.to_string_bytes()).unwrap(),
                expect,
                "value {v}"
            );
        }
    }

    /// Source: `pkg/types/mydecimal_test.go::TestToString`.
    #[test]
    fn test_to_string() {
        for (input, expected) in [
            ("123.123", "123.123"),
            ("123.1230", "123.1230"),
            ("00123.123", "123.123"),
        ] {
            let (decimal, error) = MyDecimal::from_string(input.as_bytes());
            assert_eq!(error, None, "FromString({input})");
            assert_eq!(decimal.to_string_bytes(), expected.as_bytes(), "{input}");
        }
    }

    #[test]
    fn result_string_rounds_to_go_result_fraction() {
        let (mut decimal, error) = MyDecimal::from_string(b"1.235");
        assert_eq!(error, None);
        decimal.set_result_frac(2);
        assert_eq!(decimal.to_result_string_bytes(), b"1.24");
    }

    #[test]
    fn from_uint_to_string() {
        let d = MyDecimal::from_uint(u64::MAX);
        assert_eq!(
            String::from_utf8(d.to_string_bytes()).unwrap(),
            "18446744073709551615"
        );
    }

    #[test]
    fn raw_bytes_round_trip() {
        for v in [0i64, 7, -7, 123_456_789_012_345, i64::MIN] {
            let d = MyDecimal::from_int(v);
            let bytes = d.to_raw_bytes();
            let back = MyDecimal::from_raw_bytes(bytes).unwrap();
            assert_eq!(back, d, "value {v}");
            assert_eq!(back.to_string_bytes(), d.to_string_bytes());
        }
        // The negative flag byte is validated.
        let mut bytes = MyDecimal::from_int(1).to_raw_bytes();
        bytes[3] = 2;
        assert!(MyDecimal::from_raw_bytes(bytes).is_err());

        // Malformed wire cells cannot publish counts outside the fixed word
        // buffer or base-1e9 words that would panic later formatting.
        let mut bytes = MyDecimal::from_int(1).to_raw_bytes();
        bytes[0] = u8::MAX;
        assert!(MyDecimal::from_raw_bytes(bytes).is_err());
        let mut bytes = MyDecimal::from_int(1).to_raw_bytes();
        bytes[0] = 82;
        assert!(MyDecimal::from_raw_bytes(bytes).is_err());
        let mut bytes = MyDecimal::from_int(1).to_raw_bytes();
        bytes[4..8].copy_from_slice(&1_000_000_000_i32.to_ne_bytes());
        assert!(MyDecimal::from_raw_bytes(bytes).is_err());
    }

    #[test]
    fn negative_flag_sits_at_byte_3() {
        // Field order (and thus the chunk byte layout) matches Go.
        let bytes = MyDecimal::from_int(-5).to_raw_bytes();
        assert_eq!(bytes[3], 1);
        let bytes = MyDecimal::from_int(5).to_raw_bytes();
        assert_eq!(bytes[3], 0);
    }

    fn decimal(text: &str) -> MyDecimal {
        MyDecimal::from_string(text.as_bytes()).0
    }

    /// Differential fixture: `(*MyDecimal).ToInt/ToUint/ToFloat64` output
    /// captured from the Go implementation in this repository.
    #[test]
    fn to_int_uint_float_match_go() {
        type Case = (
            &'static str,
            i64,
            Option<DecimalError>,
            u64,
            Option<DecimalError>,
            f64,
        );
        let cases: &[Case] = &[
            ("0", 0, None, 0, None, 0.0),
            ("1", 1, None, 1, None, 1.0),
            ("-1", -1, None, 0, Some(DecimalError::Overflow), -1.0),
            (
                "1.1",
                1,
                Some(DecimalError::Truncated),
                1,
                Some(DecimalError::Truncated),
                1.1,
            ),
            (
                "-1.1",
                -1,
                Some(DecimalError::Truncated),
                0,
                Some(DecimalError::Overflow),
                -1.1,
            ),
            (
                "9223372036854775807",
                9_223_372_036_854_775_807,
                None,
                9_223_372_036_854_775_807,
                None,
                9.223_372_036_854_776e18,
            ),
            (
                "9223372036854775808",
                i64::MAX,
                Some(DecimalError::Overflow),
                9_223_372_036_854_775_808,
                None,
                9.223_372_036_854_776e18,
            ),
            (
                "-9223372036854775808",
                i64::MIN,
                None,
                0,
                Some(DecimalError::Overflow),
                -9.223_372_036_854_776e18,
            ),
            (
                "-9223372036854775809",
                i64::MIN,
                Some(DecimalError::Overflow),
                0,
                Some(DecimalError::Overflow),
                -9.223_372_036_854_776e18,
            ),
            (
                "18446744073709551615",
                i64::MAX,
                Some(DecimalError::Overflow),
                u64::MAX,
                None,
                1.844_674_407_370_955_2e19,
            ),
            (
                "18446744073709551616",
                i64::MAX,
                Some(DecimalError::Overflow),
                u64::MAX,
                Some(DecimalError::Overflow),
                1.844_674_407_370_955_2e19,
            ),
            (
                "123.456",
                123,
                Some(DecimalError::Truncated),
                123,
                Some(DecimalError::Truncated),
                123.456,
            ),
            (
                "-123.456",
                -123,
                Some(DecimalError::Truncated),
                0,
                Some(DecimalError::Overflow),
                -123.456,
            ),
            (
                "0.5",
                0,
                Some(DecimalError::Truncated),
                0,
                Some(DecimalError::Truncated),
                0.5,
            ),
            (
                "99999999999999999999999999999999",
                i64::MAX,
                Some(DecimalError::Overflow),
                u64::MAX,
                Some(DecimalError::Overflow),
                1e32,
            ),
            (
                "-99999999999999999999999999999999",
                i64::MIN,
                Some(DecimalError::Overflow),
                0,
                Some(DecimalError::Overflow),
                -1e32,
            ),
        ];
        for (input, int, int_err, uint, uint_err, float) in cases {
            let value = decimal(input);
            assert_eq!(value.to_int(), (*int, *int_err), "ToInt {input}");
            assert_eq!(value.to_uint(), (*uint, *uint_err), "ToUint {input}");
            assert_eq!(value.to_float64(), (*float, None), "ToFloat64 {input}");
        }
    }

    /// Differential fixture: `(*MyDecimal).Compare` output captured from the
    /// Go implementation in this repository.
    #[test]
    fn compare_matches_go() {
        use std::cmp::Ordering;
        for (left, right, expected) in [
            ("1", "1", Ordering::Equal),
            ("1", "2", Ordering::Less),
            ("2", "1", Ordering::Greater),
            ("-1", "-2", Ordering::Greater),
            ("-2", "-1", Ordering::Less),
            ("-1", "1", Ordering::Less),
            ("1", "-1", Ordering::Greater),
            ("0", "-0", Ordering::Equal),
            ("1.001", "1.0010", Ordering::Equal),
            ("1.001", "1.002", Ordering::Less),
            ("0.0000001", "0.00000011", Ordering::Less),
            ("123456789012345678", "123456789012345679", Ordering::Less),
            ("-123.456", "-123.4560", Ordering::Equal),
            ("0", "0.0", Ordering::Equal),
        ] {
            assert_eq!(
                decimal(left).compare(&decimal(right)),
                expected,
                "{left} vs {right}"
            );
        }
    }

    /// Exact source rows from `pkg/types/mydecimal_test.go::TestCompareMyDecimal`.
    #[test]
    fn test_compare_my_decimal() {
        use std::cmp::Ordering;

        for (left, right, expected) in [
            ("12", "13", Ordering::Less),
            ("13", "12", Ordering::Greater),
            ("-10", "10", Ordering::Less),
            ("10", "-10", Ordering::Greater),
            ("-12", "-13", Ordering::Greater),
            ("0", "12", Ordering::Less),
            ("-10", "0", Ordering::Less),
            ("4", "4", Ordering::Equal),
            ("-1.1", "-1.2", Ordering::Greater),
            ("1.2", "1.1", Ordering::Greater),
            ("1.1", "1.2", Ordering::Less),
        ] {
            let (left_decimal, left_error) = MyDecimal::from_string(left.as_bytes());
            let (right_decimal, right_error) = MyDecimal::from_string(right.as_bytes());
            assert_eq!(left_error, None, "FromString({left})");
            assert_eq!(right_error, None, "FromString({right})");
            assert_eq!(
                left_decimal.compare(&right_decimal),
                expected,
                "{left} vs {right}"
            );
        }
    }

    /// Differential fixture: `strconv.FormatFloat(f, 'g', -1, 64)` and the
    /// resulting `(*MyDecimal).FromFloat64(f).String()`, both captured from
    /// the Go implementation in this repository.
    #[test]
    #[allow(clippy::approx_constant, reason = "a Go fixture input, not pi")]
    fn from_float64_matches_go() {
        for (input, formatted, expected) in [
            (0.0, "0", "0"),
            (1.0, "1", "1"),
            (-1.0, "-1", "-1"),
            (0.1, "0.1", "0.1"),
            (1e-5, "1e-05", "0.00001"),
            (1e-4, "0.0001", "0.0001"),
            (123_456.0, "123456", "123456"),
            (1e6, "1e+06", "1000000"),
            (1.5e21, "1.5e+21", "1500000000000000000000"),
            (
                -3.141_592_653_589_79,
                "-3.14159265358979",
                "-3.14159265358979",
            ),
            (1e-20, "1e-20", "0.00000000000000000001"),
            (
                12_345_678_901_234_567_890.0,
                "1.2345678901234567e+19",
                "12345678901234567000",
            ),
        ] {
            assert_eq!(format_float_g_shortest(input), formatted, "{input}");
            let (value, err) = MyDecimal::from_float64(input);
            assert_eq!(err, None, "{input}");
            assert_eq!(
                String::from_utf8(value.to_string_bytes()).unwrap(),
                expected,
                "{input}"
            );
        }
    }
}

#[cfg(test)]
#[test]
fn shared_native_float_format_preserves_full_domain_and_exponent_edges() {
    for (input, expected) in [
        (0.0, "0"),
        (-0.0, "-0"),
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
        (f64::from_bits(0x7ff8000012345678), "NaN"),
        (f64::from_bits(0xfff8000012345678), "NaN"),
        (1e-5, "1e-05"),
        (-1e-5, "-1e-05"),
        (1e-4, "0.0001"),
        (100_000.0, "100000"),
        (1_000_000.0, "1e+06"),
        (1_234_500.0, "1.2345e+06"),
        (12.345, "12.345"),
        (f64::from_bits(1), "5e-324"),
        (-f64::from_bits(1), "-5e-324"),
        (f64::MAX, "1.7976931348623157e+308"),
        (18446744073709551616.0, "1.8446744073709552e+19"),
    ] {
        assert_eq!(
            format_float_g_shortest(input),
            expected,
            "bits={:016x}",
            input.to_bits()
        );
    }
    // The decimal float constructor's existing Ryu entry has a different
    // zero policy; this facade serves both conversions and diagnostics and
    // must not silently select that entry.
    assert_eq!(
        tidb_query_datatype::codec::mysql::Decimal::native_format_go_shortest_float(-0.0),
        "0"
    );
    let (zero, error) = MyDecimal::from_float64(-0.0);
    assert_eq!(error, None);
    assert_eq!(zero.to_string_bytes(), b"0");
}

#[cfg(test)]
#[test]
fn shared_mydecimal_facade_keeps_raw_storage_and_unwind_writeback() {
    let mut value = MyDecimal::from_string(b"1.25").0;
    value.set_result_frac(1);
    value.word_buf[8] = -12345;
    let bytes = value.to_raw_bytes();
    assert_eq!(
        MyDecimal::from_shared(value.as_shared()).to_raw_bytes(),
        bytes
    );
    assert_eq!(value.digits_frac, 2);
    assert_eq!(value.result_frac, 1);
    assert_eq!(value.to_string_bytes(), b"1.25");
    assert_eq!(value.to_result_string_bytes(), b"1.3");
    assert_eq!(std::mem::size_of::<MyDecimal>(), 40);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        value.with_shared_mut(|shared| {
            assert_eq!(shared.shift(1), None);
            panic!("after actual shared mutation");
        });
    }));
    assert!(outcome.is_err());
    assert_eq!(value.to_string_bytes(), b"12.5");
    assert_eq!(value.result_frac, 1);
    assert_eq!(value.word_buf[8], -12345);
}
