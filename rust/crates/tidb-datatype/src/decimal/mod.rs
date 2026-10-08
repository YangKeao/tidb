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

use std::cmp::Ordering;
use std::ops::Deref;

use crate::MyDecimal;
use smallvec::SmallVec;
use tidb_query_datatype::codec::mysql::{
    decimal::{
        native_decimal_coefficient_binary, NativeDecimalBinaryOp, NativeDecimalBinaryPolicy,
        NativeDecimalError, NativeDecimalOp, Res as SharedDecimalResult,
    },
    native_decimal_cmp, native_decimal_from_literal, native_decimal_normalize,
    native_decimal_parse_mysql, native_decimal_shift_mysql, Decimal as SharedDecimal,
    NativeDecimalCmpParts, NativeDecimalParseRef, NativeDecimalParseValue,
};

// TPC-H's DECIMAL(15,2) values need up to 17 coefficient bytes. Keeping the
// common fixed-point widths inline avoids a heap allocation while decoding
// every row and while folding SUM/AVG states. Wider DECIMAL values still use
// SmallVec's spill path, so this does not change the supported precision.
const INLINE_DECIMAL_DIGITS: usize = 24;

/// Private construction/math adapter for SDK-owned coefficient storage.
/// `Decimal` itself owns `NativeDecimalParseValue`; this helper only preserves
/// the native inline width while local public-API adapters build intermediate
/// coefficients.
#[derive(Clone, Debug)]
struct DecimalDigits(SmallVec<[u8; INLINE_DECIMAL_DIGITS]>);

impl DecimalDigits {
    fn from_ascii(bytes: SmallVec<[u8; INLINE_DECIMAL_DIGITS]>) -> Self {
        debug_assert!(bytes.iter().all(u8::is_ascii_digit));
        Self(bytes)
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("decimal coefficients are ASCII digits")
    }

    fn from_unsigned(value: u128) -> Self {
        Self::from_ascii(NativeDecimalParseValue::coefficient_from_unsigned(value))
    }
}

impl From<String> for DecimalDigits {
    fn from(digits: String) -> Self {
        debug_assert!(digits.bytes().all(|digit| digit.is_ascii_digit()));
        Self(SmallVec::from_vec(digits.into_bytes()))
    }
}

impl Deref for DecimalDigits {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::fmt::Display for DecimalDigits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A fixed-point decimal value: `(-1)^negative * digits * 10^-scale`, where
/// `digits` is an unsigned decimal digit string (no separators) at least
/// `scale` characters long (left-padded with `0` so slicing off the
/// fractional part is always valid). Mirrors MySQL `DECIMAL`: the literal's
/// own scale is preserved as written (`3.14` and `3.140` compare equal but
/// render differently — `3.14`/`3.140`), and a numerically zero value always
/// renders and compares without a sign (`-0.00` normalizes to `0.00`).
///
/// Arithmetic is done digit-by-digit on these strings (schoolbook add/
/// subtract/multiply/long-division) rather than via a numeric type, so it is
/// exact for any precision `DECIMAL` supports — no float-style rounding
/// error, and no dependency on how Rust or Go format a binary float. `/`
/// (MySQL's `DECIMAL` division: dividend scale plus a fixed precision
/// increment, then MyDecimal's rounding) is [`Decimal::true_div`] — a
/// different, harder problem than the truncating division `DIV`/`MOD` need
/// (see [`Decimal::div_rem`]).
#[derive(Debug, Clone)]
pub struct Decimal(NativeDecimalParseValue);

/// Source `MyDecimal.ToInt`/`ToUint` non-fatal disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalIntegerWarning {
    /// A non-zero fractional part was discarded.
    Truncated,
    /// The integer magnitude was outside the destination range.
    Overflow,
}

/// Source `MyDecimal.FromString`'s single non-fatal/fatal disposition.
pub use tidb_query_datatype::codec::mysql::NativeDecimalParseError as DecimalParseError;

impl Decimal {
    /// Moves exact shared coefficient storage into the native value, preserving
    /// sign, visible/storage scales and declared shape without validation or
    /// normalization. Arithmetic and formatting retain their own preconditions.
    pub fn from_shared_parse(value: NativeDecimalParseValue) -> Self {
        Self(value)
    }

    /// Borrows the actual coefficient and all metadata without UTF-8 checks,
    /// normalization or allocation. This transport view is not SQL admission.
    pub fn as_shared_parse(&self) -> NativeDecimalParseRef<'_> {
        self.0.as_ref()
    }

    /// Moves the actual SmallVec coefficient and all five storage fields into
    /// the shared value without cloning, validation or normalization. Even raw
    /// invalid UTF-8, noncanonical zero signs and declared shape are preserved.
    pub fn into_shared_parse(self) -> NativeDecimalParseValue {
        self.0
    }

    /// Reconstructs an exact decimal representation returned by value transport.
    /// Does not validate or normalize coefficient bytes, sign or either scale;
    /// unlike a SQL constructor, it also preserves noncanonical representations.
    /// The caller restores declared column shape separately with
    /// [`Self::with_declared_shape`]. Arithmetic and text APIs retain their own
    /// representation preconditions.
    pub fn from_raw_parts(negative: bool, digits: Vec<u8>, scale: u32, storage_scale: u32) -> Self {
        Self(NativeDecimalParseValue::from_raw_parts(
            negative,
            SmallVec::from_vec(digits),
            scale,
            storage_scale,
            None,
        ))
    }

    /// Returns exact coefficient storage without ASCII or UTF-8 validation.
    /// This representation accessor is for lossless value transport, not text
    /// formatting or admission to decimal arithmetic.
    pub fn coefficient_bytes(&self) -> &[u8] {
        self.0.as_ref().digits
    }

    fn digits(&self) -> &str {
        std::str::from_utf8(self.coefficient_bytes())
            .expect("decimal coefficients are ASCII digits")
    }

    fn owned_digits(&self) -> DecimalDigits {
        DecimalDigits::from_ascii(SmallVec::from_slice(self.coefficient_bytes()))
    }

    /// Copies the exact coefficient into the shared native-math value domain.
    /// `limit` bounds each materialized buffer's logical data bytes, not SQL
    /// precision or the combined physical peak. Declared column shape is not
    /// part of this arithmetic value; both storage and result scales are.
    pub fn try_to_shared_math(&self, limit: usize) -> Result<SharedDecimal, NativeDecimalError> {
        let parts = self.0.as_ref();
        SharedDecimal::try_from_native_digits(
            parts.negative,
            parts.digits,
            parts.storage_scale,
            parts.scale,
            limit,
        )
    }

    /// Reconstructs an owned native coefficient directly from shared words.
    /// No Display, SQL parse, visible-scale rounding or fixed nine-word cell is
    /// involved. This value-producing bridge clears the declared column shape;
    /// transport retains a zero sign, while ABS/round normalize it in the core.
    pub fn try_from_shared_math(
        value: &SharedDecimal,
        limit: usize,
    ) -> Result<Self, NativeDecimalError> {
        let parts = value.words();
        if parts.result_frac > parts.storage_frac {
            return Err(NativeDecimalError::InvalidInput(
                "result scale exceeds storage scale",
            ));
        }
        let fraction = usize::try_from(parts.storage_frac)
            .map_err(|_| NativeDecimalError::Resource("storage scale exceeds indexing width"))?;
        let int_words = parts.int_digits.div_ceil(DIGITS_PER_WORD);
        let active = int_words
            .checked_add(fraction.div_ceil(DIGITS_PER_WORD))
            .ok_or(NativeDecimalError::Resource("active word count overflow"))?;
        if active == 0 || active > parts.words.len() {
            return Err(NativeDecimalError::InvalidInput(
                "native coefficient requires initialized active words",
            ));
        }
        let bytes = parts
            .int_digits
            .checked_add(fraction)
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or(NativeDecimalError::Resource(
                "coefficient byte count overflow",
            ))?;
        if bytes > limit {
            return Err(NativeDecimalError::Resource(
                "coefficient buffer exceeds limit",
            ));
        }
        if parts.words[..active]
            .iter()
            .any(|word| *word >= CODEC_POWERS10[DIGITS_PER_WORD] as u32)
        {
            return Err(NativeDecimalError::InvalidInput(
                "active word is not base-1e9",
            ));
        }
        let head = parts.int_digits % DIGITS_PER_WORD;
        if head != 0 && parts.words[0] >= CODEC_POWERS10[head] as u32 {
            return Err(NativeDecimalError::InvalidInput(
                "leading word exceeds its digit count",
            ));
        }
        let tail = fraction % DIGITS_PER_WORD;
        if tail != 0 && parts.words[active - 1] % CODEC_POWERS10[DIGITS_PER_WORD - tail] as u32 != 0
        {
            return Err(NativeDecimalError::InvalidInput(
                "fractional word has nonzero padding",
            ));
        }
        let mut digits = Vec::new();
        digits
            .try_reserve_exact(bytes)
            .map_err(|_| NativeDecimalError::Resource("coefficient allocation failed"))?;
        let mut emit_word = |mut word: u32, width: usize| {
            let mut buffer = [b'0'; DIGITS_PER_WORD];
            for byte in buffer[..width].iter_mut().rev() {
                *byte += (word % 10) as u8;
                word /= 10;
            }
            digits.extend_from_slice(&buffer[..width]);
        };
        for (index, word) in parts.words[..int_words].iter().enumerate() {
            let width = if index == 0 {
                (parts.int_digits - 1) % DIGITS_PER_WORD + 1
            } else {
                DIGITS_PER_WORD
            };
            emit_word(*word, width);
        }
        let mut remaining = fraction;
        for word in &parts.words[int_words..active] {
            let width = remaining.min(DIGITS_PER_WORD);
            emit_word(
                *word / CODEC_POWERS10[DIGITS_PER_WORD - width] as u32,
                width,
            );
            remaining -= width;
        }
        // Normalize only excess coefficient-leading zeroes; retained fraction
        // zeroes and both scales remain exact. Avoid repeated front removal.
        let removable = digits.len().saturating_sub(fraction.max(1));
        let leading = digits[..removable]
            .iter()
            .take_while(|digit| **digit == b'0')
            .count();
        digits.drain(..leading);
        Ok(Self(NativeDecimalParseValue::from_raw_parts(
            parts.negative,
            DecimalDigits::from_ascii(SmallVec::from_vec(digits)).0,
            parts.result_frac,
            parts.storage_frac,
            None,
        )))
    }

    // Retain the old infallible value API: bridge/core refusals panic here,
    // never masquerade as a SQL overflow or retry a native arithmetic kernel.
    fn shared_native_math(&self, operation: NativeDecimalOp) -> Self {
        self.try_to_shared_math(usize::MAX)
            .and_then(|value| value.try_native_math(operation, usize::MAX))
            .and_then(|value| Self::try_from_shared_math(&value, usize::MAX))
            .expect("shared native decimal math failed")
    }

    fn shared_native_binary(
        &self,
        other: &Self,
        operation: NativeDecimalBinaryOp,
        policy: NativeDecimalBinaryPolicy,
    ) -> (Self, Option<DecimalCodecWarning>) {
        let output = self
            .try_to_shared_math(usize::MAX)
            .and_then(|left| {
                other
                    .try_to_shared_math(usize::MAX)
                    .and_then(|right| left.try_native_binary(&right, operation, policy, usize::MAX))
            })
            .expect("shared native decimal binary math failed");
        let (value, warning) = match output {
            SharedDecimalResult::Ok(value) => (value, None),
            SharedDecimalResult::Truncated(value) => (value, Some(DecimalCodecWarning::Truncated)),
            SharedDecimalResult::Overflow(value) => (value, Some(DecimalCodecWarning::Overflow)),
        };
        (
            Self::try_from_shared_math(&value, usize::MAX)
                .expect("shared native decimal binary result failed"),
            warning,
        )
    }

    /// The single normalization point for ordinary values, whose stored and
    /// displayed scale are identical.
    fn new(negative: bool, digits: impl Into<DecimalDigits>, scale: u32) -> Self {
        Self::new_with_storage(negative, digits, scale, scale)
    }

    /// Normalizes a value whose internal decimal payload may retain more
    /// fraction digits than its SQL-visible result scale. `storage_scale`
    /// must never be smaller than `scale`.
    fn new_with_storage(
        negative: bool,
        digits: impl Into<DecimalDigits>,
        scale: u32,
        storage_scale: u32,
    ) -> Self {
        Self::new_with_storage_sign(negative, digits, scale, storage_scale, false)
    }

    fn new_with_storage_preserving_zero_sign(
        negative: bool,
        digits: impl Into<DecimalDigits>,
        scale: u32,
        storage_scale: u32,
    ) -> Self {
        Self::new_with_storage_sign(negative, digits, scale, storage_scale, true)
    }

    fn new_with_storage_sign(
        negative: bool,
        digits: impl Into<DecimalDigits>,
        scale: u32,
        storage_scale: u32,
        preserve_zero_sign: bool,
    ) -> Self {
        let digits = digits.into();
        Self::from_shared_parse(native_decimal_normalize(
            negative,
            digits.0,
            scale,
            storage_scale,
            preserve_zero_sign,
        ))
    }

    /// Parses a decimal literal's canonical text — an optional `-`/`+` sign
    /// then digits with at most one `.`, which is exactly what this type's own
    /// [`Display`](std::fmt::Display) produces.
    ///
    /// The sign is part of the accepted syntax rather than the caller's job:
    /// this is the parser every canonical round trip goes through (a chunk
    /// cell read back as `MyDecimal` text, a spilled aggregate state, a
    /// binary-protocol cell), and a leading `-` left in the caller's hands
    /// silently produced a value whose unsigned digit string still carried
    /// the `-`. Such a value PRINTED correctly while comparing as positive
    /// and panicking in digit arithmetic. Accepting the sign here is what
    /// removes that possibility from every caller at once.
    pub fn from_literal(text: &str) -> Self {
        Self::from_shared_parse(native_decimal_from_literal(text))
    }

    /// Builds an exact decimal from a signed base-10 coefficient and scale.
    /// This is used when a fixed-scale aggregate is finalized.
    pub fn from_scaled_i128(value: i128, scale: u32) -> Self {
        Decimal::new(
            value < 0,
            DecimalDigits::from_unsigned(value.unsigned_abs()),
            scale,
        )
    }

    /// Converts the exact chunk-layout [`MyDecimal`] into the value-layer
    /// decimal without losing either its visible `resultFrac` or the hidden
    /// base-1e9 fraction digits retained for later arithmetic.
    #[must_use]
    pub fn from_my_decimal(value: &MyDecimal) -> Self {
        Self::from_shared_parse(
            tidb_query_datatype::codec::mysql::native_decimal_from_my_decimal(value.as_shared()),
        )
    }

    /// Converts this value to Go's exact `MyDecimal` storage shape without
    /// discarding fraction digits retained beyond the displayed scale.
    pub fn to_my_decimal(&self) -> Result<MyDecimal, crate::mydecimal::DecimalError> {
        MyDecimal::from_decimal_parts(
            self.is_negative(),
            self.digits(),
            self.storage_scale(),
            self.scale(),
            false,
        )
    }

    /// Converts this value to the `MyDecimal` shape used by Go chunk datums.
    /// Go's datum-to-chunk path always has at least one integer digit, even
    /// for values below one, while hidden fraction words remain intact.
    pub fn to_chunk_my_decimal(&self) -> Result<MyDecimal, crate::mydecimal::DecimalError> {
        MyDecimal::from_decimal_parts(
            self.is_negative(),
            self.digits(),
            self.storage_scale(),
            self.scale(),
            true,
        )
    }

    /// Converts this value to the fixed `MyDecimal` cell used by Go's chunk
    /// datums, retaining Go's prefix/truncation result when the value exceeds
    /// the nine-word storage buffer.
    ///
    /// Go's `Datum` already owns a `MyDecimal`; `chunk.AppendDatum` and
    /// `MutRow.SetDatum` copy that value and do not introduce a new overflow
    /// panic. Rust's value-layer [`Decimal`] can temporarily carry more digits
    /// than that fixed buffer, so the shared fixed-word projection applies
    /// `MyDecimal.FromString`'s prefix/truncation rules directly to coefficient
    /// bytes rather than formatting and reparsing a SQL literal.
    #[must_use]
    pub fn to_chunk_my_decimal_lossy(&self) -> MyDecimal {
        MyDecimal::from_decimal_parts_lossy(
            self.is_negative(),
            self.digits(),
            self.storage_scale(),
            self.scale(),
            true,
        )
        .expect("Decimal coefficient and scale are valid")
    }

    /// Parses the signed decimal strings accepted by datatype conversion.
    pub fn from_signed_literal(text: &str) -> Self {
        Self::parse_mysql(text).0
    }

    /// Source `MyDecimal.FromString`, including the fixed word buffer,
    /// exponent parsing, prefix acceptance, and exact error disposition.
    pub fn parse_mysql(text: &str) -> (Self, Option<DecimalParseError>) {
        Self::parse_mysql_with_word_limit(text, CODEC_WORD_BUF_LEN)
    }

    pub(crate) fn parse_mysql_with_word_limit(
        text: &str,
        word_limit: usize,
    ) -> (Self, Option<DecimalParseError>) {
        let (value, disposition) = native_decimal_parse_mysql(text, word_limit);
        (Self::from_shared_parse(value), disposition)
    }

    /// Promotes an integer to a decimal of scale 0, for mixed `int op decimal`
    /// arithmetic/comparison (MySQL's implicit promotion rule).
    pub fn from_int(i: i64) -> Self {
        Self::from_shared_parse(NativeDecimalParseValue::from_int(i))
    }

    /// Promotes an unsigned integer to a decimal of scale 0. This must not
    /// pass through `i64`: values above `i64::MAX` are ordinary MySQL
    /// `UNSIGNED` values and retain their full magnitude in decimal
    /// arithmetic and comparison.
    pub fn from_uint(i: u64) -> Self {
        Self::from_shared_parse(NativeDecimalParseValue::from_uint(i))
    }

    /// Source `MyDecimal.FromFloat64`.
    pub fn from_f64(value: f64) -> Option<Self> {
        SharedDecimal::native_from_f64(value).map(|shared| {
            Self::try_from_shared_math(&shared, usize::MAX)
                .expect("shared native float decimal materialization failed")
        })
    }

    /// Source `MyDecimal.FromParquetArray`: decode a signed big-endian
    /// two's-complement Parquet DECIMAL payload and apply its logical scale.
    /// As in Go, negative input is converted to magnitude in place.
    pub fn from_parquet_array(bytes: &mut [u8], scale: i32) -> (Self, Option<DecimalCodecWarning>) {
        if bytes.is_empty() {
            return (Self::from_int(0), None);
        }
        let negative = bytes[0] & 0x80 != 0;
        if negative {
            for byte in bytes.iter_mut() {
                *byte = !*byte;
            }
            for byte in bytes.iter_mut().rev() {
                *byte = byte.wrapping_add(1);
                if *byte != 0 {
                    break;
                }
            }
        }

        let mut magnitude = "0".to_owned();
        for byte in bytes.iter().copied() {
            magnitude = digit_add(&digit_mul(&magnitude, "256"), &u32::from(byte).to_string());
        }
        if magnitude.trim_start_matches('0').len() > CODEC_WORD_BUF_LEN * DIGITS_PER_WORD {
            return (Self::from_int(0), Some(DecimalCodecWarning::Overflow));
        }
        let integer = Self::new(negative, magnitude, 0);
        let (shifted, warning) = integer.shift_mysql(-scale);
        if warning.is_some() {
            return (shifted, warning);
        }
        (shifted.truncate_to_scale(scale), None)
    }

    /// Source `NewMaxOrMinDec`/`maxDecimal`.
    pub fn max_or_min(negative: bool, precision: u32, frac: u32) -> Self {
        Self::from_shared_parse(NativeDecimalParseValue::max_or_min(
            negative, precision, frac,
        ))
    }

    /// Returns the number of fractional decimal digits preserved by this
    /// value's representation.
    pub fn scale(&self) -> u32 {
        self.0.as_ref().scale
    }

    /// Returns whether the stored numeric value is negative.
    ///
    /// This is a semantic storage/protocol accessor, not a leak of Go's
    /// base-1e9 `MyDecimal` word layout. Zero is normalized to non-negative.
    pub const fn is_negative(&self) -> bool {
        self.0.negative()
    }

    /// Returns the lossless unsigned coefficient digits retained for exact
    /// arithmetic and storage codecs.
    ///
    /// The decimal value is `coefficient * 10^-storage_scale`; callers must
    /// use [`Decimal::storage_scale`] rather than the SQL-visible [`Self::scale`]
    /// because division can retain hidden precision for a later aggregate.
    pub fn coefficient_digits(&self) -> &str {
        self.digits()
    }

    /// Returns the signed coefficient and retained fractional scale when the
    /// coefficient fits in an i128.
    /// The coefficient of a value whose visible scale IS its storage scale,
    /// for seeding a fixed-scale accumulator.
    ///
    /// [`Self::coefficient_i128`] reports `storage_scale`, which a division
    /// result can carry more of than `scale` prints. Rebuilding from the
    /// coefficient alone (`from_scaled_i128`) would then publish those hidden
    /// digits, so a value whose two scales differ keeps the exact path.
    #[must_use]
    pub fn fold_coefficient_i128(&self) -> Option<(i128, u32)> {
        if self.scale() != self.storage_scale() {
            return None;
        }
        self.coefficient_i128()
    }

    pub fn coefficient_i128(&self) -> Option<(i128, u32)> {
        let parts = self.0.as_ref();
        SharedDecimal::native_raw_coefficient_i128(
            parts.negative,
            parts.digits,
            parts.storage_scale,
        )
    }

    #[cfg(test)]
    pub(crate) fn coefficient_is_inline(&self) -> bool {
        self.0.coefficient_is_inline()
    }

    /// Builds a value straight from coefficient parts for differential tests
    /// of [`Ord::cmp`]: the tests need shapes the parser cannot produce
    /// directly (hidden division precision, excess leading zeros).
    #[cfg(test)]
    pub(crate) fn from_test_parts(
        negative: bool,
        digits: &str,
        scale: u32,
        storage_scale: u32,
    ) -> Self {
        Self::new_with_storage(negative, digits.to_string(), scale, storage_scale)
    }

    /// Returns the scale of the lossless stored coefficient.
    ///
    /// This can exceed [`Self::scale`], which is the rounded SQL presentation
    /// scale. Storage and protocol codecs need this value to avoid discarding
    /// arithmetic precision.
    pub const fn storage_scale(&self) -> u32 {
        self.0.storage_scale()
    }

    /// Stamps the declared `DECIMAL(M, D)` column shape onto this value.
    ///
    /// Source: Go `Datum.convertToMysqlDecimal`'s
    /// `ret.SetLength(target.GetFlen()); ret.SetFrac(target.GetDecimal())`.
    #[must_use]
    pub fn with_declared_shape(mut self, flen: i64, decimal: i64) -> Self {
        self.0.set_declared_shape(Some((flen, decimal)));
        self
    }

    /// The declared column shape, or `None` for a value no column produced.
    pub const fn declared_shape(&self) -> Option<(i64, i64)> {
        self.0.declared_shape()
    }

    /// The `(precision, frac)` pair storage codecs must encode under.
    ///
    /// `(0, 0)` means "no declared shape", which is precisely what Go's
    /// `EncodeDecimal` reads as `precision == 0` and answers with
    /// `PrecisionAndFrac`; an unstamped Go `Datum` reports `Length() == 0` the
    /// same way. Callers pass this pair through unchanged so the fallback stays
    /// in the one place Go put it.
    pub const fn storage_shape(&self) -> (i64, i64) {
        match self.0.declared_shape() {
            Some(shape) => shape,
            None => (0, 0),
        }
    }

    /// Source `MyDecimal.PrecisionAndFrac`.
    ///
    /// This is the value's OWN shape and never the column's; a payload written
    /// at `DECIMAL(10, 4)` still reports `(4, 2)` for `11.99`, matching Go.
    /// Storage codecs want [`Decimal::storage_shape`] instead.
    pub fn precision_and_frac(&self) -> (i32, i32) {
        self.as_shared_parse().natural_precision_and_frac()
    }

    /// Source `MyDecimal.ToHashKey`: numerically equal decimals with different
    /// written scales produce the same key.
    pub fn to_hash_key(&self) -> Result<(Vec<u8>, Option<DecimalCodecWarning>), DecimalCodecError> {
        let (precision, fraction) = self.as_shared_parse().hash_precision_and_frac();
        let (mut key, warning) = self.to_bin(precision, fraction)?;
        key.push(fraction as u8);
        Ok((
            key,
            if warning == Some(DecimalCodecWarning::Truncated) {
                None
            } else {
                warning
            },
        ))
    }

    /// Source `MyDecimal.HashKeySize`.
    pub fn hash_key_size(&self) -> Result<usize, DecimalCodecError> {
        let (precision, fraction) = self.as_shared_parse().hash_precision_and_frac();
        decimal_bin_size(precision, fraction).map(|size| size + 1)
    }

    /// Returns whether this value is numerically zero.
    pub fn is_zero(&self) -> bool {
        self.as_shared_parse().is_zero()
    }

    /// Returns this value with its sign reversed, canonicalizing zero.
    pub fn negate(&self) -> Self {
        self.shared_native_math(NativeDecimalOp::Negate)
    }

    /// Returns the non-negative magnitude of this value.
    pub fn abs(&self) -> Self {
        self.shared_native_math(NativeDecimalOp::Abs)
    }

    /// `-1` / `0` / `1`, for `SIGN`.
    pub fn signum(&self) -> i64 {
        if self.is_zero() {
            0
        } else if self.is_negative() {
            -1
        } else {
            1
        }
    }

    /// Exact decimal addition: aligns both operands to `max(scale1, scale2)`
    /// (an exact, no-rounding rescale — padding the shorter fractional part
    /// with zero digits), then adds or subtracts magnitudes depending on sign.
    pub fn add(&self, other: &Decimal) -> Decimal {
        self.shared_native_binary(
            other,
            NativeDecimalBinaryOp::Add,
            NativeDecimalBinaryPolicy::Exact,
        )
        .0
    }

    /// Source `DecimalAdd`, including MyDecimal's nine-word result bound.
    pub fn add_mysql(&self, other: &Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        self.shared_native_binary(
            other,
            NativeDecimalBinaryOp::Add,
            NativeDecimalBinaryPolicy::MySql,
        )
    }

    /// Source `DecimalSub`, including MyDecimal's nine-word result bound.
    pub fn sub_mysql(&self, other: &Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        self.shared_native_binary(
            other,
            NativeDecimalBinaryOp::Subtract,
            NativeDecimalBinaryPolicy::MySql,
        )
    }

    /// Exact decimal multiplication: result scale is `scale1 + scale2`
    /// (multiplying two exact fixed-point values never loses precision, so
    /// this needs no rounding — unlike division).
    pub fn mul(&self, other: &Decimal) -> Decimal {
        self.shared_native_binary(
            other,
            NativeDecimalBinaryOp::Multiply,
            NativeDecimalBinaryPolicy::Exact,
        )
        .0
    }

    /// Source `DecimalMul`, including unsigned-i128 eligibility, projected
    /// nine-word arithmetic, signed overflow zero and retained-scale rounding.
    pub fn mul_mysql(&self, other: &Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        self.shared_native_binary(
            other,
            NativeDecimalBinaryOp::Multiply,
            NativeDecimalBinaryPolicy::MySql,
        )
    }

    /// Source `MyDecimal.Shift`: multiply by `10^shift` inside MyDecimal's
    /// fixed nine-word buffer. Integer overflow leaves the value untouched;
    /// excess fractional words are rounded half-up and reported as truncated.
    pub fn shift_mysql(&self, shift: i32) -> (Decimal, Option<DecimalCodecWarning>) {
        self.shift_mysql_with_word_limit(shift, CODEC_WORD_BUF_LEN)
    }

    /// The source tests temporarily reduce Go's package-global `wordBufLen`.
    /// An explicit limit gives the same coverage without mutable global state.
    pub(crate) fn shift_mysql_with_word_limit(
        &self,
        shift: i32,
        word_limit: usize,
    ) -> (Decimal, Option<DecimalCodecWarning>) {
        let (value, warning) =
            native_decimal_shift_mysql(self.as_shared_parse(), shift, word_limit);
        (Self::from_shared_parse(value), warning)
    }

    /// Truncating division (`DIV`) and its remainder (`MOD`): pads both
    /// operands to a common scale first — their digit strings, read as plain
    /// integers, then have the exact same ratio as the decimal values (the
    /// scaling cancels) — then does unsigned long division on those
    /// integers. The quotient is `trunc(a/b)`; the remainder, reinterpreted
    /// at that same common scale, is exactly `a - trunc(a/b)*b` — precisely
    /// the scale MySQL's decimal `MOD` uses (`max(scale_a, scale_b)`, sign of
    /// the dividend). `None` for division by zero (MySQL: `NULL`) or a
    /// quotient too large for `i64`.
    pub fn div_rem(&self, other: &Decimal) -> Option<(i64, Decimal)> {
        let (quotient, remainder) = self.div_rem_unbounded(other)?;
        let (quotient, warning) = quotient.to_i64_trunc();
        (warning != Some(DecimalIntegerWarning::Overflow)).then_some((quotient, remainder))
    }

    /// Truncating division (`DIV`) and remainder with the complete quotient.
    ///
    /// Go's decimal `DIV` evaluates `DecimalDiv` and only then converts the
    /// quotient through `ToInt` or `ToUint`. The latter accepts every value in
    /// `[0, 2^64)` when either input is unsigned, so routing the quotient
    /// through `i64` first loses valid results above `i64::MAX`. This value
    /// layer keeps the quotient as a scale-zero [`Decimal`]; the expression
    /// layer can then apply the source conversion and distinguish overflow
    /// from a valid upper-half unsigned result.
    pub fn div_rem_unbounded(&self, other: &Decimal) -> Option<(Decimal, Decimal)> {
        if other.is_zero() {
            return None;
        }
        let (quotient, remainder) = self
            .try_to_shared_math(usize::MAX)
            .and_then(|left| {
                other
                    .try_to_shared_math(usize::MAX)
                    .and_then(|right| left.try_native_div_rem_exact(&right))
            })
            .expect("shared native exact decimal division failed")?;
        Some((
            Self::try_from_shared_math(&quotient, usize::MAX)
                .expect("shared native exact decimal quotient result failed"),
            Self::try_from_shared_math(&remainder, usize::MAX)
                .expect("shared native exact decimal remainder result failed"),
        ))
    }

    /// Source `DecimalMod`, without routing the discarded quotient through
    /// `i64` (the source accepts quotients wider than BIGINT).
    pub fn rem_mysql(&self, other: &Decimal) -> Option<Decimal> {
        self.try_to_shared_math(usize::MAX)
            .and_then(|left| {
                other
                    .try_to_shared_math(usize::MAX)
                    .and_then(|right| left.try_native_rem(&right, usize::MAX))
            })
            .and_then(|value| {
                value
                    .map(|value| Self::try_from_shared_math(&value, usize::MAX))
                    .transpose()
            })
            .expect("shared native decimal remainder failed")
    }

    /// Source `DecimalDiv`: retain the whole base-1e9 fraction words produced
    /// by the division while exposing `div_precision_increment` through
    /// `resultFrac`. Use [`Self::div_mysql_with_warning`] when the caller
    /// needs Go's fixed-word disposition as well.
    pub fn div_mysql(&self, other: &Decimal, frac_increment: u32) -> Option<Decimal> {
        self.div_mysql_with_warning(other, frac_increment)
            .map(|(value, _)| value)
    }

    /// Source `DecimalDiv`, retaining the fixed-word disposition beside the
    /// quotient. `None` means division by zero.
    pub fn div_mysql_with_warning(
        &self,
        other: &Decimal,
        frac_increment: u32,
    ) -> Option<(Decimal, Option<DecimalCodecWarning>)> {
        let output = self
            .try_to_shared_math(usize::MAX)
            .and_then(|left| {
                other
                    .try_to_shared_math(usize::MAX)
                    .and_then(|right| left.try_native_mysql_div(&right, frac_increment, usize::MAX))
            })
            .expect("shared native decimal division failed")?;
        let (value, warning) = match output {
            SharedDecimalResult::Ok(value) => (value, None),
            SharedDecimalResult::Truncated(value) => (value, Some(DecimalCodecWarning::Truncated)),
            SharedDecimalResult::Overflow(value) => (value, Some(DecimalCodecWarning::Overflow)),
        };
        Some((
            Self::try_from_shared_math(&value, usize::MAX)
                .expect("shared native decimal division result failed"),
            warning,
        ))
    }

    /// MyDecimal `ToString`, which exposes stored fraction words without the
    /// `resultFrac` presentation rounding used by `String`.
    pub fn storage_string(&self) -> String {
        Decimal::new_with_storage(
            self.is_negative(),
            self.owned_digits(),
            self.storage_scale(),
            self.storage_scale(),
        )
        .to_string()
    }

    /// True (rounding) division by a positive integer divisor, to
    /// `target_scale` fractional digits — `AVG`'s `SUM / COUNT`, where MySQL
    /// grows the result scale by the caller's `div_precision_increment`
    /// rather than dividing
    /// exactly. Delegates to the shared MySQL decimal division owner with the
    /// increment implied by `target_scale`; unlike integer `DIV`, this rounds.
    /// `target_scale` must be `>= self.scale` (always true for `AVG`, which
    /// only grows scale).
    pub fn div_round(&self, divisor: i64, target_scale: u32) -> Decimal {
        self.true_div(&Decimal::from_int(divisor), target_scale)
            .expect("AVG divisor is positive")
    }

    /// True (rounding) division by an arbitrary `Decimal` divisor — MySQL's
    /// `/` operator, which (confirmed via `goeval`, not assumed) grows the
    /// result scale by the SAME fixed increment `AVG`'s `div_round` uses
    /// (4), applied past the DIVIDEND's own scale only — the divisor's own
    /// scale never affects the result scale (`5 / 2.5` and `5 / 2` both
    /// land at the dividend's `0 + 4 = 4` fractional digits). `None` for
    /// division by zero (MySQL: `NULL`). Aligns both operands to a common
    /// scale first (the same trick [`Decimal::div_rem`] uses — their digit
    /// strings, read as plain integers, then have the exact same ratio as
    /// the decimal values, since the shared scale cancels), then applies
    /// the identical "one extra digit, then round it away" technique
    /// `div_round` uses, generalized to a `Decimal` (not just an `i64`)
    /// divisor. Sign follows the standard XOR rule, same as every other
    /// decimal operator.
    pub fn true_div(&self, other: &Decimal, target_scale: u32) -> Option<Decimal> {
        self.true_div_with_warning(other, target_scale)
            .map(|(value, _)| value)
    }

    /// MySQL `/` division with the fixed-word disposition retained beside the
    /// quotient. `None` means division by zero.
    pub fn true_div_with_warning(
        &self,
        other: &Decimal,
        target_scale: u32,
    ) -> Option<(Decimal, Option<DecimalCodecWarning>)> {
        self.div_mysql_with_warning(other, target_scale.saturating_sub(self.scale()))
    }

    /// Rounds to the nearest integer, ties away from zero — MySQL's
    /// decimal-to-integer conversion rule for bitwise/shift operators (which
    /// operate on integers, not decimals). `None` on overflow past `i64`.
    pub fn round_to_i64(&self) -> Option<i64> {
        self.as_shared_parse().round_to_i64()
    }

    /// Source `MyDecimal.ToInt`: truncates toward zero and reports a non-zero
    /// discarded fraction separately from overflow.
    pub fn to_i64_trunc(&self) -> (i64, Option<DecimalIntegerWarning>) {
        let parts = self.0.as_ref();
        match SharedDecimal::native_to_i64_trunc(parts.negative, parts.digits, parts.storage_scale)
        {
            SharedDecimalResult::Ok(value) => (value, None),
            SharedDecimalResult::Truncated(value) => {
                (value, Some(DecimalIntegerWarning::Truncated))
            }
            SharedDecimalResult::Overflow(value) => (value, Some(DecimalIntegerWarning::Overflow)),
        }
    }

    /// Source `MyDecimal.ToUint`: truncates toward zero, rejects negatives,
    /// and saturates positive overflow.
    pub fn to_u64_trunc(&self) -> (u64, Option<DecimalIntegerWarning>) {
        let parts = self.0.as_ref();
        match SharedDecimal::native_to_u64_trunc(parts.negative, parts.digits, parts.storage_scale)
        {
            SharedDecimalResult::Ok(value) => (value, None),
            SharedDecimalResult::Truncated(value) => {
                (value, Some(DecimalIntegerWarning::Truncated))
            }
            SharedDecimalResult::Overflow(value) => (value, Some(DecimalIntegerWarning::Overflow)),
        }
    }

    /// Like [`Decimal::round_to_i64`], but CLAMPS to `i64::MIN`/`MAX`
    /// instead of failing on overflow — `CAST(... AS SIGNED)`'s own rule
    /// (confirmed via `goeval`: `CAST(1e300 AS SIGNED)` is
    /// `9223372036854775807`, a genuine saturating clamp, not the hard
    /// `ErrOverflow` `~x`'s own bitwise conversion raises — MySQL's
    /// "truncate as warning" `SQL_MODE` downgrades the overflow to a
    /// clamp-and-warn for an explicit `CAST`, unlike an implicit bitwise
    /// coercion).
    pub fn round_to_i64_saturating(&self) -> i64 {
        self.as_shared_parse().round_to_i64_saturating()
    }

    /// `CAST(... AS UNSIGNED)`'s decimal rule: round half away from zero to an
    /// integer (Go `ModeHalfUp`, matching [`Decimal::round_to_i64`]), then Go
    /// `MyDecimal.ToUint`. A negative value is `ToUint`'s `ErrOverflow`, which
    /// the cast reports as `0`; a magnitude past `u64::MAX` saturates to
    /// `u64::MAX` (Go `ToUint` returns `MaxUint64` on positive overflow). Unlike
    /// routing through the `i64` path, this preserves values in
    /// `(i64::MAX, u64::MAX]` — the upper half of an `UNSIGNED BIGINT`.
    #[must_use]
    pub fn round_to_u64_saturating(&self) -> u64 {
        self.as_shared_parse().round_to_u64_saturating()
    }

    /// `CAST`/`CONVERT`'s own `DECIMAL(flen, scale)` target: rounds to
    /// `scale` fractional digits (ties away from zero, same as
    /// [`Decimal::round_to_scale`]), then clamps the MAGNITUDE to the
    /// largest value representable in `flen` total digits — confirmed via
    /// `goeval`: `CAST(123456 AS DECIMAL(5,2))` is `999.99`, not an error
    /// or a silently-oversized result. `flen == 0` means unspecified (no
    /// magnitude clamp at all — see `tidb_ast::CastType::Decimal`'s own
    /// doc for why); `flen <= scale` (a malformed target nobody writes
    /// intentionally — real MySQL itself errors constructing it) is
    /// treated as "zero digits of integer part allowed", clamping any
    /// nonzero magnitude straight to the all-`9`s value rather than
    /// underflowing `flen - scale` — a deliberately narrow, not fully
    /// MySQL-faithful, fallback for a degenerate case, not a realistic
    /// query.
    pub fn cast_to_precision(&self, flen: u32, scale: u32) -> Decimal {
        self.try_to_shared_math(usize::MAX)
            .and_then(|value| value.try_native_cast_to_precision(flen, scale, usize::MAX))
            .and_then(|value| Self::try_from_shared_math(&value, usize::MAX))
            .expect("shared native decimal precision cast failed")
    }

    /// Converts to the nearest `f64` — MySQL's implicit `DECIMAL`-to-
    /// `FLOAT`/`DOUBLE` promotion rule when a `Decimal` operand meets a
    /// `Float` one. Lossy for precision beyond `f64`'s ~15-17 significant
    /// digits, same as MySQL's own conversion; parses this value's own
    /// canonical `Display` text, which is always valid decimal syntax.
    pub fn to_f64(&self) -> f64 {
        self.as_shared_parse().to_f64()
    }

    /// The EXACT mathematical ceiling (`ceiling: true`) or floor
    /// (`false`), as a new `Decimal` at scale 0 — computed by the shared
    /// exact word core (not via `f64`), so it's exact for arbitrary
    /// precision (unlike `round_to_i64`, this never loses precision to
    /// `i64`'s own range — `CEIL`/`FLOOR`'s own `i64`-fitting check, if
    /// any, is the CALLER's job, matching real MySQL: `CEIL`/`FLOOR`
    /// return `BIGINT` when the exact result fits, else `DECIMAL`,
    /// confirmed via `goeval`, not assumed). `CEIL` rounds toward
    /// positive infinity, `FLOOR` toward negative infinity (confirmed via
    /// `goeval`: `CEIL(-3.14)` is `-3`, `FLOOR(-3.14)` is `-4` — the
    /// magnitude rounds up in the OPPOSITE direction from the value's own
    /// sign, i.e. `CEIL` truncates a negative value's magnitude while
    /// `FLOOR` rounds it up, and vice versa for a positive value).
    pub fn ceil_floor(&self, ceiling: bool) -> Decimal {
        self.shared_native_math(if ceiling {
            NativeDecimalOp::Ceil
        } else {
            NativeDecimalOp::Floor
        })
    }

    /// Rounds to `target_scale` fractional digits, ties away from zero
    /// (`ModeHalfUp` — MySQL's default rounding mode) — the general form of
    /// [`Decimal::round_to_i64`] (always scale 0) and [`Decimal::ceil_floor`]
    /// (always scale 0, never a caller-chosen target), used by
    /// `ROUND(decimal, frac)`. `target_scale` may be negative (rounding into
    /// the integer part, e.g. `ROUND(12345, -2)` is `12300`) or exceed
    /// `self.scale` (grows the fractional part with exact zero digits, no
    /// rounding). The caller clamps `target_scale` to MySQL's `DECIMAL` max
    /// scale (30) before calling, matching real MySQL (confirmed by reading
    /// `calculateDecimal4RoundAndTruncate` in `builtin_math.go`, not
    /// assumed): `ROUND(3.14159, 100)` does not grow to 100 fractional
    /// digits.
    pub fn round_to_scale(&self, target_scale: i32) -> Decimal {
        self.round_or_truncate_to_scale(target_scale, true)
    }

    /// Ports `MyDecimal.Round(..., ModeCeiling)` exactly.
    ///
    /// Despite its name, the source mode is not mathematical ceiling: its
    /// current behavior rounds a non-zero discarded magnitude away from zero
    /// for both signs. This is distinct from [`Self::ceil_floor`], which owns
    /// SQL `CEIL`/`FLOOR` semantics.
    pub fn round_ceiling_to_scale(&self, target_scale: i32) -> Decimal {
        let result_scale = target_scale.max(0) as u32;
        let shift = self.storage_scale() as i32 - target_scale;
        if shift <= 0 {
            let digits = pad_scale(self.digits(), self.storage_scale(), result_scale);
            return Decimal::new(self.is_negative(), digits, result_scale);
        }

        let shift = shift as usize;
        let mut digits = self.owned_digits();
        if digits.len() <= shift {
            digits = format!("{}{digits}", "0".repeat(shift + 1 - digits.len())).into();
        }
        let split = digits.len() - shift;
        let kept = &digits[..split];
        // Go's non-word-aligned `MyDecimal.Round` branch has a documented
        // ceiling TODO and inspects only the first digit after the cut. The
        // word-aligned branch scans every discarded word; preserve that
        // source inconsistency instead of applying mathematical ceiling to
        // the whole remainder in both cases.
        let discarded_nonzero = if target_scale >= 0 && target_scale % DIGITS_PER_WORD as i32 == 0 {
            digits[split..].bytes().any(|digit| digit != b'0')
        } else {
            digits
                .as_bytes()
                .get(split)
                .is_some_and(|digit| *digit != b'0')
        };
        let mut kept = if discarded_nonzero {
            digit_add(kept, "1")
        } else {
            kept.to_owned()
        };
        if target_scale < 0 {
            kept.push_str(&"0".repeat((-target_scale) as usize));
        }
        Decimal::new(self.is_negative(), kept, result_scale)
    }

    /// Truncates (never rounds) to `target_scale` fractional digits
    /// (`ModeTruncate`), used by `TRUNCATE(decimal, frac)`. Same shape as
    /// [`Decimal::round_to_scale`] but the digit immediately past the cut is
    /// always dropped rather than inspected.
    pub fn truncate_to_scale(&self, target_scale: i32) -> Decimal {
        self.round_or_truncate_to_scale(target_scale, false)
    }

    /// Fits this value into a `DECIMAL(precision, scale)` column: rounds to
    /// `scale` fractional digits, then checks the rounded value has at most
    /// `precision - scale` significant integer digits. Returns the rounded
    /// value if it fits, `None` if the integer part overflows. Real
    /// MySQL/TiDB rounds FIRST, so a value that only overflows AFTER
    /// rounding is rejected — `99.995` rounds to `100.00`, which overflows
    /// `DECIMAL(4,2)` (confirmed via `gorun`), while `99.994` rounds to
    /// `99.99` and fits. A value below 1 has zero significant integer
    /// digits (`0.50` fits `DECIMAL(4,2)` — the placeholder leading `0`
    /// doesn't count). Used by `tidb_exec`'s column-width validation on
    /// `INSERT`/`UPDATE`.
    pub fn fit_precision_scale(&self, precision: u32, scale: u32) -> Option<Decimal> {
        let (value, overflowed) = self.fit_precision_scale_or_clamp(precision, scale)?;
        (!overflowed).then_some(value)
    }

    /// Shared assignment fitting with its signed maximum on overflow.
    pub(crate) fn fit_precision_scale_or_clamp(
        &self,
        precision: u32,
        scale: u32,
    ) -> Option<(Decimal, bool)> {
        self.0
            .fit_precision_scale(precision, scale)
            .map(|(value, overflowed)| (Self::from_shared_parse(value), overflowed))
    }

    /// Thin native-policy bridge for [`Decimal::round_to_scale`] and
    /// [`Decimal::truncate_to_scale`]. The shared core owns rounding and the
    /// original unchecked scale arithmetic, including profile-dependent wraps.
    fn round_or_truncate_to_scale(&self, target_scale: i32, round: bool) -> Decimal {
        let result_scale = target_scale.max(0) as u32;
        self.round_or_truncate_to_scale_with_storage(target_scale, round, result_scale)
    }

    fn round_or_truncate_to_scale_with_storage(
        &self,
        target_scale: i32,
        round: bool,
        storage_scale: u32,
    ) -> Decimal {
        self.try_to_shared_math(usize::MAX)
            .and_then(|value| {
                value.try_native_round_with_storage(target_scale, round, storage_scale, usize::MAX)
            })
            .and_then(|value| Self::try_from_shared_math(&value, usize::MAX))
            .expect("shared native decimal rounding failed")
    }
}

impl std::fmt::Display for Decimal {
    /// The canonical string form (MyDecimal's `String()`): the sign (omitted
    /// for zero), then the digits with the decimal point inserted `scale`
    /// places from the right — omitted entirely when `scale == 0`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts = self.0.as_ref();
        f.write_str(&SharedDecimal::native_format_visible(
            parts.negative,
            parts.digits,
            parts.scale,
            parts.storage_scale,
        ))
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    /// Value-based order, ignoring scale (`1.5` and `1.50` are equal): a
    /// sign mismatch decides it outright (zero is always non-negative, so a
    /// mismatch means one side is genuinely nonzero); otherwise the aligned
    /// magnitudes decide, reversed when both are negative.
    ///
    /// Allocation-free: sorts, hash-join probes and range filters compare
    /// decimals once per row, where the previous pad-both-sides-into-`String`
    /// form cost four heap allocations Go's word-wise `MyDecimal.Compare`
    /// does not have.
    fn cmp(&self, other: &Self) -> Ordering {
        let left = self.0.as_ref();
        let right = other.0.as_ref();
        native_decimal_cmp(
            NativeDecimalCmpParts {
                negative: left.negative,
                digits: self.digits(),
                storage_scale: left.storage_scale,
            },
            NativeDecimalCmpParts {
                negative: right.negative,
                digits: other.digits(),
                storage_scale: right.storage_scale,
            },
        )
    }
}

/// Right-pads an unsigned digit string with trailing zero digits to extend it
/// from `scale` to `target` fractional digits — exact, since a trailing
/// fractional zero never changes the value.
fn pad_scale(digits: &str, scale: u32, target: u32) -> String {
    tidb_query_datatype::codec::mysql::native_decimal_pad_scale(digits, scale, target)
}

/// MyDecimal stores fractional digits in base-1e9 words. A division result's
/// hidden arithmetic precision therefore rounds up to a whole nine-digit word
/// even when its SQL-visible `resultFrac` is smaller.
fn word_scale(scale: u32) -> u32 {
    scale.div_ceil(9) * 9
}

/// Left-pads two unsigned digit strings with `0` to equal length, so they can
/// be compared or added digit-by-digit.
fn pad_equal(a: &str, b: &str) -> (String, String) {
    let len = a.len().max(b.len());
    (format!("{a:0>len$}"), format!("{b:0>len$}"))
}

/// Numerically compares two unsigned decimal digit strings of possibly
/// different lengths (equal-length numeral strings compare lexicographically
/// = numerically).
fn digit_cmp(a: &str, b: &str) -> Ordering {
    let (a, b) = pad_equal(a, b);
    a.cmp(&b)
}

/// Adds unsigned coefficients, retaining the original leading-zero width.
fn digit_add(a: &str, b: &str) -> String {
    shared_coefficient_binary(a, b, NativeDecimalBinaryOp::Add)
}

fn shared_coefficient_binary(a: &str, b: &str, operation: NativeDecimalBinaryOp) -> String {
    String::from_utf8(
        native_decimal_coefficient_binary(a.as_bytes(), b.as_bytes(), operation, usize::MAX)
            .expect("shared native coefficient arithmetic failed"),
    )
    .expect("shared coefficient digits are ASCII")
}

/// Subtracts unsigned `b` from unsigned `a`, assuming `a >= b` (the caller
/// compares magnitudes first via `digit_cmp` and picks the operand order).
fn digit_sub(a: &str, b: &str) -> String {
    shared_coefficient_binary(a, b, NativeDecimalBinaryOp::Subtract)
}

/// Multiplies unsigned coefficients, returning canonical digits.
fn digit_mul(a: &str, b: &str) -> String {
    shared_coefficient_binary(a, b, NativeDecimalBinaryOp::Multiply)
}

/// Strips leading zero digits, collapsing an all-zero string to `"0"`.
fn strip_leading_zeros(s: &str) -> String {
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

pub(crate) mod codec;

use codec::{MyDecimalWords, CODEC_POWERS10, CODEC_WORD_BUF_LEN, DIGITS_PER_WORD};

pub use codec::{decimal_bin_size, DecimalCodecError, DecimalCodecFailure, DecimalCodecWarning};

#[cfg(test)]
mod shared_exact_integer_division_tests {
    use super::Decimal;

    #[test]
    fn exact_division_facade_keeps_value_scales_signs_and_fresh_shape() {
        for (left_negative, right_negative, expected_negative) in [
            (false, false, false),
            (true, false, true),
            (false, true, true),
            (true, true, false),
        ] {
            let left = Decimal::from_raw_parts(left_negative, b"5250".to_vec(), 1, 3)
                .with_declared_shape(20, 1);
            let right = Decimal::from_raw_parts(right_negative, b"200".to_vec(), 2, 2)
                .with_declared_shape(20, 2);
            let (quotient, remainder) = left.div_rem_unbounded(&right).unwrap();
            assert_eq!(quotient.coefficient_digits(), "2");
            assert_eq!((quotient.storage_scale(), quotient.scale()), (0, 0));
            assert_eq!(quotient.is_negative(), expected_negative);
            assert_eq!(remainder.coefficient_digits(), "1250");
            assert_eq!((remainder.storage_scale(), remainder.scale()), (3, 2));
            assert_eq!(remainder.is_negative(), left_negative);
            assert_eq!(quotient.declared_shape(), None);
            assert_eq!(remainder.declared_shape(), None);
        }
    }

    #[test]
    fn exact_division_facade_keeps_zero_short_circuit_and_i64_narrowing() {
        let one = Decimal::from_int(1);
        let zero = Decimal::from_raw_parts(true, b"0000".to_vec(), 2, 3);
        let invalid = Decimal::from_raw_parts(false, vec![0xff], 0, 0);
        assert!(invalid.div_rem_unbounded(&zero).is_none());
        assert!(invalid.div_rem(&zero).is_none());
        assert!(std::panic::catch_unwind(|| invalid.div_rem_unbounded(&one)).is_err());
        let (quotient, remainder) = zero.div_rem_unbounded(&one).unwrap();
        assert_eq!(quotient.coefficient_digits(), "0");
        assert_eq!((quotient.storage_scale(), quotient.scale()), (0, 0));
        assert_eq!((remainder.storage_scale(), remainder.scale()), (3, 2));
        assert!(!quotient.is_negative() && !remainder.is_negative());
        let minimum = Decimal::from_int(i64::MIN);
        assert_eq!(minimum.div_rem(&one).unwrap().0, i64::MIN);
        let minus_one = Decimal::from_int(-1);
        assert!(minimum.div_rem(&minus_one).is_none());
        let (quotient, remainder) = minimum.div_rem_unbounded(&minus_one).unwrap();
        assert_eq!(quotient.coefficient_digits(), "9223372036854775808");
        assert_eq!((quotient.storage_scale(), quotient.scale()), (0, 0));
        assert!(!quotient.is_negative());
        assert!(remainder.is_zero());
    }
}

#[cfg(test)]
mod native_mysql_division_tests {
    use super::{Decimal, DecimalCodecWarning};

    #[test]
    fn native_mysql_division_facade_keeps_source_shape_and_disposition() {
        let cases = [
            (
                Decimal::from_int(1),
                Decimal::from_int(3),
                "333333333".to_owned(),
                9,
                4,
                false,
                None,
            ),
            (
                Decimal::from_test_parts(true, "15345", 1, 3),
                Decimal::from_int(5),
                "3069000000".to_owned(),
                9,
                5,
                true,
                None,
            ),
            (
                Decimal::from_test_parts(true, &"9".repeat(90), 0, 0),
                Decimal::from_int(1),
                "9".repeat(81),
                0,
                0,
                true,
                Some(DecimalCodecWarning::Overflow),
            ),
            (
                Decimal::from_test_parts(
                    false,
                    &format!("1{}12345678901234567890", "0".repeat(80)),
                    20,
                    20,
                ),
                Decimal::from_int(1),
                format!("1{}123456789012345678900000", "0".repeat(80)),
                24,
                24,
                false,
                Some(DecimalCodecWarning::Truncated),
            ),
            (
                Decimal::new_with_storage_preserving_zero_sign(true, "000000".to_owned(), 6, 6),
                Decimal::from_int(3),
                "0".repeat(10),
                10,
                10,
                false,
                None,
            ),
            (
                Decimal::from_test_parts(true, "1", 0, 90),
                Decimal::from_int(3),
                "0".repeat(81),
                81,
                4,
                false,
                Some(DecimalCodecWarning::Truncated),
            ),
        ];
        for (left, right, digits, storage, visible, negative, warning) in cases {
            let left = left.with_declared_shape(120, 30);
            let (output, actual_warning) = left.div_mysql_with_warning(&right, 4).unwrap();
            assert_eq!(actual_warning, warning);
            assert_eq!(output.coefficient_digits(), digits);
            assert_eq!((output.storage_scale(), output.scale()), (storage, visible));
            assert_eq!(output.is_negative(), negative);
            assert_eq!(output.declared_shape(), None);
            assert_eq!(
                left.div_mysql(&right, 4).unwrap().coefficient_digits(),
                digits
            );
        }
        // A genuinely zero full-precision quotient bypasses the result bound.
        let tiny = Decimal::from_test_parts(true, "1", 0, 90);
        let (zero, warning) = tiny
            .div_mysql_with_warning(&Decimal::from_int(3), 0)
            .unwrap();
        assert_eq!(warning, None);
        assert_eq!((zero.storage_scale(), zero.scale()), (90, 0));
        assert!(zero.is_zero() && !zero.is_negative());
        assert!(tiny
            .div_mysql_with_warning(&Decimal::from_int(0), u32::MAX)
            .is_none());
    }
}

#[cfg(test)]
mod native_remainder_tests {
    use super::Decimal;

    #[test]
    fn native_remainder_facade_preserves_source_scales_sign_and_wide_values() {
        let wide = format!("{}12345", "9".repeat(108));
        let cases = [
            (
                Decimal::from_test_parts(true, "15345", 1, 3),
                Decimal::from_test_parts(true, "21000", 2, 4),
                "6450".to_owned(),
                4,
                2,
                true,
            ),
            (
                Decimal::from_test_parts(false, "15345", 1, 3),
                Decimal::from_test_parts(true, "21000", 2, 4),
                "6450".to_owned(),
                4,
                2,
                false,
            ),
            (
                Decimal::from_test_parts(true, "40", 0, 1),
                Decimal::from_test_parts(true, "2000", 2, 3),
                "000".to_owned(),
                3,
                2,
                false,
            ),
            (
                Decimal::from_int(1),
                Decimal::from_test_parts(false, "1", 0, 3),
                "000".to_owned(),
                3,
                0,
                false,
            ),
            (
                Decimal::from_test_parts(true, &wide, 2, 5),
                Decimal::from_test_parts(false, &format!("1{}", "0".repeat(116)), 4, 7),
                format!("{wide}00"),
                7,
                4,
                true,
            ),
            (
                Decimal::from_test_parts(false, &format!("1{}1", "0".repeat(107)), 0, 0),
                Decimal::from_int(2),
                "1".to_owned(),
                0,
                0,
                false,
            ),
            (
                Decimal::new_with_storage_preserving_zero_sign(true, "000".to_owned(), 3, 3),
                Decimal::from_test_parts(false, "20000", 2, 4),
                "0000".to_owned(),
                4,
                3,
                false,
            ),
        ];
        for (left, right, digits, storage, visible, negative) in cases {
            let output = left.with_declared_shape(120, 4).rem_mysql(&right).unwrap();
            assert_eq!(output.coefficient_digits(), digits);
            assert_eq!((output.storage_scale(), output.scale()), (storage, visible));
            assert_eq!(output.is_negative(), negative);
            assert_eq!(output.declared_shape(), None);
        }
        let zero = Decimal::new_with_storage_preserving_zero_sign(true, "000".to_owned(), 3, 3);
        assert!(Decimal::from_int(1).rem_mysql(&zero).is_none());
    }
}

#[cfg(test)]
mod native_binary_tests {
    use super::{digit_add, digit_mul, digit_sub, Decimal, MyDecimalWords};

    #[test]
    fn native_binary_facades_keep_hidden_scales_and_coefficient_consumers() {
        let hidden = Decimal::from_test_parts(false, "1", 0, 40).with_declared_shape(40, 0);
        let (product, warning) = hidden.mul_mysql(&hidden);
        assert_eq!(warning, None); // unsigned-i128 eligibility precedes nine-word projection
        assert_eq!((product.storage_scale(), product.scale()), (80, 0));
        assert_eq!(product.coefficient_digits(), format!("{}1", "0".repeat(79)));
        assert_eq!(product.declared_shape(), None);
        let wide = Decimal::from_test_parts(false, &"1".repeat(90), 0, 0);
        let (zero, warning) = wide.sub_mysql(&wide);
        assert_eq!(warning, None);
        assert_eq!(zero.coefficient_digits(), "0");
        assert!(!zero.is_negative());
        let projected = MyDecimalWords::from_decimal(&wide);
        assert_eq!(projected.digits_int, 81);
        assert_eq!(projected.word_buf, [111111111; 9]);
        assert_eq!(digit_add("0099", "1"), "0100");
        assert_eq!(digit_sub("0100", "1"), "0099");
        assert_eq!(digit_mul("0099", "01"), "99");
    }
}

#[cfg(test)]
mod native_negate_tests {
    use super::Decimal;

    #[test]
    fn native_negate_facade_preserves_coefficient_and_hidden_scale() {
        // Fixed sign expectations; original ordinary-value construction clears
        // declared shape and negative zero without changing retained digits.
        let wide_digits = format!("{}{}", "9".repeat(108), "1".repeat(120));
        let inputs = [
            (
                Decimal::from_test_parts(false, "333333333", 4, 9).with_declared_shape(12, 4),
                true,
            ),
            (
                Decimal::from_test_parts(true, &wide_digits, 31, 120).with_declared_shape(228, 120),
                false,
            ),
            (
                Decimal::new_with_storage_preserving_zero_sign(true, "000".to_owned(), 3, 3),
                false,
            ),
        ];
        for (input, expected_negative) in inputs {
            let result = input.negate();
            assert_eq!(result.is_negative(), expected_negative);
            assert_eq!(result.coefficient_digits(), input.coefficient_digits());
            assert_eq!(result.scale(), input.scale());
            assert_eq!(result.storage_scale(), input.storage_scale());
            assert_eq!(result.declared_shape(), None);
        }
    }
}

#[cfg(test)]
mod native_math_bridge_tests {
    use super::{Decimal, NativeDecimalError};
    use tidb_query_datatype::codec::mysql::decimal::NativeDecimalOp;

    #[test]
    fn native_math_bridge_keeps_wide_hidden_scales_and_clears_shape() {
        let wide_digits = format!("{}{}", "9".repeat(108), "1".repeat(120));
        let values = [
            Decimal::from_test_parts(false, "333333333", 4, 9).with_declared_shape(12, 4),
            Decimal::from_test_parts(true, &wide_digits, 31, 120).with_declared_shape(228, 120),
            Decimal::new_with_storage_preserving_zero_sign(true, "000".to_owned(), 3, 3),
        ];
        for original in &values {
            let shared = original.try_to_shared_math(usize::MAX).unwrap();
            assert_eq!(shared.storage_scale(), original.storage_scale());
            assert_eq!(shared.result_scale(), original.scale());
            let owned = shared.try_clone_native_math(usize::MAX).unwrap();
            let restored = Decimal::try_from_shared_math(&owned, usize::MAX).unwrap();
            assert_eq!(restored.coefficient_digits(), original.coefficient_digits());
            assert_eq!(restored.is_negative(), original.is_negative());
            assert_eq!(restored.storage_scale(), original.storage_scale());
            assert_eq!(restored.scale(), original.scale());
            assert_eq!(restored.declared_shape(), None);
        }
        let input = Decimal::from_literal("-15.5").with_declared_shape(10, 1);
        for (value, digits, scale) in [
            (input.abs(), "155", 1),
            (input.ceil_floor(true), "15", 0),
            (input.ceil_floor(false), "16", 0),
            (input.round_to_scale(0), "16", 0),
            (input.truncate_to_scale(0), "15", 0),
        ] {
            assert_eq!(value.coefficient_digits(), digits);
            assert_eq!((value.storage_scale(), value.scale()), (scale, scale));
            assert_eq!(value.declared_shape(), None);
        }
        let retained = input.round_or_truncate_to_scale_with_storage(0, true, 4);
        assert_eq!(retained.coefficient_digits(), "160000");
        assert_eq!((retained.storage_scale(), retained.scale()), (4, 0));
        assert!(retained.is_negative());
        assert_eq!(retained.declared_shape(), None);
        let shared = values[1].try_to_shared_math(usize::MAX).unwrap();
        assert!(shared.words().words.len() > 9);
        assert!(matches!(
            Decimal::try_from_shared_math(&shared, 1),
            Err(NativeDecimalError::Resource(_))
        ));
        let negative_zero = values[2].try_to_shared_math(usize::MAX).unwrap();
        for operation in [
            NativeDecimalOp::Abs,
            NativeDecimalOp::Round(3),
            NativeDecimalOp::Truncate(0),
        ] {
            let result = negative_zero.try_native_math(operation, 1024).unwrap();
            let result = Decimal::try_from_shared_math(&result, 1024).unwrap();
            assert!(result.is_zero());
            assert!(!result.is_negative());
            assert_eq!(result.declared_shape(), None);
        }
    }
}

#[cfg(test)]
mod shared_raw_integer_projection_tests {
    use super::{Decimal, DecimalIntegerWarning};

    #[test]
    fn raw_integer_facades_ignore_visible_shape_and_preserve_signed_parser() {
        use DecimalIntegerWarning::{Overflow, Truncated};

        let value = Decimal::from_raw_parts(false, b"1".to_vec(), u32::MAX, 0)
            .with_declared_shape(i64::MIN, i64::MAX);
        assert_eq!(value.coefficient_i128(), Some((1, 0)));
        assert_eq!(value.to_i64_trunc(), (1, None));
        assert_eq!(value.to_u64_trunc(), (1, None));
        let hidden = Decimal::from_raw_parts(true, b"12340".to_vec(), 1, 3);
        assert_eq!(hidden.coefficient_i128(), Some((-12340, 3)));
        assert_eq!(hidden.to_i64_trunc(), (-12, Some(Truncated)));
        assert_eq!(hidden.to_u64_trunc(), (0, Some(Overflow)));
        let zero = Decimal::from_raw_parts(true, b"000".to_vec(), 1, 3);
        assert_eq!(zero.coefficient_i128(), Some((0, 3)));
        assert_eq!(zero.to_i64_trunc(), (0, None));
        assert_eq!(zero.to_u64_trunc(), (0, Some(Overflow)));
        let raw_minimum = b"-170141183460469231731687303715884105728".to_vec();
        assert_eq!(
            Decimal::from_raw_parts(false, raw_minimum.clone(), 0, 9).coefficient_i128(),
            Some((i128::MIN, 9))
        );
        assert_eq!(
            Decimal::from_raw_parts(true, raw_minimum, 0, 9).coefficient_i128(),
            None
        );
        assert_eq!(
            Decimal::from_raw_parts(
                true,
                b"170141183460469231731687303715884105728".to_vec(),
                0,
                0
            )
            .coefficient_i128(),
            None
        );
        let overflow = Decimal::from_raw_parts(false, b"18446744073709551616x".to_vec(), 0, 1);
        assert_eq!(overflow.to_i64_trunc(), (i64::MAX, Some(Overflow)));
        assert_eq!(overflow.to_u64_trunc(), (u64::MAX, Some(Overflow)));
        let fraction = Decimal::from_raw_parts(false, "é".as_bytes().to_vec(), 0, 2);
        assert_eq!(fraction.to_i64_trunc(), (0, Some(Truncated)));
        assert_eq!(fraction.to_u64_trunc(), (0, Some(Truncated)));
    }

    #[test]
    fn raw_integer_facades_keep_utf8_scale_and_slice_panic_domains() {
        use std::panic::catch_unwind;

        for (digits, scale) in [
            (b"\xff".as_slice(), 0),
            (b"1".as_slice(), 2),
            ("é".as_bytes(), 1),
        ] {
            let value = Decimal::from_raw_parts(false, digits.to_vec(), 0, scale);
            assert!(catch_unwind(|| value.to_i64_trunc()).is_err());
            assert!(catch_unwind(|| value.to_u64_trunc()).is_err());
            let negative = Decimal::from_raw_parts(true, digits.to_vec(), 0, scale);
            assert_eq!(
                negative.to_u64_trunc(),
                (0, Some(DecimalIntegerWarning::Overflow))
            );
        }
        let invalid = Decimal::from_raw_parts(false, vec![0xff], 0, 0);
        let panic = catch_unwind(|| invalid.coefficient_i128()).unwrap_err();
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap();
        assert!(message.starts_with("decimal coefficients are ASCII digits"));
    }
}

#[cfg(test)]
mod presentation_bridge_tests {
    use super::{Decimal, SharedDecimal};

    #[test]
    fn shared_visible_format_preserves_raw_and_hidden_decimal_representation() {
        for (negative, digits, scale, storage, expected) in [
            (false, b"00123".as_slice(), 2, 2, "001.23"),
            (true, b"000".as_slice(), 2, 2, "-0.00"),
            (true, b"0000".as_slice(), 2, 4, "0.00"),
            (false, b"".as_slice(), 0, 0, "0"),
            (true, b"".as_slice(), 0, 0, "-0"),
            (false, b"0001".as_slice(), 0, 0, "0001"),
            (false, b"123".as_slice(), 3, 3, "0.123"),
            (false, b"12x".as_slice(), 1, 1, "12.x"),
            (false, b"10049".as_slice(), 2, 4, "1.00"),
            (true, b"10050".as_slice(), 2, 4, "-1.01"),
            (false, b"99995".as_slice(), 2, 4, "10.00"),
        ] {
            let value = Decimal::from_raw_parts(negative, digits.to_vec(), scale, storage)
                .with_declared_shape(17, 3);
            assert_eq!(value.to_string(), expected);
            assert_eq!(value.coefficient_bytes(), digits);
            assert_eq!(value.is_negative(), negative);
            assert_eq!((value.scale(), value.storage_scale()), (scale, storage));
            assert_eq!(value.declared_shape(), Some((17, 3)));
        }
        // to_f64 still parses SQL-visible rounded text, not the hidden
        // coefficient. The raw zero sign remains observable without rounding.
        let hidden = Decimal::from_raw_parts(false, b"10049".to_vec(), 2, 4);
        assert_eq!(hidden.to_f64().to_bits(), 1.0_f64.to_bits());
        let raw_zero = Decimal::from_raw_parts(true, b"000".to_vec(), 2, 2);
        assert_eq!(raw_zero.to_f64().to_bits(), (-0.0_f64).to_bits());
        for (digits, scale, storage) in [
            (b"\xff".as_slice(), 0, 0),
            (b"1".as_slice(), 2, 2),
            (b"123".as_slice(), 2, 1),
            ("é".as_bytes(), 1, 1),
        ] {
            let value = Decimal::from_raw_parts(false, digits.to_vec(), scale, storage);
            assert!(std::panic::catch_unwind(|| value.to_string()).is_err());
        }
    }

    #[test]
    fn shared_float_facade_keeps_go_g_text_and_existing_mysql_parser_projection() {
        // These are pinned Go-g spellings, not strings computed from the
        // result under test. Keep the native parser as an independent existing
        // source projection; this change does not migrate that parser.
        for (value, go_text) in [
            (0.0, "0"),
            (-0.0, "0"),
            (1.25, "1.25"),
            (-1.25, "-1.25"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (999999.0, "999999"),
            (1000000.0, "1e+06"),
            (1000001.0, "1.000001e+06"),
            (1.0e20, "1e+20"),
            (f64::from_bits(1), "5e-324"),
            (-f64::from_bits(1), "-5e-324"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (f64::MAX, "1.7976931348623157e+308"),
            (-f64::MAX, "-1.7976931348623157e+308"),
            (f64::from(0.1_f32), "0.10000000149011612"),
            (1.00000049, "1.00000049"),
        ] {
            assert_eq!(
                SharedDecimal::native_format_go_shortest_float(value),
                go_text
            );
            let expected = Decimal::from_signed_literal(go_text);
            let actual = Decimal::from_f64(value).unwrap();
            assert_eq!(
                actual.coefficient_bytes(),
                expected.coefficient_bytes(),
                "{go_text}"
            );
            assert_eq!(actual.is_negative(), expected.is_negative(), "{go_text}");
            assert_eq!(
                (actual.scale(), actual.storage_scale()),
                (expected.scale(), expected.storage_scale()),
                "{go_text}"
            );
            assert_eq!(actual.declared_shape(), None);
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(Decimal::from_f64(value).is_none());
        }
        assert_eq!(Decimal::from_f64(-0.0).unwrap().to_string(), "0");
        assert_eq!(
            Decimal::from_f64(f64::from_bits(1)).unwrap().to_string(),
            "0"
        );
        assert_eq!(
            Decimal::from_f64(f64::MIN_POSITIVE).unwrap().to_string(),
            "0"
        );
        assert_eq!(
            Decimal::from_f64(f64::MAX).unwrap().to_string(),
            "9".repeat(81)
        );
        assert_eq!(
            Decimal::from_f64(-f64::MAX).unwrap().to_string(),
            format!("-{}", "9".repeat(81))
        );
        assert_eq!(
            Decimal::from_f64(f64::from(0.1_f32)).unwrap().to_string(),
            "0.10000000149011612"
        );
    }
}

#[cfg(test)]
mod raw_representation_tests {
    use super::Decimal;

    #[test]
    fn raw_decimal_parts_keep_sign_scales_shape_and_arbitrary_bytes() {
        for (negative, digits, scale, storage_scale, shape) in [
            (true, b"0000".as_slice(), 2, 4, (0, 0)),
            (false, b"00123".as_slice(), 1, 3, (-1, i64::MAX)),
            (true, b"\xff\0\xc3".as_slice(), u32::MAX, 0, (i64::MIN, -7)),
            (false, b"".as_slice(), 0, u32::MAX, (10, 2)),
        ] {
            let value = Decimal::from_raw_parts(negative, digits.to_vec(), scale, storage_scale);
            assert_eq!(value.coefficient_bytes(), digits);
            assert_eq!(value.is_negative(), negative);
            assert_eq!(value.scale(), scale);
            assert_eq!(value.storage_scale(), storage_scale);
            assert_eq!(value.declared_shape(), None);
            let shaped = value.with_declared_shape(shape.0, shape.1);
            assert_eq!(shaped.declared_shape(), Some(shape));
            assert_eq!(shaped.coefficient_bytes(), digits);
            assert_eq!(shaped.is_negative(), negative);
            assert_eq!(
                (shaped.scale(), shaped.storage_scale()),
                (scale, storage_scale)
            );
        }
    }
}

#[cfg(test)]
#[test]
fn shared_precision_cast_preserves_requested_scale_and_value_metadata() {
    // Fixed source rules, including malformed target behavior: clamp compares
    // only the rounded integer width with flen.saturating_sub(requested_scale).
    for (input, flen, scale, expected, result_scale) in [
        ("2.5", 2, 0, "3", 0),
        ("-2.5", 2, 0, "-3", 0),
        ("99.994", 4, 2, "99.99", 2),
        ("99.995", 4, 2, "99.99", 2),
        ("99.995", 0, 2, "100.00", 2),
        ("-99.995", 4, 2, "-99.99", 2),
        ("1.23", 1, 2, "0.09", 2),
        ("0.12", 1, 2, "0.12", 2),
        ("1.23", 2, 2, "0.99", 2),
        ("0.00", 1, 2, "0.00", 2),
        ("12344", 0, u32::MAX, "12340", 0),
        ("12344", 0, u32::MAX - 1, "12300", 0),
        ("1.234", u32::MAX, 2, "1.23", 2),
    ] {
        let source = Decimal::from_literal(input).with_declared_shape(20, 7);
        let value = source.cast_to_precision(flen, scale);
        assert_eq!(value.to_string(), expected, "{input} ({flen},{scale})");
        assert_eq!(
            (value.scale(), value.storage_scale()),
            (result_scale, result_scale)
        );
        assert_eq!(value.declared_shape(), None);
        assert_eq!(source.declared_shape(), Some((20, 7)));
    }
    for (negative, digits, expected_digits, expected_negative) in [
        (false, b"00012499".as_slice(), b"125".as_slice(), false),
        (true, b"00012499".as_slice(), b"125".as_slice(), true),
        (true, b"00000000".as_slice(), b"00".as_slice(), false),
    ] {
        let source = Decimal::from_raw_parts(negative, digits.to_vec(), 1, 4)
            .with_declared_shape(i64::MIN, i64::MAX);
        let value = source.cast_to_precision(0, 2);
        assert_eq!(value.coefficient_bytes(), expected_digits);
        assert_eq!(value.is_negative(), expected_negative);
        assert_eq!((value.scale(), value.storage_scale()), (2, 2));
        assert_eq!(value.declared_shape(), None);
        assert_eq!(source.coefficient_bytes(), digits);
        assert_eq!(source.is_negative(), negative);
        assert_eq!((source.scale(), source.storage_scale()), (1, 4));
        assert_eq!(source.declared_shape(), Some((i64::MIN, i64::MAX)));
    }
    // This pure value API does not introduce SQL's precision/scale caps or
    // route the coefficient through the fixed nine-word decimal constructor.
    let digits = "1234567890".repeat(9);
    let wide = Decimal::from_literal(&digits).with_declared_shape(120, 0);
    for (flen, expected) in [(100, format!("{digits}00")), (90, "9".repeat(90))] {
        let value = wide.cast_to_precision(flen, 2);
        assert_eq!(value.coefficient_bytes(), expected.as_bytes());
        assert_eq!((value.scale(), value.storage_scale()), (2, 2));
        assert_eq!(value.declared_shape(), None);
    }
}

#[cfg(test)]
#[test]
fn shared_decimal_parse_facade_moves_storage_and_preserves_raw_shift_identity() {
    let raw =
        Decimal::from_raw_parts(true, vec![0xff], 7, 2).with_declared_shape(i64::MIN, i64::MAX);
    let (copied, warning) = raw.shift_mysql_with_word_limit(0, 0);
    assert_eq!(warning, None);
    assert_eq!(copied.coefficient_bytes(), &[0xff]);
    assert!(copied.is_negative());
    assert_eq!((copied.scale(), copied.storage_scale()), (7, 2));
    assert_eq!(copied.declared_shape(), Some((i64::MIN, i64::MAX)));

    let digits = SmallVec::<[u8; INLINE_DECIMAL_DIGITS]>::from_slice(&[b'1'; 90]);
    let allocation = digits.as_ptr();
    let value = Decimal::from_shared_parse(native_decimal_normalize(false, digits, 0, 0, false));
    assert_eq!(value.coefficient_bytes().as_ptr(), allocation);
    assert_eq!(value.coefficient_bytes(), &[b'1'; 90]);
    assert!(Decimal::from_int(i64::MIN).coefficient_is_inline());
    assert!(Decimal::from_uint(u64::MAX).coefficient_is_inline());
    assert!(Decimal::from_scaled_i128(12345, 2).coefficient_is_inline());
}
