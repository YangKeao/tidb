// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{Decimal, DecimalCodecWarning, DecimalDigits};
use crate::{FieldType, MyDecimal};

/// One decimal value of a column-wise arithmetic evaluation, in the shape
/// the value-layer `Decimal` fast paths compute in: a signed `i128`
/// coefficient at `storage_scale` fraction digits, of which `scale` are the
/// SQL-visible result scale.
#[derive(Debug, Clone, Copy)]
pub struct CompactDecimal {
    coefficient: i128,
    storage_scale: u32,
    scale: u32,
}

impl CompactDecimal {
    #[inline]
    pub fn from_my_decimal(value: &MyDecimal) -> Option<Self> {
        // `Decimal::from_my_decimal`: the stored fraction digits are padded
        // up to the result scale when the cell declares more of them.
        let (coefficient, digits_frac) = value.to_i128_scaled()?;
        let result_frac = u32::try_from(value.result_frac()).ok()?;
        let storage_scale = digits_frac.max(result_frac);
        let coefficient = if storage_scale > digits_frac {
            coefficient.checked_mul(10i128.checked_pow(storage_scale - digits_frac)?)?
        } else {
            coefficient
        };
        Some(Self {
            coefficient,
            storage_scale,
            scale: result_frac,
        })
    }

    /// Constructs an exact integer; native signedness is resolved by the caller.
    #[inline]
    pub fn from_integer(coefficient: i128) -> Self {
        Self {
            coefficient,
            storage_scale: 0,
            scale: 0,
        }
    }

    /// Constructs an exactly scaled integer for the existing native cast admission.
    #[inline]
    pub fn from_scaled_integer(coefficient: i128, scale: u32) -> Option<Self> {
        Some(Self {
            coefficient: coefficient.checked_mul(10i128.checked_pow(scale)?)?,
            storage_scale: scale,
            scale,
        })
    }

    pub(super) fn from_decimal(value: &Decimal) -> Option<Self> {
        let (coefficient, storage_scale) = value.coefficient_i128()?;
        Some(Self {
            coefficient,
            storage_scale,
            scale: value.scale(),
        })
    }

    pub(super) fn into_decimal(self) -> Decimal {
        Decimal::new_with_storage(
            self.coefficient < 0,
            DecimalDigits::from_unsigned(self.coefficient.unsigned_abs()),
            self.scale,
            self.storage_scale,
        )
    }

    /// `Decimal::add` (`try_add_fast`): operands aligned to the wider storage
    /// scale, the visible scale the wider of the two.
    #[inline]
    pub fn add(self, other: Self) -> Option<Self> {
        let storage_scale = self.storage_scale.max(other.storage_scale);
        let left = self.aligned(storage_scale)?;
        let right = other.aligned(storage_scale)?;
        Some(Self {
            coefficient: left.checked_add(right)?,
            storage_scale,
            scale: self.scale.max(other.scale),
        })
    }

    #[inline]
    pub fn sub(self, other: Self) -> Option<Self> {
        self.add(Self {
            coefficient: other.coefficient.checked_neg()?,
            ..other
        })
    }

    /// `Decimal::mul_mysql` (`try_mul_mysql_fast`): scales add; a result
    /// scale past MySQL's 30 takes the general path.
    #[inline]
    pub fn mul(self, other: Self) -> Option<Self> {
        let scale = self.scale.checked_add(other.scale)?;
        if scale > 30 {
            return None;
        }
        Some(Self {
            coefficient: self.coefficient.checked_mul(other.coefficient)?,
            storage_scale: self.storage_scale.checked_add(other.storage_scale)?,
            scale,
        })
    }

    pub(super) fn add_decimal(self, other: Self) -> Option<Self> {
        let storage_scale = self.storage_scale.max(other.storage_scale);
        // Original Decimal aligns unsigned magnitudes before applying signs.
        if self.aligned(storage_scale)? == i128::MIN || other.aligned(storage_scale)? == i128::MIN {
            return None;
        }
        self.add(other)
    }

    pub(super) fn mul_decimal(self, other: Self) -> Option<Self> {
        // Preserve the original unsigned-magnitude i128 fast-path bound.
        i128::try_from(
            self.coefficient
                .unsigned_abs()
                .checked_mul(other.coefficient.unsigned_abs())?,
        )
        .ok()?;
        self.mul(other)
    }

    fn aligned(self, storage_scale: u32) -> Option<i128> {
        if storage_scale == self.storage_scale {
            Some(self.coefficient)
        } else {
            self.coefficient
                .checked_mul(10i128.checked_pow(storage_scale - self.storage_scale)?)
        }
    }

    /// `Decimal::to_chunk_my_decimal`: the cell the row path appends.
    #[inline]
    pub fn to_my_decimal(self) -> Option<MyDecimal> {
        MyDecimal::from_scaled_i128(self.coefficient, self.storage_scale, self.scale)
    }
}

/// Full arithmetic value owned by the original datatype package. Compact
/// representation is private and never narrows a wide or invalid Decimal.
#[derive(Clone, Debug)]
pub struct DecimalArithmeticValue {
    value: ArithmeticValue,
}

#[derive(Clone, Debug)]
enum ArithmeticValue {
    Compact {
        value: CompactDecimal,
        declared_shape: Option<(i64, i64)>,
    },
    Wide(Decimal),
}

impl From<Decimal> for DecimalArithmeticValue {
    fn from(value: Decimal) -> Self {
        let declared_shape = value.declared_shape();
        let compact = if valid_decimal(&value) && !(value.is_zero() && value.is_negative()) {
            CompactDecimal::from_decimal(&value).filter(|v| v.storage_scale <= 38)
        } else {
            None
        };
        Self {
            value: compact.map_or_else(
                || ArithmeticValue::Wide(value),
                |value| ArithmeticValue::Compact {
                    value,
                    declared_shape,
                },
            ),
        }
    }
}

fn valid_decimal(value: &Decimal) -> bool {
    let coefficient = value.coefficient_digits().as_bytes();
    !coefficient.is_empty()
        && coefficient.iter().all(u8::is_ascii_digit)
        && coefficient.len() >= value.storage_scale() as usize
        && value.scale() <= value.storage_scale()
}

pub(super) fn column_shape(value: &MyDecimal, field: &FieldType) -> (i64, i64) {
    (
        field.flen(),
        if field.decimal() == crate::UNSPECIFIED_LENGTH {
            i64::from(value.digits_frac())
        } else {
            field.decimal()
        },
    )
}

impl DecimalArithmeticValue {
    /// The same column shape conversion used by the ordinary Datum reader.
    pub fn from_column(value: &MyDecimal, field: &FieldType) -> Self {
        let value = CompactDecimal::from_my_decimal(value)
            .filter(|v| v.storage_scale <= 38 && v.coefficient != i128::MIN)
            .map_or_else(
                || ArithmeticValue::Wide(Decimal::from_column(value, field)),
                |compact| ArithmeticValue::Compact {
                    value: compact,
                    declared_shape: Some(column_shape(value, field)),
                },
            );
        Self { value }
    }

    /// Compact values are valid by construction; no coefficient text is needed.
    pub fn is_valid(&self) -> bool {
        match &self.value {
            ArithmeticValue::Compact { .. } => true,
            ArithmeticValue::Wide(v) => valid_decimal(v),
        }
    }

    pub fn into_decimal(self) -> Decimal {
        match self.value {
            ArithmeticValue::Compact {
                value,
                declared_shape,
            } => {
                let value = value.into_decimal();
                match declared_shape {
                    Some((flen, frac)) => value.with_declared_shape(flen, frac),
                    None => value,
                }
            }
            ArithmeticValue::Wide(value) => value,
        }
    }

    fn decimal(&self) -> std::borrow::Cow<'_, Decimal> {
        match &self.value {
            ArithmeticValue::Wide(value) => std::borrow::Cow::Borrowed(value),
            ArithmeticValue::Compact { .. } => std::borrow::Cow::Owned(self.clone().into_decimal()),
        }
    }

    pub fn is_zero(&self) -> bool {
        match &self.value {
            ArithmeticValue::Compact { value, .. } => value.coefficient == 0,
            ArithmeticValue::Wide(v) => v.is_zero(),
        }
    }

    pub fn add_mysql(&self, other: &Self) -> (Self, Option<DecimalCodecWarning>) {
        let (v, w) = self.decimal().add_mysql(&other.decimal());
        (v.into(), w)
    }
    pub fn sub_mysql(&self, other: &Self) -> (Self, Option<DecimalCodecWarning>) {
        let (v, w) = self.decimal().sub_mysql(&other.decimal());
        (v.into(), w)
    }
    pub fn mul_mysql(&self, other: &Self) -> (Self, Option<DecimalCodecWarning>) {
        if let (
            ArithmeticValue::Compact { value: a, .. },
            ArithmeticValue::Compact { value: b, .. },
        ) = (&self.value, &other.value)
        {
            if let Some(value) = a.mul_decimal(*b).filter(|v| v.storage_scale <= 38) {
                return (
                    Self {
                        value: ArithmeticValue::Compact {
                            value,
                            declared_shape: None,
                        },
                    },
                    None,
                );
            }
        }
        let (v, w) = self.decimal().mul_mysql(&other.decimal());
        (v.into(), w)
    }
    pub fn div_mysql_with_warning(
        &self,
        other: &Self,
        increment: u32,
    ) -> Option<(Self, Option<DecimalCodecWarning>)> {
        self.decimal()
            .div_mysql_with_warning(&other.decimal(), increment)
            .map(|(v, w)| (v.into(), w))
    }

    /// Matches the ordinary Datum-to-Chunk boundary including wide truncation.
    pub fn to_chunk_my_decimal_lossy(&self) -> MyDecimal {
        if let ArithmeticValue::Compact { value, .. } = self.value {
            if let Some(value) = value.to_my_decimal() {
                return value;
            }
        }
        self.decimal().to_chunk_my_decimal_lossy()
    }
}

impl std::fmt::Display for DecimalArithmeticValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.decimal().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(value: &Decimal) -> (bool, String, u32, u32, Option<(i64, i64)>) {
        (
            value.is_negative(),
            value.coefficient_digits().into(),
            value.storage_scale(),
            value.scale(),
            value.declared_shape(),
        )
    }

    #[test]
    fn compact_carrier_preserves_parts_and_original_fallbacks() {
        let values = [
            Decimal::from_literal("-123456789012345678901234567890.1234"),
            Decimal::from_literal("123456789012345678901234567890123456789012345"),
            Decimal::from_literal("0.00000000000000000000000000000001"),
            Decimal::from_int(8)
                .true_div(&Decimal::from_int(7), 7)
                .unwrap(),
            Decimal::new_with_storage_preserving_zero_sign(true, "0".to_owned(), 0, 0),
            Decimal::from_int(-7).with_declared_shape(65, -1),
            Decimal::from_literal("-170141183460469231731687303715884105728"),
            Decimal::from_literal("-85070591730234615865843651857942052864"),
            Decimal::from_int(2),
            Decimal::from_literal(&"9".repeat(81)),
        ];
        for value in &values {
            let carrier = DecimalArithmeticValue::from(value.clone());
            assert!(carrier.is_valid());
            assert_eq!(parts(&carrier.clone().into_decimal()), parts(value));
            assert_eq!(
                carrier.to_chunk_my_decimal_lossy(),
                value.to_chunk_my_decimal_lossy()
            );
            for other in &values {
                let rhs = DecimalArithmeticValue::from(other.clone());
                for (actual, expected) in [
                    (carrier.add_mysql(&rhs), value.add_mysql(other)),
                    (carrier.sub_mysql(&rhs), value.sub_mysql(other)),
                    (carrier.mul_mysql(&rhs), value.mul_mysql(other)),
                ] {
                    assert_eq!(actual.1, expected.1);
                    assert_eq!(parts(&actual.0.into_decimal()), parts(&expected.0));
                }
            }
        }
        let invalid = DecimalArithmeticValue::from(Decimal {
            negative: false,
            digits: DecimalDigits(smallvec::SmallVec::from_slice(b"abc")),
            scale: 0,
            storage_scale: 0,
            declared_shape: None,
        });
        assert!(!invalid.is_valid());
        for scale in [-1, 4] {
            let field = FieldType::new(crate::FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(scale);
            for value in &values[..4] {
                let stored = value.to_chunk_my_decimal_lossy();
                assert_eq!(
                    parts(&DecimalArithmeticValue::from_column(&stored, &field).into_decimal()),
                    parts(&Decimal::from_column(&stored, &field))
                );
            }
        }
        assert!(CompactDecimal::from_integer(i128::MAX)
            .mul(CompactDecimal::from_integer(2))
            .is_none());
        assert!(CompactDecimal::from_scaled_integer(1, 39).is_none());
        assert_eq!(
            CompactDecimal::from_scaled_integer(-12, 2)
                .unwrap()
                .into_decimal()
                .to_string(),
            "-12.00"
        );
    }
}
