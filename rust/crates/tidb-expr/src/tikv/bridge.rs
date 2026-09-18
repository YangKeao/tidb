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

//! Exact, checked transport, not SQL coercion. Metadata remains in FieldType.
//! A supported type can still contain an unsupported value shape; those errors
//! must not cause an already evaluated expression to be replayed natively.

use tidb_chunk::{chunk::Chunk, column::ColumnReadView};
use tidb_datatype::{
    BinaryJSON, BinaryLiteral, Datum, Decimal, FieldType, FieldTypeCode, MyDecimal, MySqlDuration,
    MysqlEnum, MysqlSet, Time, TimeType, VectorFloat32,
};
use tidb_query_expr::standalone::{
    date_time_from_chunk, date_time_to_chunk, decimal_from_chunk, decimal_to_chunk,
    json_from_binary, json_to_binary, Column as EngineColumn, Duration as EngineDuration,
    Enum as EngineEnum, Json as EngineJson, Set as EngineSet, VectorFloat32 as EngineVector,
};

use super::{engine_error, invalid};
use crate::EvalError;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Int,
    Real,
    Bytes,
    Decimal,
    DateTime,
    Duration,
    Json,
    Enum,
    Set,
    Vector,
}

fn family(ty: &FieldType) -> Option<Family> {
    use FieldTypeCode::*;
    if ty.is_array() {
        return None;
    }
    Some(match ty.code() {
        Tiny | Short | Int24 | Long | LongLong | Year => Family::Int,
        Bit if (1..=64).contains(&ty.flen()) => Family::Int,
        Float | Double => Family::Real,
        Varchar | VarString | String | Blob | TinyBlob | MediumBlob | LongBlob | Null => {
            Family::Bytes
        }
        NewDecimal => Family::Decimal,
        Date | Datetime | Timestamp if (-1..=6).contains(&ty.decimal()) => Family::DateTime,
        Duration if (-1..=6).contains(&ty.decimal()) => Family::Duration,
        Json => Family::Json,
        Enum => Family::Enum,
        // SET's SQL eval family is String, but the engine carries it as its own
        // input-only type, so the CODEC family is what selects the carrier.
        Set => Family::Set,
        // TiKV's vector payload is native-endian, unlike TiDB's LE chunk image.
        VectorFloat32 if cfg!(target_endian = "little") => Family::Vector,
        // Legacy NEWDATE, GEOMETRY and unspecified/unknown codes have no
        // implemented native engine EvalType/owned carrier.
        _ => return None,
    })
}

pub(super) fn supported_type(ty: &FieldType) -> bool {
    family(ty).is_some()
}

fn shape_error(error: impl std::fmt::Display) -> EvalError {
    invalid(&format!("TiKV value bridge: {error}"))
}

fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], EvalError> {
    bytes
        .try_into()
        .map_err(|_| invalid("TiKV value bridge: invalid cell width"))
}

/// Read without the chunk convenience accessors, which deliberately panic on
/// malformed storage. Check selection, validity, signed offsets and cell bounds
/// before decoding. Only selected rows are materialized, in order with repeats.
fn cells<T>(
    input: &Chunk,
    logical_rows: usize,
    view: &ColumnReadView<'_>,
    mut decode: impl FnMut(&[u8]) -> Result<T, EvalError>,
) -> Result<Vec<Option<T>>, EvalError> {
    (0..logical_rows)
        .map(|logical| {
            let row = match input.sel() {
                Some(selection) => *selection
                    .get(logical)
                    .ok_or_else(|| invalid("TiKV selection is out of bounds"))?,
                None => logical,
            };
            if row >= view.rows() {
                return Err(invalid("TiKV selection is out of bounds"));
            }
            let bitmap = view
                .null_bitmap()
                .get(row / 8)
                .ok_or_else(|| invalid("TiKV input validity bitmap is truncated"))?;
            if bitmap & (1 << (row % 8)) == 0 {
                return Ok(None);
            }
            let (start, end) = if let Some(width) = view.fixed_len() {
                let start = row
                    .checked_mul(width)
                    .ok_or_else(|| invalid("TiKV cell offset overflow"))?;
                (
                    start,
                    start
                        .checked_add(width)
                        .ok_or_else(|| invalid("TiKV cell offset overflow"))?,
                )
            } else {
                let offset = |index| -> Result<usize, EvalError> {
                    usize::try_from(
                        *view
                            .offsets()
                            .get(index)
                            .ok_or_else(|| invalid("TiKV input offsets are truncated"))?,
                    )
                    .map_err(|_| invalid("TiKV input offset is negative"))
                };
                (offset(row)?, offset(row + 1)?)
            };
            let bytes = view
                .data()
                .get(start..end)
                .ok_or_else(|| invalid("TiKV input cell is out of bounds"))?;
            decode(bytes).map(Some)
        })
        .collect()
}

pub(super) fn copy_column(
    input: &Chunk,
    index: usize,
    ty: &FieldType,
) -> Result<EngineColumn, EvalError> {
    let family = family(ty).ok_or_else(|| invalid("unsupported TiKV bridge input type"))?;
    if index >= input.num_cols() {
        return Err(invalid("TiKV expression input column is out of bounds"));
    }
    // num_rows reads the first column owner; do this before holding any owner
    // guard, since another index can alias it and recursive read locking can
    // deadlock behind a waiting writer.
    let logical_rows = input.num_rows();
    let column = input.column(index);
    let view = column.read_view();
    let expected_width = match ty.code() {
        FieldTypeCode::Float => Some(4),
        FieldTypeCode::NewDecimal => Some(40),
        FieldTypeCode::Bit => None,
        _ => match family {
            Family::Int | Family::Real | Family::DateTime | Family::Duration => Some(8),
            _ => None,
        },
    };
    if view.fixed_len() != expected_width {
        return Err(invalid("TiKV input layout does not match its FieldType"));
    }
    Ok(match family {
        Family::Int => EngineColumn::Int(cells(input, logical_rows, &view, |bytes| {
            if ty.code() == FieldTypeCode::Bit {
                let width = (ty.flen() as usize).div_ceil(8);
                if bytes.len() != width {
                    return Err(invalid("TiKV BIT cell does not match its declared width"));
                }
                let value = bytes
                    .iter()
                    .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte));
                check_bit(value, ty)?;
                Ok(value as i64)
            } else {
                // Retain all 64 bits; unsigned interpretation is schema metadata.
                Ok(i64::from_ne_bytes(array(bytes)?))
            }
        })?),
        Family::Real => EngineColumn::Real(cells(input, logical_rows, &view, |bytes| {
            let value = if ty.code() == FieldTypeCode::Float {
                f64::from(f32::from_ne_bytes(array(bytes)?))
            } else {
                f64::from_ne_bytes(array(bytes)?)
            };
            if !value.is_finite() {
                return Err(invalid("nonfinite TiKV real input"));
            }
            Ok(value)
        })?),
        Family::Bytes => EngineColumn::Bytes(cells(input, logical_rows, &view, |bytes| {
            if ty.code() == FieldTypeCode::Null {
                return Err(invalid("non-NULL value in a NULL-typed TiKV input"));
            }
            Ok(bytes.to_vec())
        })?),
        Family::Decimal => EngineColumn::Decimal(cells(input, logical_rows, &view, |bytes| {
            decimal_from_chunk(bytes).map_err(engine_error)
        })?),
        Family::DateTime => EngineColumn::DateTime(cells(input, logical_rows, &view, |bytes| {
            let time = Time::from_go_raw(u64::from_ne_bytes(array(bytes)?)).map_err(shape_error)?;
            check_time(time, ty)?;
            // Both engines' chunk Time layouts store local SQL wall fields.
            // Packed datum/epoch codecs would incorrectly apply a timezone to
            // TIMESTAMP again. Chunk transport never consults the session TZ.
            let raw = time.go_raw().to_le_bytes();
            date_time_from_chunk(&raw).map_err(engine_error)
        })?),
        Family::Duration => EngineColumn::Duration(cells(input, logical_rows, &view, |bytes| {
            let nanos = i64::from_ne_bytes(array(bytes)?);
            let fsp = ty.decimal().max(0) as i8;
            if nanos % 10_i64.pow(9 - fsp as u32) != 0 {
                return Err(invalid(
                    "TiKV duration bridge would round hidden fractional digits",
                ));
            }
            EngineDuration::from_nanos(nanos, fsp).map_err(|error| engine_error(error.into()))
        })?),
        Family::Json => EngineColumn::Json(cells(input, logical_rows, &view, json_from_chunk)?),
        Family::Enum => EngineColumn::Enum(cells(input, logical_rows, &view, |bytes| {
            let (value, name) = if bytes.is_empty() {
                (0, &[][..])
            } else {
                let value = u64::from_ne_bytes(array(
                    bytes
                        .get(..8)
                        .ok_or_else(|| invalid("truncated TiKV enum cell"))?,
                )?);
                (value, &bytes[8..])
            };
            check_enum(name, value, ty)?;
            Ok(EngineEnum::new(name.to_vec(), value))
        })?),
        // Same chunk cell as ENUM: `[8-byte native-endian bitmask][name bytes]`.
        // The name is not re-derived from `elems`; the engine treats the cell's
        // own bytes as authoritative and only the bit mask is compared.
        Family::Set => EngineColumn::Set(cells(input, logical_rows, &view, |bytes| {
            let (value, name) = if bytes.is_empty() {
                (0, &[][..])
            } else {
                let value = u64::from_ne_bytes(array(
                    bytes
                        .get(..8)
                        .ok_or_else(|| invalid("truncated TiKV set cell"))?,
                )?);
                (value, &bytes[8..])
            };
            Ok(EngineSet::new(name.to_vec(), value))
        })?),
        Family::Vector => {
            EngineColumn::VectorFloat32(cells(input, logical_rows, &view, |bytes| {
                let count = u32::from_le_bytes(array(
                    bytes
                        .get(..4)
                        .ok_or_else(|| invalid("truncated TiKV vector header"))?,
                )?) as usize;
                let payload = &bytes[4..];
                if count.checked_mul(4) != Some(payload.len()) {
                    return Err(invalid("TiKV vector length does not match its header"));
                }
                EngineVector::new(payload.to_vec()).map_err(|error| engine_error(error.into()))
            })?)
        }
    })
}

fn check_bit(value: u64, ty: &FieldType) -> Result<(), EvalError> {
    if ty.flen() < 64 && value >> ty.flen() != 0 {
        return Err(invalid("TiKV BIT value exceeds its declared width"));
    }
    Ok(())
}

fn check_enum(name: &[u8], value: u64, ty: &FieldType) -> Result<(), EvalError> {
    let valid = if value == 0 {
        name.is_empty()
    } else {
        usize::try_from(value - 1).ok().is_some_and(|index| {
            ty.with_elems_visible(|elems| {
                elems.get(index).is_some_and(|elem| elem.as_bytes() == name)
            })
        })
    };
    if !valid {
        return Err(invalid(
            "TiKV enum name/value disagrees with FieldType elements",
        ));
    }
    Ok(())
}

fn check_time(time: Time, ty: &FieldType) -> Result<(), EvalError> {
    let expected = match ty.code() {
        FieldTypeCode::Date => TimeType::Date,
        FieldTypeCode::Datetime => TimeType::DateTime,
        FieldTypeCode::Timestamp => TimeType::Timestamp,
        _ => return Err(invalid("TiKV temporal result does not match FieldType")),
    };
    let core = time.core_time();
    if time.kind() != expected
        || core.year() > 9999
        || core.month() > 12
        || core.day() > 31
        || core.hour() > 23
        || core.minute() > 59
        || core.second() > 59
        || core.microsecond() > 999_999
    {
        return Err(invalid("unsupported TiKV temporal value shape"));
    }
    // Zero and invalid-calendar dates remain SQL values; do not normalize them
    // through chrono or force a second SQL-mode validation during transport.
    Ok(())
}

fn datums<T>(
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> Result<Datum, EvalError>,
) -> Result<Vec<Datum>, EvalError> {
    values
        .into_iter()
        .map(|value| match value {
            Some(value) => convert(value),
            None => Ok(Datum::Null),
        })
        .collect()
}

pub(super) fn into_datums(column: EngineColumn, ty: &FieldType) -> Result<Vec<Datum>, EvalError> {
    let expected = family(ty).ok_or_else(|| invalid("unsupported TiKV bridge result type"))?;
    match column {
        EngineColumn::Int(values) if expected == Family::Int => datums(values, |value| {
            if ty.code() == FieldTypeCode::Bit {
                check_bit(value as u64, ty)?;
                let width = (ty.flen() as usize).div_ceil(8);
                Ok(Datum::Bit(BinaryLiteral::from(
                    value.to_be_bytes()[8 - width..].to_vec(),
                )))
            } else if ty.is_unsigned() && ty.code() != FieldTypeCode::Year {
                Ok(Datum::UInt(value as u64))
            } else {
                Ok(Datum::Int(value))
            }
        }),
        EngineColumn::Real(values) if expected == Family::Real => datums(values, |value| {
            if !value.is_finite() {
                return Err(invalid("nonfinite TiKV real result"));
            }
            if ty.code() == FieldTypeCode::Float {
                if f64::from(value as f32).to_bits() != value.to_bits() {
                    return Err(invalid(
                        "TiKV real result cannot be represented exactly as FLOAT",
                    ));
                }
                Ok(Datum::Float32(value))
            } else {
                Ok(Datum::Real(value))
            }
        }),
        EngineColumn::Bytes(values) if expected == Family::Bytes => datums(values, |value| {
            if ty.code() == FieldTypeCode::Null {
                return Err(invalid("non-NULL TiKV result for NULL FieldType"));
            }
            let mut datum = Datum::Null;
            datum.set_string(value, ty.collation());
            Ok(datum)
        }),
        EngineColumn::Decimal(values) if expected == Family::Decimal => datums(values, |value| {
            let bytes = decimal_to_chunk(&value).map_err(engine_error)?;
            let decimal = MyDecimal::from_raw_bytes(array(&bytes)?).map_err(shape_error)?;
            Ok(Datum::Decimal(Decimal::from_my_decimal(&decimal)))
        }),
        EngineColumn::DateTime(values) if expected == Family::DateTime => datums(values, |value| {
            let bytes = date_time_to_chunk(&value).map_err(engine_error)?;
            let time =
                Time::from_go_raw(u64::from_le_bytes(array(&bytes)?)).map_err(shape_error)?;
            check_time(time, ty)?;
            Ok(Datum::Time(time))
        }),
        EngineColumn::Duration(values) if expected == Family::Duration => datums(values, |value| {
            // TiDB's chunk reader retains the unspecified-FSP sentinel whereas
            // the native constructor normalizes it to its effective precision 0.
            let fsp = if ty.decimal() == -1 && value.fsp() == 0 {
                -1
            } else {
                i64::from(value.fsp())
            };
            Ok(Datum::Duration(MySqlDuration::from_raw_parts(
                value.to_nanos(),
                fsp,
            )))
        }),
        EngineColumn::Json(values) if expected == Family::Json => datums(values, |value| {
            let bytes = json_to_binary(&value).map_err(engine_error)?;
            let (&code, payload) = bytes
                .split_first()
                .ok_or_else(|| invalid("empty TiKV JSON result"))?;
            if temporal_json(code, payload) {
                return Err(invalid("temporal JSON is not an exact TiKV bridge shape"));
            }
            Ok(Datum::Json(BinaryJSON::from_encoded_parts(code, payload)))
        }),
        EngineColumn::Enum(values) if expected == Family::Enum => datums(values, |value| {
            check_enum(value.name(), value.value(), ty)?;
            Ok(Datum::Enum(
                MysqlEnum::new(value.name(), value.value()),
                ty.collation(),
            ))
        }),
        EngineColumn::Set(values) if expected == Family::Set => datums(values, |value| {
            Ok(Datum::Set(
                MysqlSet::new(value.name(), value.value()),
                ty.collation(),
            ))
        }),
        EngineColumn::VectorFloat32(values) if expected == Family::Vector => {
            datums(values, |value| {
                if !value.value.len().is_multiple_of(4) {
                    return Err(invalid("invalid TiKV vector result width"));
                }
                let elements = value
                    .value
                    .chunks_exact(4)
                    .map(|bytes| array(bytes).map(f32::from_ne_bytes))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Datum::VectorFloat32(
                    VectorFloat32::create(elements).map_err(shape_error)?,
                ))
            })
        }
        _ => Err(invalid("TiKV result carrier does not match its FieldType")),
    }
}

fn json_from_chunk(bytes: &[u8]) -> Result<EngineJson, EvalError> {
    let value = json_from_binary(bytes).map_err(engine_error)?;
    if temporal_json(bytes[0], &bytes[1..]) {
        return Err(invalid("temporal JSON is not an exact TiKV bridge shape"));
    }
    Ok(value)
}

/// Called only AFTER the native helper's bounded, nonoverlapping structural
/// preflight. No text/serde conversion (which would lose unsigned/opaque tags).
/// Temporal JSON is deliberately refused, including inside containers: TiDB
/// embeds CoreTime whereas TiKV embeds Time with different type/FSP low bits.
fn temporal_json(code: u8, bytes: &[u8]) -> bool {
    match code {
        0x0e..=0x11 => true,
        0x01 | 0x03 => {
            let count =
                u32::from_le_bytes(bytes[..4].try_into().expect("validated JSON count")) as usize;
            let start = 8 + if code == 0x01 { count * 6 } else { 0 };
            (0..count).any(|index| {
                let entry = start + index * 5;
                let child = bytes[entry];
                if child == 0x04 {
                    return false;
                }
                let offset = u32::from_le_bytes(
                    bytes[entry + 1..entry + 5]
                        .try_into()
                        .expect("validated JSON offset"),
                ) as usize;
                temporal_json(child, &bytes[offset..])
            })
        }
        _ => false,
    }
}

/// A pre-execution guard for native values the bridge cannot preserve. This is
/// not an error-retry policy: call before entering ANY engine kernel. The direct
/// public evaluation API still reports unsupported payloads as errors.
///
/// Bad column indexes, selection bounds and storage layouts are not classified
/// as native-only SQL values; copy_column reports those errors independently.
pub(super) fn requires_native(input: &Chunk, index: usize, ty: &FieldType) -> bool {
    if index >= input.num_cols() || !supported_type(ty) {
        return false;
    }
    // Numeric-integer/byte families have no value-level representation gap.
    // In particular, do not allocate a per-row boolean vector merely to prove
    // every integer or LENGTH input has the unconditional answer false.
    if !matches!(
        ty.code(),
        FieldTypeCode::Float
            | FieldTypeCode::Double
            | FieldTypeCode::Duration
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp
            | FieldTypeCode::NewDecimal
            | FieldTypeCode::Json
            | FieldTypeCode::VectorFloat32
    ) {
        return false;
    }
    // num_rows reads the first column owner; do this before holding any owner
    // guard, since another index can alias it and recursive read locking can
    // deadlock behind a waiting writer.
    let logical_rows = input.num_rows();
    let column = input.column(index);
    let view = column.read_view();
    let unsupported = cells(input, logical_rows, &view, |bytes| {
        Ok(match ty.code() {
            FieldTypeCode::Float if view.fixed_len() == Some(4) => {
                !f32::from_ne_bytes(array(bytes)?).is_finite()
            }
            FieldTypeCode::Double if view.fixed_len() == Some(8) => {
                !f64::from_ne_bytes(array(bytes)?).is_finite()
            }
            FieldTypeCode::Duration if view.fixed_len() == Some(8) => {
                let nanos = i64::from_ne_bytes(array(bytes)?);
                let fsp = ty.decimal().max(0) as i8;
                nanos % 10_i64.pow(9 - fsp as u32) != 0
                    || EngineDuration::from_nanos(nanos, fsp).is_err()
            }
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
                if view.fixed_len() == Some(8) =>
            {
                let time = Time::from_go_raw(u64::from_ne_bytes(array(bytes)?));
                time.is_ok_and(|time| check_time(time, ty).is_err())
            }
            FieldTypeCode::NewDecimal if view.fixed_len() == Some(40) => {
                decimal_from_chunk(bytes).is_err()
            }
            FieldTypeCode::Json if view.fixed_len().is_none() => json_from_chunk(bytes).is_err(),
            FieldTypeCode::VectorFloat32 if view.fixed_len().is_none() => {
                let header = bytes
                    .get(..4)
                    .ok_or_else(|| invalid("truncated vector header"))?;
                let count = u32::from_le_bytes(array(header)?) as usize;
                if count.checked_mul(4) != Some(bytes.len() - 4) {
                    return Err(invalid("invalid vector length"));
                }
                bytes[4..].chunks_exact(4).any(|bytes| {
                    !f32::from_le_bytes(bytes.try_into().expect("four-byte vector element"))
                        .is_finite()
                })
            }
            _ => false,
        })
    });
    unsupported.is_ok_and(|values| values.into_iter().flatten().any(|value| value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeFlags;

    fn round_trip(ty: &FieldType, values: &[Datum]) -> Vec<Datum> {
        let mut input = Chunk::new(std::slice::from_ref(ty), values.len(), values.len().max(1));
        for value in values {
            input.append_datum(0, value);
        }
        assert!(!requires_native(&input, 0, ty));
        into_datums(copy_column(&input, 0, ty).unwrap(), ty).unwrap()
    }

    #[test]
    fn supported_families_and_explicit_exclusions() {
        use FieldTypeCode::*;
        for code in [
            Tiny, Short, Int24, Long, LongLong, Year, Float, Double, NewDecimal, Date, Datetime,
            Timestamp, Duration, Json, Enum, Set, Varchar, VarString, String, TinyBlob, MediumBlob,
            LongBlob, Blob, Null,
        ] {
            assert!(supported_type(&FieldType::new(code)), "{code:?}");
            assert!(
                !supported_type(&FieldType::new(code).with_array(true)),
                "{code:?}"
            );
        }
        for code in [NewDate, Geometry, Unspecified, Unknown(42)] {
            assert!(!supported_type(&FieldType::new(code)), "{code:?}");
        }
        assert!(supported_type(&FieldType::new(Bit).with_flen(64)));
        assert!(!supported_type(&FieldType::new(Bit).with_flen(65)));
        assert!(!supported_type(&FieldType::new(Duration).with_decimal(7)));
        assert_eq!(
            supported_type(&FieldType::new(VectorFloat32)),
            cfg!(target_endian = "little")
        );
    }

    #[test]
    fn selected_rows_preserve_nulls_order_duplicates_and_unsigned_bits() {
        let ty = FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::UNSIGNED);
        let mut input = Chunk::new(std::slice::from_ref(&ty), 4, 4);
        input.append_uint64(0, u64::MAX);
        input.append_null(0);
        input.append_uint64(0, 1_u64 << 63);
        input.append_uint64(0, 17);
        input.set_sel(Some(vec![2, 0, 2, 1]));
        let copied = copy_column(&input, 0, &ty).unwrap();
        assert_eq!(
            copied,
            EngineColumn::Int(vec![Some(i64::MIN), Some(-1), Some(i64::MIN), None])
        );
        assert_eq!(
            into_datums(copied, &ty).unwrap(),
            vec![
                Datum::UInt(1 << 63),
                Datum::UInt(u64::MAX),
                Datum::UInt(1 << 63),
                Datum::Null
            ]
        );
        input.set_sel(Some(vec![]));
        assert_eq!(
            copy_column(&input, 0, &ty).unwrap(),
            EngineColumn::Int(vec![])
        );
    }

    #[test]
    fn bounds_layout_and_carrier_mismatches_are_fallible() {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new(std::slice::from_ref(&ty), 1, 1);
        input.append_int64(0, 7);
        assert!(copy_column(&input, 1, &ty).is_err());
        assert!(copy_column(&input, 0, &FieldType::new(FieldTypeCode::Float)).is_err());
        input.set_sel(Some(vec![usize::MAX]));
        assert!(copy_column(&input, 0, &ty).is_err());
        assert!(!requires_native(&input, 0, &ty));
        assert!(into_datums(EngineColumn::Bytes(vec![None]), &ty).is_err());
        let mut truncated = Chunk::new(std::slice::from_ref(&ty), 1, 1);
        truncated
            .column_mut(0)
            .append_raw_cells([(true, &[1_u8][..])].into_iter());
        assert!(copy_column(&truncated, 0, &ty).is_err());
    }

    #[test]
    fn float32_and_double_use_distinct_storage_and_datum_kinds() {
        let float = FieldType::new(FieldTypeCode::Float);
        let double = FieldType::new(FieldTypeCode::Double);
        let value = f64::from(0.1_f32);
        assert_eq!(
            round_trip(&float, &[Datum::Float32(value), Datum::Null]),
            vec![Datum::Float32(value), Datum::Null]
        );
        assert_eq!(
            round_trip(&double, &[Datum::Real(0.1), Datum::Null]),
            vec![Datum::Real(0.1), Datum::Null]
        );
        assert!(into_datums(EngineColumn::Real(vec![Some(0.1)]), &float).is_err());
        let result = into_datums(EngineColumn::Real(vec![Some(-0.0)]), &float).unwrap();
        let Datum::Float32(value) = result[0] else {
            panic!("not FLOAT");
        };
        assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
        let mut input = Chunk::new(std::slice::from_ref(&float), 2, 2);
        input.append_float32(0, f32::NAN);
        input.append_float32(0, 1.0);
        assert!(requires_native(&input, 0, &float));
        assert!(copy_column(&input, 0, &float).is_err());
        input.set_sel(Some(vec![1]));
        assert!(!requires_native(&input, 0, &float));
    }

    #[test]
    fn decimal_hidden_fraction_is_not_formatted_away() {
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(2);
        let (decimal, error) = MyDecimal::from_string(b"-123.456789");
        assert!(error.is_none());
        let mut raw = decimal.to_raw_bytes();
        raw[2] = 2; // resultFrac is independent of stored fractional words.
        let decimal = MyDecimal::from_raw_bytes(raw).unwrap();
        let mut input = Chunk::new(std::slice::from_ref(&ty), 2, 2);
        input.append_my_decimal(0, &decimal);
        input.append_null(0);
        input.set_sel(Some(vec![0, 1, 0]));
        let column = copy_column(&input, 0, &ty).unwrap();
        let EngineColumn::Decimal(values) = &column else {
            panic!("not DECIMAL");
        };
        assert_eq!(
            decimal_to_chunk(values[0].as_ref().unwrap()).unwrap(),
            decimal.to_raw_bytes()
        );
        let result = into_datums(column, &ty).unwrap();
        for index in [0, 2] {
            let Datum::Decimal(value) = &result[index] else {
                panic!("not DECIMAL");
            };
            let raw = value.to_my_decimal().unwrap();
            assert_eq!(raw.digits_frac(), 6);
            assert_eq!(raw.result_frac(), 2);
            assert_eq!(raw.to_raw_bytes(), decimal.to_raw_bytes());
        }
        assert_eq!(result[1], Datum::Null);
    }

    #[test]
    fn malformed_decimal_is_rejected_by_safe_helper() {
        let ty = FieldType::new(FieldTypeCode::NewDecimal);
        let mut input = Chunk::new(std::slice::from_ref(&ty), 1, 1);
        // Public raw-cell append can carry untrusted chunk bytes; no native
        // Decimal decoder is invoked before checking the boolean/header.
        let mut bytes = [0_u8; 40];
        bytes[3] = 2;
        input
            .column_mut(0)
            .append_raw_cells([(true, bytes.as_slice())].into_iter());
        assert!(copy_column(&input, 0, &ty).is_err());
    }

    #[test]
    fn temporal_wall_fields_zero_dates_and_fsp_round_trip() {
        for (code, kind, fsp) in [
            (FieldTypeCode::Date, TimeType::Date, 0),
            (FieldTypeCode::Datetime, TimeType::DateTime, 6),
            (FieldTypeCode::Timestamp, TimeType::Timestamp, 6),
        ] {
            let ty = FieldType::new(code).with_decimal(fsp);
            // A timezone's DST fold/gap must not be resolved during transport.
            let value = Time::from_date_checked(2024, 11, 3, 1, 30, 12, 123456, kind, fsp).unwrap();
            let zero = Time::from_date_checked(0, 0, 0, 0, 0, 0, 0, kind, fsp).unwrap();
            let values = [Datum::Time(value), Datum::Null, Datum::Time(zero)];
            assert_eq!(round_trip(&ty, &values), values);
        }
    }

    #[test]
    fn duration_fsp_is_preserved_and_rounding_is_refused_before_execution() {
        let ty = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
        let values = [
            Datum::Duration(MySqlDuration::from_raw_parts(-1_234_567_000, 6)),
            Datum::Null,
        ];
        assert_eq!(round_trip(&ty, &values), values);
        let unspecified = FieldType::new(FieldTypeCode::Duration);
        let values = [Datum::Duration(MySqlDuration::from_raw_parts(
            1_000_000_000,
            -1,
        ))];
        assert_eq!(round_trip(&unspecified, &values), values);
        let coarse = ty.with_decimal(2);
        let mut input = Chunk::new(std::slice::from_ref(&coarse), 2, 2);
        input.append_duration(0, MySqlDuration::from_raw_parts(1_234_567_000, 2));
        input.append_null(0);
        assert!(requires_native(&input, 0, &coarse));
        assert!(copy_column(&input, 0, &coarse).is_err());
        input.set_sel(Some(vec![1]));
        assert!(!requires_native(&input, 0, &coarse));
    }

    #[test]
    fn enum_preserves_non_utf8_name_index_and_collation() {
        let name = vec![0xff, b'a'];
        let ty = FieldType::new(FieldTypeCode::Enum).with_elems([name.clone()]);
        let value = Datum::Enum(MysqlEnum::new(name, 1), ty.collation());
        let empty = Datum::Enum(MysqlEnum::new("", 0), ty.collation());
        assert_eq!(
            round_trip(&ty, &[value.clone(), Datum::Null, empty.clone()]),
            vec![value, Datum::Null, empty]
        );
        assert!(into_datums(
            EngineColumn::Enum(vec![Some(EngineEnum::new(b"wrong".to_vec(), 1))]),
            &ty
        )
        .is_err());
    }

    #[test]
    fn bit_round_trip_keeps_declared_width_and_high_bit() {
        for (bits, bytes) in [(9, vec![0x01, 0xff]), (64, vec![0xff; 8])] {
            let ty = FieldType::new(FieldTypeCode::Bit).with_flen(bits);
            let value = Datum::Bit(BinaryLiteral::from(bytes));
            assert_eq!(
                round_trip(&ty, &[value.clone(), Datum::Null]),
                vec![value, Datum::Null]
            );
        }
        let ty = FieldType::new(FieldTypeCode::Bit).with_flen(9);
        assert!(into_datums(EngineColumn::Int(vec![Some(512)]), &ty).is_err());
    }

    #[test]
    fn strings_are_bytes_and_null_type_is_always_null() {
        let ty = FieldType::new(FieldTypeCode::Varchar);
        let mut value = Datum::Null;
        value.set_string(vec![0xff, 0, b'a'], ty.collation());
        assert_eq!(
            round_trip(&ty, &[value.clone(), Datum::Null]),
            vec![value, Datum::Null]
        );
        let null = FieldType::new(FieldTypeCode::Null);
        assert_eq!(round_trip(&null, &[Datum::Null]), vec![Datum::Null]);
        assert!(into_datums(EngineColumn::Bytes(vec![Some(vec![])]), &null).is_err());
    }

    #[test]
    fn json_tags_and_nested_binary_payload_are_exact() {
        let ty = FieldType::new(FieldTypeCode::Json);
        let unsigned = BinaryJSON::from_encoded_parts(0x0a, u64::MAX.to_le_bytes().to_vec());
        let signed = BinaryJSON::from_encoded_parts(0x09, 1_i64.to_le_bytes().to_vec());
        let opaque = BinaryJSON::from_encoded_parts(0x0d, vec![252, 3, 0, 0xff, 1]);
        let nested = BinaryJSON::parse(r#"{"a":[null,true,1,"x"],"z":-1.5}"#).unwrap();
        let values = [
            Datum::Json(unsigned),
            Datum::Json(signed),
            Datum::Json(opaque),
            Datum::Json(nested),
            Datum::Null,
        ];
        assert_eq!(round_trip(&ty, &values), values);
        assert!(json_from_chunk(&[0x01, 0]).is_err());
    }

    #[test]
    fn temporal_json_declines_before_engine_including_nested_values() {
        let ty = FieldType::new(FieldTypeCode::Json);
        let time = Time::from_date_checked(2024, 1, 2, 3, 4, 5, 0, TimeType::Timestamp, 0).unwrap();
        let json = BinaryJSON::from_time(time);
        let mut input = Chunk::new(std::slice::from_ref(&ty), 2, 2);
        input.append_json(0, &json);
        input.append_null(0);
        assert!(requires_native(&input, 0, &ty));
        assert!(copy_column(&input, 0, &ty).is_err());
        // Canonical binary array containing one temporal element.
        let mut array = vec![0x03];
        array.extend_from_slice(&1_u32.to_le_bytes());
        array.extend_from_slice(&21_u32.to_le_bytes());
        array.push(json.type_code());
        array.extend_from_slice(&13_u32.to_le_bytes());
        array.extend_from_slice(json.value());
        assert!(json_from_binary(&array).is_ok());
        assert!(json_from_chunk(&array).is_err());
        input.set_sel(Some(vec![1]));
        assert!(!requires_native(&input, 0, &ty));
    }

    #[test]
    #[cfg(target_endian = "little")]
    fn vectors_preserve_negative_zero_dimensions_and_nulls() {
        let ty = FieldType::new(FieldTypeCode::VectorFloat32);
        let vector = VectorFloat32::create(vec![-0.0, 0.1, f32::MAX]).unwrap();
        let result = round_trip(&ty, &[Datum::VectorFloat32(vector.clone()), Datum::Null]);
        let Datum::VectorFloat32(output) = &result[0] else {
            panic!("not VECTOR");
        };
        assert_eq!(output.serialize(), vector.serialize());
        assert_eq!(result[1], Datum::Null);
        let mut input = Chunk::new(std::slice::from_ref(&ty), 1, 1);
        let mut invalid = VectorFloat32::init(1);
        invalid.elements_mut()[0] = f32::INFINITY;
        input.append_vector_float32(0, &invalid);
        assert!(requires_native(&input, 0, &ty));
        assert!(copy_column(&input, 0, &ty).is_err());
    }
}
