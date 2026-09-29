// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Checked, exact transport for the initial local TiKV value domain.
//!
//! This module neither casts SQL values nor selects kernels. Its errors are
//! bridge admission diagnostics, not SQL errors or invitations to replay a
//! native evaluator. Callers must preserve demand timing: transporting every
//! constant or unselected physical row eagerly can introduce errors in dead
//! branches. Keep the complete detached SQL type alongside its kernel projection.

use std::fmt;

use tidb_query_datatype::codec::data_type::{Real, ScalarValue, ScalarValueRef};
use tidb_query_datatype::{EvalType, FieldTypeTp};

use crate::{BinaryLiteral, Collation, Datum, DatumKind, FieldType, FieldTypeCode, StringDatum};

/// A representation/admission diagnostic, without statement warning policy.
#[derive(Clone, Debug, PartialEq)]
pub enum BridgeError {
    /// This SQL type is not in the initial bridge domain.
    UnsupportedFieldType(FieldTypeCode),
    /// ARRAY must not silently become ordinary JSON; retains the element code.
    ArrayFieldType(FieldTypeCode),
    /// A metadata field cannot be represented by the protobuf integer width.
    MetadataOutOfRange {
        /// Source field name.
        field: &'static str,
        /// Exact source value, including unsigned flag words.
        value: i128,
    },
    /// A Go element string cannot be represented as a protobuf UTF-8 string.
    InvalidElementEncoding(usize),
    /// This datum kind has no exact initial-domain carrier.
    UnsupportedDatumKind(DatumKind),
    /// This kernel class has not been admitted by the initial bridge.
    UnsupportedEvalType(EvalType),
    /// The value's carrier class differs from the caller's expected class.
    EvalTypeMismatch {
        /// Class required by the prepared input or output slot.
        expected: EvalType,
        /// Class of the supplied value or metadata.
        actual: EvalType,
    },
    /// The output record cannot reconstruct the requested datum kind exactly.
    InvalidValueMetadata {
        /// Requested source/result datum kind.
        kind: DatumKind,
        /// Missing or incompatible metadata field.
        field: &'static str,
    },
    /// TiKV's NotNan carrier cannot represent this real value; never SQL NULL.
    NonRepresentableReal {
        /// Original floating-point bits, retained even for distinct NaNs.
        bits: u64,
    },
}

impl fmt::Display for BridgeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFieldType(code) => {
                write!(formatter, "local bridge does not admit SQL type {code:?}")
            }
            Self::ArrayFieldType(code) => {
                write!(formatter, "local bridge does not admit ARRAY of {code:?}")
            }
            Self::MetadataOutOfRange { field, value } => {
                write!(
                    formatter,
                    "local bridge metadata {field}={value} exceeds wire width"
                )
            }
            Self::InvalidElementEncoding(index) => {
                write!(formatter, "local bridge element {index} is not UTF-8")
            }
            Self::UnsupportedDatumKind(kind) => {
                write!(formatter, "local bridge does not admit datum kind {kind:?}")
            }
            Self::UnsupportedEvalType(eval_type) => {
                write!(
                    formatter,
                    "local bridge does not admit eval type {eval_type:?}"
                )
            }
            Self::EvalTypeMismatch { expected, actual } => {
                write!(
                    formatter,
                    "local bridge expected {expected:?}, received {actual:?}"
                )
            }
            Self::InvalidValueMetadata { kind, field } => {
                write!(
                    formatter,
                    "local bridge {kind:?} has incompatible {field} metadata"
                )
            }
            Self::NonRepresentableReal { bits } => {
                write!(
                    formatter,
                    "local bridge cannot represent real bits {bits:#018x}"
                )
            }
        }
    }
}

impl std::error::Error for BridgeError {}

/// Datum identity not present in a TiKV scalar's evaluation class.
///
/// `BinaryLiteral` is explicit provenance, distinct from both ordinary String
/// and Bytes even when their payload and binary collation are identical. A BIT
/// column is a different kind and is not admitted yet. This record does not
/// replace the lowerer's literal-versus-input distinction or the SQL FieldType.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueMetadata {
    /// Input identity, or an explicitly selected computed-output identity.
    pub kind: DatumKind,
    /// Required only for String; never inferred from a binary literal's bytes.
    pub string_collation: Option<Collation>,
    /// Reserved for exact Decimal transport; must be absent in this slice.
    pub decimal_declared_shape: Option<(i64, i64)>,
}

/// Detaches all mutable metadata backing while retaining the complete SQL type.
///
/// In particular, `FieldType::clone` alone shares mutable ENUM/SET element and
/// binary-literal-marker slices. Snapshot once per specialization, not per row.
/// The returned value retains raw flags, widths, exact names, ARRAY/element type,
/// arbitrary element bytes and their markers, even outside bridge admission.
#[must_use]
pub fn snapshot_field_type(sql: &FieldType) -> FieldType {
    sql.deep_copy_like_go()
}

/// Projects an admitted SQL type without narrowing metadata or rewriting it.
///
/// The caller must retain [`snapshot_field_type`] separately and supply a wire
/// collation ID already checked by the collation owner. This function preserves
/// that ID's sign and the exact charset spelling; it does not resolve/default
/// names or infer literal provenance. ENUM/SET, ARRAY, BIT, Decimal, temporal,
/// JSON, vector and unknown types remain explicit unimplemented domains.
pub fn project_field_type(
    sql: &FieldType,
    signed_wire_collation: i32,
) -> Result<tipb::FieldType, BridgeError> {
    if sql.is_array() {
        return Err(BridgeError::ArrayFieldType(sql.array_element_code()));
    }
    let kernel_type = match sql.code() {
        FieldTypeCode::Tiny => FieldTypeTp::Tiny,
        FieldTypeCode::Short => FieldTypeTp::Short,
        FieldTypeCode::Long => FieldTypeTp::Long,
        FieldTypeCode::LongLong => FieldTypeTp::LongLong,
        FieldTypeCode::Int24 => FieldTypeTp::Int24,
        FieldTypeCode::Year => FieldTypeTp::Year,
        FieldTypeCode::Float => FieldTypeTp::Float,
        FieldTypeCode::Double => FieldTypeTp::Double,
        FieldTypeCode::Null => FieldTypeTp::Null,
        FieldTypeCode::Varchar => FieldTypeTp::VarChar,
        FieldTypeCode::VarString => FieldTypeTp::VarString,
        FieldTypeCode::String => FieldTypeTp::String,
        FieldTypeCode::TinyBlob => FieldTypeTp::TinyBlob,
        FieldTypeCode::MediumBlob => FieldTypeTp::MediumBlob,
        FieldTypeCode::LongBlob => FieldTypeTp::LongBlob,
        FieldTypeCode::Blob => FieldTypeTp::Blob,
        code => return Err(BridgeError::UnsupportedFieldType(code)),
    };
    let flag = u32::try_from(sql.raw_flags()).map_err(|_| BridgeError::MetadataOutOfRange {
        field: "flags",
        value: i128::from(sql.raw_flags()),
    })?;
    let flen = i32::try_from(sql.flen()).map_err(|_| BridgeError::MetadataOutOfRange {
        field: "flen",
        value: i128::from(sql.flen()),
    })?;
    let decimal = i32::try_from(sql.decimal()).map_err(|_| BridgeError::MetadataOutOfRange {
        field: "decimal",
        value: i128::from(sql.decimal()),
    })?;
    let elems = sql.with_elems_visible(|elements| {
        elements
            .iter()
            .enumerate()
            .map(|(index, element)| {
                element
                    .as_utf8()
                    .map(str::to_owned)
                    .map_err(|_| BridgeError::InvalidElementEncoding(index))
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    // Use TiKV's named type conversion, not cross-crate enum casts. For other
    // fields use the raw protobuf setters: FieldTypeAccessor truncates flags
    // and narrows isize widths. No SQL declaration validation occurs here.
    let mut projected = tipb::FieldType::from(kernel_type);
    projected.set_flag(flag);
    projected.set_flen(flen);
    projected.set_decimal(decimal);
    projected.set_collate(signed_wire_collation);
    projected.set_charset(sql.charset_name().to_owned());
    projected.set_elems(elems.into());
    Ok(projected)
}

/// Copies an initial-domain datum into an existing TiKV nullable scalar.
///
/// UInt uses the Int carrier's unchanged 64 bits. The selected kernel signature,
/// not this bridge, determines operand signedness. Real uses checked NotNan
/// construction; `ScalarValue::from(f64)` would silently convert NaN into NULL.
/// Bytes and literal bytes are neither decoded nor numerically interpreted.
pub fn to_scalar(
    datum: &Datum,
    expected: EvalType,
) -> Result<(ScalarValue, ValueMetadata), BridgeError> {
    check_eval_type(expected)?;
    check_kind_type(datum.kind(), expected)?;
    let metadata = ValueMetadata {
        kind: datum.kind(),
        string_collation: match datum {
            Datum::String(value) => Some(value.collation()),
            _ => None,
        },
        decimal_declared_shape: None,
    };
    let scalar = match datum {
        Datum::Null => match expected {
            EvalType::Int => ScalarValue::Int(None),
            EvalType::Real => ScalarValue::Real(None),
            EvalType::Bytes => ScalarValue::Bytes(None),
            other => return Err(BridgeError::UnsupportedEvalType(other)),
        },
        Datum::Int(value) => ScalarValue::Int(Some(*value)),
        Datum::UInt(value) => ScalarValue::Int(Some(i64::from_ne_bytes(value.to_ne_bytes()))),
        Datum::Real(value) | Datum::Float32(value) => {
            let real = Real::new(*value).map_err(|_| BridgeError::NonRepresentableReal {
                bits: value.to_bits(),
            })?;
            ScalarValue::Real(Some(real))
        }
        Datum::String(value) => ScalarValue::Bytes(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => ScalarValue::Bytes(Some(value.clone())),
        Datum::BinaryLiteral(value) => ScalarValue::Bytes(Some(value.as_bytes().to_vec())),
        other => return Err(BridgeError::UnsupportedDatumKind(other.kind())),
    };
    Ok((scalar, metadata))
}

/// Reconstructs an exact datum using an explicitly supplied output identity.
///
/// Reuse input metadata only for a passthrough. Computed values need the result
/// boundary's metadata, not a guessed copy of an operand's unsignedness, literal
/// origin or declared shape. Carrier mismatches are rejected even for NULL.
pub fn from_scalar(
    value: ScalarValueRef<'_>,
    expected: EvalType,
    output: &ValueMetadata,
) -> Result<Datum, BridgeError> {
    check_eval_type(expected)?;
    if value.eval_type() != expected {
        return Err(BridgeError::EvalTypeMismatch {
            expected,
            actual: value.eval_type(),
        });
    }
    check_kind_type(output.kind, expected)?;
    if output.decimal_declared_shape.is_some() {
        return Err(invalid_metadata(output, "decimal_declared_shape"));
    }
    if (output.kind == DatumKind::String) != output.string_collation.is_some() {
        return Err(invalid_metadata(output, "string_collation"));
    }
    match (value, output.kind) {
        (
            ScalarValueRef::Int(None) | ScalarValueRef::Real(None) | ScalarValueRef::Bytes(None),
            _,
        ) => Ok(Datum::Null),
        (ScalarValueRef::Int(Some(value)), DatumKind::Int) => Ok(Datum::Int(*value)),
        (ScalarValueRef::Int(Some(value)), DatumKind::UInt) => {
            Ok(Datum::UInt(u64::from_ne_bytes(value.to_ne_bytes())))
        }
        (ScalarValueRef::Real(Some(value)), DatumKind::Real) => {
            Ok(Datum::Real((*value).into_inner()))
        }
        (ScalarValueRef::Real(Some(value)), DatumKind::Float32) => {
            Ok(Datum::Float32((*value).into_inner()))
        }
        (ScalarValueRef::Bytes(Some(bytes)), DatumKind::String) => {
            let collation = output
                .string_collation
                .ok_or_else(|| invalid_metadata(output, "string_collation"))?;
            Ok(Datum::String(StringDatum::new(bytes.to_vec(), collation)))
        }
        (ScalarValueRef::Bytes(Some(bytes)), DatumKind::Bytes) => Ok(Datum::Bytes(bytes.to_vec())),
        (ScalarValueRef::Bytes(Some(bytes)), DatumKind::BinaryLiteral) => {
            Ok(Datum::BinaryLiteral(BinaryLiteral::from(bytes)))
        }
        _ => Err(invalid_metadata(output, "kind")),
    }
}

fn check_eval_type(eval_type: EvalType) -> Result<(), BridgeError> {
    match eval_type {
        EvalType::Int | EvalType::Real | EvalType::Bytes => Ok(()),
        other => Err(BridgeError::UnsupportedEvalType(other)),
    }
}

fn check_kind_type(kind: DatumKind, expected: EvalType) -> Result<(), BridgeError> {
    let actual = match kind {
        DatumKind::Null => return Ok(()),
        DatumKind::Int | DatumKind::UInt => EvalType::Int,
        DatumKind::Real | DatumKind::Float32 => EvalType::Real,
        DatumKind::String | DatumKind::Bytes | DatumKind::BinaryLiteral => EvalType::Bytes,
        other => return Err(BridgeError::UnsupportedDatumKind(other)),
    };
    if actual != expected {
        return Err(BridgeError::EvalTypeMismatch { expected, actual });
    }
    Ok(())
}

fn invalid_metadata(output: &ValueMetadata, field: &'static str) -> BridgeError {
    BridgeError::InvalidValueMetadata {
        kind: output.kind,
        field,
    }
}
