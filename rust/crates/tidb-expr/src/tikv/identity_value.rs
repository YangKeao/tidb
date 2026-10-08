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

//! Representation-only adaptation to the shared identity frame. The shared
//! codec owns tags and framing; decoding consumes only the returned frame,
//! without retaining or consulting the original datum.

use tidb_datatype::{
    BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, MySqlDuration, MysqlEnum, MysqlSet,
    Time, TimeType, VectorFloat32,
};
use tidb_query_expr::{
    decode_native_identity, encode_native_identity, NativeIdentityFrameError,
    NativeIdentityRef as View,
};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{ExpressionRuntimeFailure, LocalError, NativeCollation};
use crate::{Datum, EvalError};

fn invalid_frame() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native identity representation frame",
    ))
}

fn capacity_error(message: String) -> EvalError {
    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
        LocalError::ResourceLimit(message.into()),
        None,
    ))
}

fn frame_error(error: NativeIdentityFrameError) -> EvalError {
    match error {
        NativeIdentityFrameError::Invalid => invalid_frame(),
        NativeIdentityFrameError::Capacity => {
            capacity_error("native identity frame allocation or size failed".to_owned())
        }
    }
}

fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>, EvalError> {
    let mut result = Vec::new();
    result.try_reserve_exact(bytes.len()).map_err(|error| {
        capacity_error(format!(
            "native identity payload allocation failed: {error}"
        ))
    })?;
    result.extend_from_slice(bytes);
    Ok(result)
}

fn collation_tag(collation: Collation) -> Result<u8, EvalError> {
    let tag = u8::try_from(collation.native_policy().tag()).map_err(|_| invalid_frame())?;
    if tag > 15 {
        return Err(invalid_frame());
    }
    Ok(tag)
}

/// Inverts only explicit representation identities, never registry IDs or the
/// process-global collation mode. Pinyin metadata does not invoke its kernels.
fn collation_from_tag(tag: u8) -> Result<Collation, EvalError> {
    let policy = NativeCollation::from_tag(i64::from(tag)).ok_or_else(invalid_frame)?;
    Ok(match policy {
        NativeCollation::Binary => Collation::Binary,
        NativeCollation::AsciiBin => Collation::AsciiBin,
        NativeCollation::Latin1Bin => Collation::Latin1Bin,
        NativeCollation::Utf8Bin => Collation::Utf8Bin,
        NativeCollation::Utf8GeneralCi => Collation::Utf8GeneralCi,
        NativeCollation::Utf8UnicodeCi => Collation::Utf8UnicodeCi,
        NativeCollation::Utf8Mb4Bin => Collation::Utf8Mb4Bin,
        NativeCollation::Utf8Mb4GeneralCi => Collation::Utf8Mb4GeneralCi,
        NativeCollation::Utf8Mb4UnicodeCi => Collation::Utf8Mb4UnicodeCi,
        NativeCollation::Utf8Mb40900AiCi => Collation::Utf8Mb40900AiCi,
        NativeCollation::Utf8Mb40900Bin => Collation::Utf8Mb40900Bin,
        NativeCollation::Utf8Mb4ZhPinyinTiDbAsCs => Collation::Utf8Mb4ZhPinyinTiDbAsCs,
        NativeCollation::GbkBin => Collation::GbkBin,
        NativeCollation::GbkChineseCi => Collation::GbkChineseCi,
        NativeCollation::Gb18030Bin => Collation::Gb18030Bin,
        NativeCollation::Gb18030ChineseCi => Collation::Gb18030ChineseCi,
    })
}

pub(crate) fn encode(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    let mut vector_bytes = Vec::new();
    let view = match value {
        Datum::Null => return Ok(None),
        Datum::MinNotNull => View::MinNotNull,
        Datum::MaxValue => View::MaxValue,
        Datum::Int(value) => View::Int(*value),
        Datum::UInt(value) => View::UInt(*value),
        Datum::Decimal(value) => View::Decimal {
            negative: value.is_negative(),
            scale: value.scale(),
            storage_scale: value.storage_scale(),
            declared_shape: value.declared_shape(),
            coefficient: value.coefficient_bytes(),
        },
        Datum::Real(value) => View::Real(value.to_bits()),
        // Native KindFloat32 retains its entire f64 payload, not a narrowed f32.
        Datum::Float32(value) => View::Float32(value.to_bits()),
        Datum::String(value) => View::String {
            collation: collation_tag(value.collation())?,
            bytes: value.bytes(),
        },
        Datum::Bytes(value) => View::Bytes(value),
        Datum::BinaryLiteral(value) => View::BinaryLiteral(value.as_bytes()),
        Datum::Duration(value) => View::Duration {
            nanos: value.nanoseconds(),
            fsp: value.fsp(),
        },
        Datum::Enum(value, collation) => View::Enum {
            collation: collation_tag(*collation)?,
            value: value.value(),
            name: value.name_bytes(),
        },
        Datum::Bit(value) => View::Bit(value.as_bytes()),
        Datum::Set(value, collation) => View::Set {
            collation: collation_tag(*collation)?,
            value: value.value(),
            name: value.name_bytes(),
        },
        Datum::Time(value) => View::Time {
            core: value.core_time().raw(),
            kind: match value.kind() {
                TimeType::Date => 0,
                TimeType::DateTime => 1,
                TimeType::Timestamp => 2,
            },
            fsp: value.fsp(),
        },
        Datum::Json(value) => View::Json {
            type_code: value.type_code(),
            bytes: value.value(),
        },
        Datum::Raw(value) => View::Raw(value),
        Datum::VectorFloat32(value) => {
            let bytes = value
                .len()
                .checked_mul(4)
                .ok_or_else(|| capacity_error("native identity vector size overflow".to_owned()))?;
            vector_bytes.try_reserve_exact(bytes).map_err(|error| {
                capacity_error(format!("native identity vector allocation failed: {error}"))
            })?;
            for element in value.elements() {
                vector_bytes.extend_from_slice(&element.to_bits().to_le_bytes());
            }
            View::Vector(&vector_bytes)
        }
    };
    encode_native_identity(view).map(Some).map_err(frame_error)
}

pub(crate) fn decode(value: Option<Vec<u8>>) -> Result<Datum, EvalError> {
    let Some(frame) = value else {
        return Ok(Datum::Null);
    };
    Ok(match decode_native_identity(&frame).map_err(frame_error)? {
        View::MinNotNull => Datum::MinNotNull,
        View::MaxValue => Datum::MaxValue,
        View::Int(value) => Datum::Int(value),
        View::UInt(value) => Datum::UInt(value),
        View::Decimal {
            negative,
            scale,
            storage_scale,
            declared_shape,
            coefficient,
        } => {
            let value =
                Decimal::from_raw_parts(negative, copy_bytes(coefficient)?, scale, storage_scale);
            Datum::Decimal(match declared_shape {
                Some((precision, scale)) => value.with_declared_shape(precision, scale),
                None => value,
            })
        }
        View::Real(bits) => Datum::Real(f64::from_bits(bits)),
        View::Float32(bits) => Datum::Float32(f64::from_bits(bits)),
        View::String { collation, bytes } => {
            Datum::new_collation_string(copy_bytes(bytes)?, collation_from_tag(collation)?)
        }
        View::Bytes(bytes) => Datum::Bytes(copy_bytes(bytes)?),
        View::BinaryLiteral(bytes) => Datum::BinaryLiteral(BinaryLiteral::from(copy_bytes(bytes)?)),
        View::Duration { nanos, fsp } => Datum::Duration(MySqlDuration::from_raw_parts(nanos, fsp)),
        View::Enum {
            collation,
            value,
            name,
        } => Datum::Enum(
            MysqlEnum::new(copy_bytes(name)?, value),
            collation_from_tag(collation)?,
        ),
        View::Bit(bytes) => Datum::Bit(BinaryLiteral::from(copy_bytes(bytes)?)),
        View::Set {
            collation,
            value,
            name,
        } => Datum::Set(
            MysqlSet::new(copy_bytes(name)?, value),
            collation_from_tag(collation)?,
        ),
        View::Time { core, kind, fsp } => {
            let kind = match kind {
                0 => TimeType::Date,
                1 => TimeType::DateTime,
                2 => TimeType::Timestamp,
                _ => return Err(invalid_frame()),
            };
            Datum::Time(Time::from_raw_parts(CoreTime::from_raw(core), kind, fsp))
        }
        View::Json { type_code, bytes } => Datum::Json(BinaryJSON::from_encoded_parts(
            type_code,
            copy_bytes(bytes)?,
        )),
        View::Raw(bytes) => Datum::Raw(copy_bytes(bytes)?),
        View::Vector(bytes) => {
            // The shared codec validates only divisibility by four, not SQL
            // dimensions or finiteness. init retains its original allocator;
            // reconstruction is not a claim of checked deep-heap recovery.
            let mut value = VectorFloat32::init(bytes.len() / 4);
            for (element, bits) in value.elements_mut().iter_mut().zip(bytes.chunks_exact(4)) {
                *element = f32::from_bits(u32::from_le_bytes([bits[0], bits[1], bits[2], bits[3]]));
            }
            Datum::VectorFloat32(value)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstructs_only_returned_raw_representations() {
        let coefficient = [0, 255, b'0'];
        let vector = [0x42, 0, 0xc0, 0x7f, 0, 0, 0, 0x80, 0, 0, 0x80, 0x7f];
        for view in [
            View::String {
                collation: 11,
                bytes: &[255, 0, 0xc3],
            },
            View::Decimal {
                negative: true,
                scale: u32::MAX,
                storage_scale: 3,
                declared_shape: Some((-1, i64::MAX)),
                coefficient: &coefficient,
            },
            View::Time {
                core: u64::MAX,
                kind: 0,
                fsp: u8::MAX,
            },
            View::Float32(0x7ff8_0000_1234_5678),
            View::Vector(&vector),
        ] {
            // No original Datum exists here: only the actual supplied frame is
            // available to reconstruction, including non-SQL raw metadata.
            let frame = encode_native_identity(view).unwrap();
            let value = decode(Some(frame.clone())).unwrap();
            assert_eq!(encode(&value).unwrap(), Some(frame));
        }
        assert_eq!(decode(None).unwrap(), Datum::Null);
        assert!(matches!(
            decode(Some(Vec::new())),
            Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract
        ));
    }
}
