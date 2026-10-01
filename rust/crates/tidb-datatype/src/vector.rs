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

//! `pkg/types/vector.go` and `vector_functions.go` shared native-policy facade.

pub use tidb_query_datatype::codec::mysql::{
    check_native_vector_dim_valid as check_vector_dim_valid,
    deserialize_native_vector_float32 as deserialize_vector_float32,
    peek_native_vector_float32 as peek_vector_float32, NativeVectorError as VectorError,
    NativeVectorFloat32 as VectorFloat32, NATIVE_MAX_VECTOR_DIMENSION as MAX_VECTOR_DIMENSION,
};

#[cfg(test)]
use std::cmp::Ordering;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_endianess() {
        let mut vector = VectorFloat32::init(2);
        vector.elements_mut().copy_from_slice(&[1.1, 2.2]);
        assert_eq!(
            vector.serialize(),
            [2, 0, 0, 0, 0xCD, 0xCC, 0x8C, 0x3F, 0xCD, 0xCC, 0x0C, 0x40]
        );
    }

    #[test]
    fn test_zero_vector() {
        let zero = VectorFloat32::default();
        assert!(zero.is_zero_value());
        assert_eq!(zero.compare(&zero), Ordering::Equal);
        assert_eq!(zero.serialize(), [0, 0, 0, 0]);
        assert_eq!(zero.serialized_size(), 4);
        assert_eq!(zero.to_string(), "[]");

        let mut prefixed = vec![1, 2, 3];
        zero.serialize_to(&mut prefixed);
        assert_eq!(prefixed, [1, 2, 3, 0, 0, 0, 0]);

        let serialized = zero.serialize();
        let (round_trip, remaining) = deserialize_vector_float32(&serialized).unwrap();
        assert!(remaining.is_empty());
        assert!(round_trip.is_zero_value());
        assert_eq!(round_trip.len(), 0);
        assert_eq!(round_trip.to_string(), "[]");
        assert_eq!(round_trip.compare(&zero), Ordering::Equal);
        assert_eq!(zero.compare(&round_trip), Ordering::Equal);
    }

    #[test]
    fn test_vector_parse() {
        for invalid in [
            "abc",
            "null",
            "\"json_str\"",
            "123",
            "[123",
            "123]",
            "[123,]",
        ] {
            assert!(VectorFloat32::parse(invalid).is_err(), "{invalid}");
        }
        let zero = VectorFloat32::default();
        assert_eq!(VectorFloat32::parse("[]").unwrap(), zero);
        let parsed = VectorFloat32::parse("[1.1, 2.2, 3.3]").unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed.to_string(), "[1.1,2.2,3.3]");
        assert!(!parsed.is_zero_value());
        assert_eq!(parsed.compare(&zero), Ordering::Greater);
        assert_eq!(zero.compare(&parsed), Ordering::Less);
        assert_eq!(
            VectorFloat32::parse("[-1e39, 1e39]")
                .unwrap_err()
                .to_string(),
            "value -1e+39 out of range for float32"
        );
        assert!(check_vector_dim_valid(-1).is_err());
        for invalid in ["[1,2,3,4.4]ddddddddddddfasfa", "[1,2,3]extra"] {
            assert!(
                VectorFloat32::parse(invalid)
                    .unwrap_err()
                    .to_string()
                    .contains("Invalid vector text"),
                "{invalid}"
            );
        }
    }

    #[test]
    fn test_vector_datum() {
        let datum = crate::Datum::new_vector_float32(VectorFloat32::default());
        let crate::Datum::VectorFloat32(vector) = datum else {
            panic!("expected vector datum")
        };
        assert_eq!(vector.len(), 0);
        assert_eq!(vector.to_string(), "[]");
        assert!(vector.is_zero_value());
        assert_eq!(vector.compare(&VectorFloat32::default()), Ordering::Equal);
        assert_eq!(VectorFloat32::default().compare(&vector), Ordering::Equal);
    }

    #[test]
    fn test_vector_compare() {
        let parsed = VectorFloat32::parse("[1.1, 2.2, 3.3]").unwrap();
        let other = VectorFloat32::parse("[-1.1, 4.2]").unwrap();
        assert_eq!(parsed.compare(&other), Ordering::Greater);
        assert_eq!(other.compare(&parsed), Ordering::Less);
        let other = VectorFloat32::parse("[1.1, 4.2]").unwrap();
        assert_eq!(parsed.compare(&other), Ordering::Less);
        assert_eq!(other.compare(&parsed), Ordering::Greater);
    }

    #[test]
    fn test_vector_serialize() {
        let parsed = VectorFloat32::parse("[1.1, 2.2, 3.3]").unwrap();
        let mut serialized = parsed.serialize();
        serialized.extend_from_slice(&[1, 2, 3, 4]);
        let (round_trip, remaining) = deserialize_vector_float32(&serialized).unwrap();
        assert_eq!(round_trip, parsed);
        assert_eq!(remaining, [1, 2, 3, 4]);
        assert!(deserialize_vector_float32(&[0xF1, 0xFC]).is_err());
    }

    // Go: pkg/types/vector_test.go::TestVectorDeserializeOverflow
    // 0x40000000 elements overflows the uint32 size computation
    // (0x40000000*4 + 4 wraps to 4), so without checked arithmetic the header
    // would be accepted with a 4-byte buffer and a huge dimension that
    // Elements() would read out of bounds. The Rust port must reject it too;
    // unlike Go's `(v, err)` signature, the Err branch carries no value, so
    // Go's trailing `v.IsZeroValue()` assertion has no Rust counterpart.
    #[test]
    fn test_vector_deserialize_overflow() {
        let b: [u8; 4] = [0x00, 0x00, 0x00, 0x40];
        assert!(peek_vector_float32(&b).is_err());
        assert!(deserialize_vector_float32(&b).is_err());
        assert!(VectorFloat32::default().is_zero_value());
    }

    #[test]
    fn vector_functions_cover_source_precision_errors_and_edge_cases() {
        let left = VectorFloat32::must_create(vec![1.0, 2.0, 3.0]);
        let right = VectorFloat32::must_create(vec![4.0, 5.0, 6.0]);
        assert_eq!(left.l2_squared_distance(&right).unwrap(), 27.0);
        assert_eq!(left.l2_distance(&right).unwrap(), 27_f64.sqrt());
        assert_eq!(left.inner_product(&right).unwrap(), 32.0);
        assert_eq!(left.negative_inner_product(&right).unwrap(), -32.0);
        assert_eq!(left.l1_distance(&right).unwrap(), 9.0);
        assert_eq!(left.add(&right).unwrap().elements(), [5.0, 7.0, 9.0]);
        assert_eq!(right.sub(&left).unwrap().elements(), [3.0, 3.0, 3.0]);
        assert_eq!(left.mul(&right).unwrap().elements(), [4.0, 10.0, 18.0]);
        assert!(left.add(&VectorFloat32::must_create(vec![1.0])).is_err());
        assert!(VectorFloat32::must_create(vec![f32::MAX])
            .add(&VectorFloat32::must_create(vec![f32::MAX]))
            .is_err());
        assert!(VectorFloat32::default()
            .cosine_distance(&VectorFloat32::default())
            .unwrap()
            .is_nan());
    }
}
