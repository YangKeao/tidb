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

use crate::{Datum, EvalError};
use tidb_datatype::FieldType;
use tidb_query_datatype::codec::native_vector_convert::NativeVectorConvertInput as Input;

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, VectorFloat32};

    #[test]
    fn vector_cast_bridge_preserves_raw_clone_strict_text_errors_and_actual_source_names() {
        let mut vector = VectorFloat32::init(3);
        vector.elements_mut().copy_from_slice(&[
            f32::from_bits(0x7fa1_2345),
            f32::NEG_INFINITY,
            -0.0,
        ]);
        let raw = vector.serialize();
        let original = Datum::new_vector_float32(vector);
        let Datum::VectorFloat32(mut cloned) = eval_cast_vector(&original, None, Some(3)).unwrap()
        else {
            panic!("vector cast must return the shared vector carrier");
        };
        assert_eq!(cloned.serialize(), raw);
        cloned.elements_mut()[0] = 1.0;
        let Datum::VectorFloat32(original_vector) = &original else {
            unreachable!()
        };
        assert_eq!(original_vector.serialize(), raw);
        assert_eq!(
            eval_cast_vector(&original, None, Some(2)),
            Err(EvalError::Vector(
                "vector has 3 dimensions, does not fit VECTOR(2)".into()
            ))
        );
        for value in [
            Datum::new_string(vec![b'[', 0xff, b']']),
            Datum::new_bytes(vec![b'[', 0xff, b']']),
        ] {
            assert_eq!(
                eval_cast_vector(&value, None, Some(0)),
                Err(EvalError::Vector(
                    "invalid utf-8 sequence of 1 bytes from index 1".into()
                ))
            );
        }
        let parse_error = "Invalid vector text: [1,]";
        for value in [
            Datum::new_string("[1,]"),
            Datum::new_bytes(b"[1,]".to_vec()),
        ] {
            assert_eq!(
                eval_cast_vector(&value, None, Some(0)),
                Err(EvalError::Vector(parse_error.into()))
            );
        }
        for value in [
            Datum::new_string("[1,2]"),
            Datum::new_bytes(b"[1,2]".to_vec()),
        ] {
            assert_eq!(
                eval_cast_vector(&value, None, None),
                Ok(Datum::new_vector_float32(VectorFloat32::must_create(vec![
                    1.0, 2.0
                ])))
            );
            assert_eq!(
                eval_cast_vector(&value, None, Some(1)),
                Err(EvalError::Vector(
                    "vector has 2 dimensions, does not fit VECTOR(1)".into()
                ))
            );
        }
        assert_eq!(
            eval_cast_vector(&Datum::Int(7), None, None),
            Err(EvalError::Vector(
                "cannot cast from unspecified to vector".into()
            ))
        );
        for (code, name) in [
            (FieldTypeCode::Unknown(13), ""),
            (FieldTypeCode::Unknown(253), ""),
            (FieldTypeCode::NewDate, ""),
            (FieldTypeCode::Year, "year"),
            (FieldTypeCode::VarString, "var_string"),
            (FieldTypeCode::Blob, "text"),
        ] {
            let source = FieldType::new(code)
                .with_charset_name("binary")
                .with_collation_name("binary");
            assert_eq!(
                eval_cast_vector(&Datum::Int(7), Some(&source), Some(2)),
                Err(EvalError::Vector(format!(
                    "cannot cast from {name} to vector"
                )))
            );
        }
        let array = FieldType::new(FieldTypeCode::LongLong).with_array(true);
        assert_eq!(
            eval_cast_vector(&Datum::Int(7), Some(&array), None),
            Err(EvalError::Vector("cannot cast from json to vector".into()))
        );
        let target = tidb_ast::CastType::Vector {
            dimensions: Some(2),
        };
        assert_eq!(
            crate::cast::eval_cast(&target, Datum::Null, None, &crate::context::NoColumns),
            Ok(Datum::Null)
        );
        assert_eq!(
            crate::cast::eval_cast(&target, Datum::MaxValue, None, &crate::context::NoColumns),
            Err(EvalError::Unsupported("range sentinel cast operand"))
        );
    }
}

pub(crate) fn eval_cast_vector(
    value: &Datum,
    source: Option<&FieldType>,
    dimensions: Option<u32>,
) -> Result<Datum, EvalError> {
    let input = match value {
        Datum::Null => None,
        Datum::VectorFloat32(value) => Some(Input::Vector(value)),
        Datum::String(value) => Some(Input::String(value.bytes())),
        Datum::Bytes(value) => Some(Input::Bytes(value)),
        _ => Some(Input::Other),
    };
    tidb_query_expr::native_cast_vector(
        input,
        dimensions,
        source.map(|field| field.code().as_shared_type_name_code()),
    )
    .map(|value| match value {
        Some(value) => Datum::new_vector_float32(value),
        None => Datum::Null,
    })
    .map_err(EvalError::Vector)
}
