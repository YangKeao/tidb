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

use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;
use tidb_query_expr::{NativeCastStringInput as Input, NativeCastStringTarget as Target};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorLevel, ReadyValuePoolOwner, ReadyValuePoolPolicy};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{BinaryLiteral, Collation, FieldTypeCode, FieldTypeFlags, MysqlEnum};

    #[test]
    fn string_cast_bridge_keeps_source_metadata_effect_order_and_original_packet_handler() {
        #[derive(Default)]
        struct Original {
            events: RefCell<Vec<String>>,
            warnings: RefCell<Vec<(u16, String)>>,
            packet_reads: Cell<usize>,
            strict: Cell<bool>,
        }
        impl Columns for Original {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("actual operand already evaluated")
            }
            fn connection_charset_info(&self) -> (&str, &str) {
                self.events.borrow_mut().push("connection".into());
                ("utf8mb4", "utf8mb4_bin")
            }
            fn max_allowed_packet(&self) -> u64 {
                let reads = self.packet_reads.get();
                self.packet_reads.set(reads + 1);
                let limit = if reads == 0 { 2 } else { 7 };
                self.events.borrow_mut().push(format!("packet:{limit}"));
                limit
            }
            fn truncate_level(&self) -> ErrorLevel {
                self.events.borrow_mut().push("level".into());
                if self.strict.get() {
                    ErrorLevel::Error
                } else {
                    ErrorLevel::Warn
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.events.borrow_mut().push(format!("append:{code}"));
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        let owner = ReadyValuePoolOwner::new(
            ReadyValuePoolPolicy::checked(0, 0, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                .unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let original = Original::default();
        scope.with_columns(&original, |bound| {
            // No new facade or admission policy is added to this pure path.
            let year = FieldType::new(FieldTypeCode::Year);
            assert_eq!(
                eval_cast_char_in(bound, &Datum::Int(0), Some(&year), Some(2), Some("BINARY")),
                Ok(Datum::new_bytes(b"0".to_vec()))
            );
            assert!(original.events.take().is_empty());
            assert_eq!(
                eval_cast_char_in(bound, &Datum::UInt(0), Some(&year), None, Some("binary")),
                Ok(Datum::new_string("0000"))
            );
            assert!(original.events.take().is_empty());
            assert_eq!(
                eval_cast_binary_in(bound, &Datum::Int(0), Some(&year), None),
                Ok(Datum::new_bytes(b"0000".to_vec()))
            );
            assert!(original.events.take().is_empty());
            let unknown_year = FieldType::new(FieldTypeCode::Unknown(13));
            assert_eq!(
                eval_cast_char_in(
                    bound,
                    &Datum::Int(0),
                    Some(&unknown_year),
                    None,
                    Some("utf8mb4")
                ),
                Ok(Datum::new_string("0"))
            );
            assert!(original.events.take().is_empty());
            let raw = Datum::new_bytes(vec![b'a', b'b', 0xff]);
            let unspecified =
                FieldType::new(FieldTypeCode::Unspecified).with_collation_name("binary");
            assert_eq!(
                eval_cast_char_in(bound, &raw, Some(&unspecified), Some(1), Some("utf8mb4")),
                Ok(Datum::new_string("a"))
            );
            assert_eq!(original.events.take(), vec!["append:3854", "append:1406"]);
            assert_eq!(
                original.warnings.take(),
                vec![
                    (
                        3854,
                        "Cannot convert string '6162FF' from binary to utf8mb4".to_owned()
                    ),
                    (1406, "Data Too Long, field len 1, data len 2".to_owned()),
                ]
            );
            let unknown_string =
                FieldType::new(FieldTypeCode::Unknown(253)).with_collation_name("binary");
            let mut flag_only = FieldType::new(FieldTypeCode::VarString)
                .with_charset_name("binary")
                .with_collation_name("utf8mb4_bin");
            flag_only.add_flags(FieldTypeFlags::BINARY);
            let uppercase_collation =
                FieldType::new(FieldTypeCode::VarString).with_collation_name("BINARY");
            let array = FieldType::new(FieldTypeCode::VarString)
                .with_collation_name("binary")
                .with_array(true);
            for source in [&unknown_string, &flag_only, &uppercase_collation, &array] {
                assert_eq!(
                    eval_cast_char_in(bound, &raw, Some(source), None, Some("utf8mb4")),
                    Err(EvalError::Unsupported("invalid UTF-8 string coercion"))
                );
                assert!(original.events.take().is_empty());
            }
            let invalid_enum = Datum::new_enum(MysqlEnum::new(vec![0xff], 1), Collation::Binary);
            assert!(invalid_enum.sql_string().is_err());
            assert_eq!(
                eval_cast_char_in(bound, &invalid_enum, None, None, None),
                Err(EvalError::Unsupported("invalid UTF-8 string coercion"))
            );
            assert_eq!(original.events.take(), vec!["connection"]);
            assert_eq!(
                eval_cast_binary_in(bound, &invalid_enum, None, None),
                Err(EvalError::Unsupported("invalid UTF-8 string coercion"))
            );
            assert!(original.events.take().is_empty());
            for value in [
                Datum::BinaryLiteral(BinaryLiteral::from_uint(255, None)),
                Datum::Bit(BinaryLiteral::from_uint(255, None)),
            ] {
                assert_eq!(
                    eval_cast_binary_in(bound, &value, None, None),
                    Ok(Datum::new_bytes(vec![0xff]))
                );
                assert_eq!(
                    eval_cast_char_in(bound, &value, None, Some(4), Some("BINARY")),
                    Ok(Datum::new_bytes(vec![0xff]))
                );
                assert!(original.events.take().is_empty());
            }
            assert_eq!(
                eval_cast_binary_in(bound, &Datum::new_bytes(b"abcd".to_vec()), None, Some(2)),
                Ok(Datum::new_bytes(b"ab".to_vec()))
            );
            assert_eq!(original.events.take(), vec!["append:1406"]);
            assert_eq!(
                original.warnings.take(),
                vec![(1406, "Data Too Long, field len 2, data len 4".to_owned())]
            );
            let overflow_message =
                "Result of cast_as_binary() was larger than max_allowed_packet (7) - truncated";
            assert_eq!(
                eval_cast_binary_in(bound, &Datum::new_bytes(b"a".to_vec()), None, Some(3)),
                Ok(Datum::Null)
            );
            assert_eq!(
                original.events.take(),
                vec!["packet:2", "packet:7", "level", "append:1301"]
            );
            assert_eq!(
                original.warnings.take(),
                vec![(1301, overflow_message.to_owned())]
            );
            original.packet_reads.set(0);
            original.strict.set(true);
            assert_eq!(
                eval_cast_binary_in(bound, &Datum::new_bytes(b"a".to_vec()), None, Some(3)),
                Err(EvalError::AllowedPacketOverflowed(
                    overflow_message.to_owned()
                ))
            );
            assert_eq!(
                original.events.take(),
                vec!["packet:2", "packet:7", "level"]
            );
            assert!(original.warnings.take().is_empty());
            original.packet_reads.set(0);
            assert_eq!(
                eval_cast_binary_in(bound, &Datum::new_bytes(b"a".to_vec()), None, Some(2)),
                Ok(Datum::new_bytes(vec![b'a', 0]))
            );
            assert_eq!(original.events.take(), vec!["packet:2"]);
            assert!(original.warnings.take().is_empty());
        });
        drop(scope);
        execution.close();
    }
}

fn input(value: &Datum) -> Input<'_> {
    match value {
        Datum::Int(value) => Input::Int(*value),
        Datum::UInt(value) => Input::UInt(*value),
        Datum::String(value) => Input::String(value.bytes()),
        Datum::Bytes(value) => Input::Bytes(value),
        Datum::BinaryLiteral(value) => Input::BinaryLiteral(value.as_bytes()),
        Datum::Bit(value) => Input::Bit(value.as_bytes()),
        _ => Input::Other,
    }
}

fn evaluate(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    target: Target<'_>,
) -> Result<Datum, EvalError> {
    let source = source.map(|field| tidb_query_expr::NativeCastStringSource {
        code: field.code().as_shared_string_type(),
        collation: field.collation_name(),
    });
    let result = tidb_query_expr::native_cast_string(
        input(value),
        target,
        source,
        || ctx.connection_charset_info().0,
        || value.sql_string(),
        || ctx.max_allowed_packet(),
        // The original handler owns any repeated limit/policy reads. Do not
        // substitute a cached limit or a precomputed warning here.
        |name| ctx.handle_allowed_packet_overflowed(name),
        |code, message| ctx.append_warning(code, message),
    )
    .map_err(|error| match error {
        tidb_query_expr::NativeCastStringError::Child(error) => error,
        tidb_query_expr::NativeCastStringError::InvalidUtf8StringCoercion => {
            EvalError::Unsupported("invalid UTF-8 string coercion")
        }
    })?;
    Ok(match result {
        tidb_query_expr::NativeCastStringResult::Null => Datum::Null,
        tidb_query_expr::NativeCastStringResult::Text(value) => Datum::new_string(value),
        tidb_query_expr::NativeCastStringResult::Bytes(value) => Datum::new_bytes(value),
    })
}

pub(crate) fn eval_cast_char_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    len: Option<u32>,
    charset: Option<&str>,
) -> Result<Datum, EvalError> {
    evaluate(ctx, value, source, Target::Char { len, charset })
}

pub(crate) fn eval_cast_binary_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    len: Option<u32>,
) -> Result<Datum, EvalError> {
    evaluate(ctx, value, source, Target::Binary { len })
}
