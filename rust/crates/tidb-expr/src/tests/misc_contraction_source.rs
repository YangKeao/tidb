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

//! One-for-one contraction receipts for the thirteen tests deleted with
//! `builtin_ext/misc.rs`. The former source inputs stay visible without a
//! test-only copy of UUID parsing/generation, Vitess hashing, or identity
//! kernels.

use std::cell::RefCell;

use super::assert_misc_refusal;
use crate::{Columns, Datum, Decimal, EvalError};
use tidb_datatype::{BinaryLiteral, Collation, MysqlEnum, MysqlSet};

const REMOVED: EvalError = EvalError::Unsupported(
    "native miscellaneous evaluation was removed; TiKV engine required or function unsupported",
);

fn assert_values(name: &str, vals: &[Datum], ctx: &dyn Columns) {
    assert_eq!(
        crate::func::eval_func_values_in(name, vals, ctx),
        Some(Err(REMOVED)),
        "{name} {vals:?}"
    );
    assert_eq!(
        crate::func::eval_func_values(name, vals, ctx),
        Some(Err(REMOVED)),
        "low-level {name} {vals:?}"
    );
}

#[derive(Default)]
struct WarningContext(RefCell<Vec<(u16, String)>>);

impl Columns for WarningContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.0.borrow_mut().push((code, message.to_owned()));
    }
}

#[test]
fn any_value_returns_its_argument_is_native_refusal() {
    for value in [
        Datum::Null,
        Datum::Int(1234),
        Datum::Int(-0x99),
        Datum::Real(3.1415926),
        Datum::new_string("Hello, World"),
    ] {
        assert_values("ANY_VALUE", &[value], &crate::NoColumns);
    }
}

#[test]
fn any_value_hybrid_rows_are_native_refusal() {
    for value in [
        Datum::Enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin),
        Datum::Set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin),
        Datum::Bit(BinaryLiteral::from(vec![0x01])),
    ] {
        assert_values("ANY_VALUE", &[value], &crate::NoColumns);
    }
}

#[test]
fn removed_misc_names_refuse_before_arity_and_foreign_names_do_not_match() {
    for (name, vals) in [
        ("ANY_VALUE", vec![]),
        ("ANY_VALUE", vec![Datum::Int(1), Datum::Int(2)]),
        ("NAME_CONST", vec![]),
        ("NAME_CONST", vec![Datum::new_string("name")]),
        (
            "NAME_CONST",
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
        ),
    ] {
        assert_values(name, &vals, &crate::NoColumns);
    }
    assert!(!crate::func::is_removed_native_misc("UNKNOWN"));
}

#[test]
fn name_const_source_domains_are_native_refusal() {
    for vals in [
        vec![Datum::new_string("test_int"), Datum::Int(3)],
        vec![Datum::new_string("test_uint"), Datum::UInt(u64::MAX)],
        vec![Datum::new_string("test_float"), Datum::Real(3.14159)],
        vec![Datum::new_string("test_string"), Datum::new_string("TiDB")],
        vec![
            Datum::new_string("test_binary"),
            Datum::new_bytes(vec![0, 0xff, 0x80]),
        ],
        vec![Datum::new_string("test_null"), Datum::Null],
        vec![
            Datum::new_string("test_decimal"),
            Datum::Decimal(Decimal::from_literal("123.123")),
        ],
        vec![Datum::Null, Datum::Int(-7)],
    ] {
        assert_values("NAME_CONST", &vals, &crate::NoColumns);
    }
}

#[test]
fn uuid_version_source_rows_are_explicitly_contracted() {
    for text in [
        "5f13f854-d74a-11f0-9b7a-0ae0156bd76b",
        "c6437ef1-5b86-3a4e-a071-c2d4ad414e65",
        "a3e3b4a1-ea6d-471e-9860-8303a8b261f6",
        "271a8175-dadd-5df9-b0bd-20a4a0b441e6",
        "1f0e48c1-7860-69cc-9b3f-35f89c103d4d",
        "019b1440-87b7-7380-ab00-ce413e795004",
        "6ccd780cbaba102695645b8c656024db",
        "urn:uuid:6ccd780c-baba-1026-9564-5b8c656024db",
        "{99a9ad03-5298-11ec-8f5c-00ff90147ac3*",
        "123e4567-e89b-02d3-a456-426614174000",
        "abc",
    ] {
        assert_values(
            "UUID_VERSION",
            &[Datum::new_string(text)],
            &crate::NoColumns,
        );
    }
    assert_values("UUID_VERSION", &[Datum::Null], &crate::NoColumns);
}

#[test]
fn is_uuid_source_rows_are_explicitly_contracted() {
    for text in [
        "6ccd780c-baba-1026-9564-5b8c656024db",
        "6CCD780C-BABA-1026-9564-5B8C656024DB",
        "6ccd780cbaba102695645b8c656024db",
        "{6ccd780c-baba-1026-9564-5b8c656024db}",
        "6ccd780c-baba-1026-9564-5b8c6560",
        "6CCD780C-BABA-1026-9564-5B8C656024DQ",
        " 6ccd780c-baba-1026-9564-5b8c656024db",
        "6ccd780c-baba-1026-9564-5b8c656024db ",
        " 6ccd780c-baba-1026-9564-5b8c656024db ",
        "{99a9ad03-5298-11ec-8f5c-00ff90147ac3*",
        "urn:uuid:99a9ad03-5298-11ec-8f5c-00ff90147ac3",
    ] {
        assert_values("IS_UUID", &[Datum::new_string(text)], &crate::NoColumns);
    }
    assert_values("IS_UUID", &[Datum::Null], &crate::NoColumns);
}

#[test]
fn is_uuid_scalar_coercion_rows_are_explicitly_contracted() {
    for value in [Datum::Int(1), Datum::Real(1.0)] {
        assert_values("IS_UUID", &[value], &crate::NoColumns);
    }
}

#[test]
fn is_uuid_byte_boundaries_are_explicitly_contracted() {
    let mut invalid_wrapper = vec![0xff];
    invalid_wrapper.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3*");
    let mut leading_space = vec![b' '];
    leading_space.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3");
    leading_space.push(0xff);
    for value in [
        Datum::new_string(vec![0xff]),
        Datum::Bytes(vec![0xff]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
        Datum::Bytes(invalid_wrapper),
        Datum::Bytes(leading_space),
    ] {
        assert_values("IS_UUID", &[value], &crate::NoColumns);
    }
}

#[test]
fn uuid_timestamp_source_rows_are_explicitly_contracted() {
    for text in [
        "5f13f854-d74a-11f0-9b7a-0ae0156bd76b",
        "c6437ef1-5b86-3a4e-a071-c2d4ad414e65",
        "a3e3b4a1-ea6d-471e-9860-8303a8b261f6",
        "271a8175-dadd-5df9-b0bd-20a4a0b441e6",
        "1f0e48c1-7860-69cc-9b3f-35f89c103d4d",
        "019b1440-87b7-7380-ab00-ce413e795004",
        "6ccd780cbaba102695645b8c656024db",
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "abc",
    ] {
        assert_values(
            "UUID_TIMESTAMP",
            &[Datum::new_string(text)],
            &crate::NoColumns,
        );
    }
    assert_values("UUID_TIMESTAMP", &[Datum::Null], &crate::NoColumns);
}

#[test]
fn uuid_binary_source_rows_are_explicitly_contracted() {
    let canonical = "6ccd780c-baba-1026-9564-5b8c656024db";
    let normal = vec![
        0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24,
        0xdb,
    ];
    let swapped = vec![
        0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24,
        0xdb,
    ];
    for spelling in [
        canonical,
        "6CCD780C-BABA-1026-9564-5B8C656024DB",
        "6ccd780cbaba102695645b8c656024db",
        "{6ccd780c-baba-1026-9564-5b8c656024db}",
        "6ccd780c-baba-1026-9564-5b8c6560",
        " 6ccd780c-baba-1026-9564-5b8c656024db",
        "6ccd780c-baba-1026-9564-5b8c656024db ",
        " 6ccd780c-baba-1026-9564-5b8c656024db ",
    ] {
        assert_values(
            "UUID_TO_BIN",
            &[Datum::new_string(spelling)],
            &crate::NoColumns,
        );
    }
    for vals in [
        vec![Datum::new_string(canonical), Datum::Int(1)],
        vec![Datum::new_string(canonical), Datum::Null],
        vec![Datum::new_string(canonical), Datum::new_string("a")],
        vec![Datum::Null],
        vec![],
        vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
    ] {
        assert_values("UUID_TO_BIN", &vals, &crate::NoColumns);
    }
    for vals in [
        vec![Datum::Bytes(normal.clone())],
        vec![Datum::Bytes(normal.clone()), Datum::Int(1)],
        vec![Datum::Bytes(normal.clone()), Datum::new_string("a")],
        vec![Datum::Bytes(swapped)],
        vec![Datum::Bytes(normal[..15].to_vec())],
        vec![Datum::Null],
        vec![],
        vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)],
    ] {
        assert_values("BIN_TO_UUID", &vals, &crate::NoColumns);
    }
}

#[test]
fn tidb_shard_source_rows_are_explicitly_contracted() {
    for vals in [vec![], vec![Datum::Int(1), Datum::Int(2)]] {
        assert_values("TIDB_SHARD", &vals, &crate::NoColumns);
    }
    for value in [
        Datum::Int(-1),
        Datum::Int(0),
        Datum::Int(1),
        Datum::Int(9_999_999_999_999_999),
        Datum::UInt(u64::MAX),
        Datum::new_string("abc"),
        Datum::new_string("ope"),
        Datum::new_string("wopddd"),
        Datum::new_string("1"),
        Datum::new_string("-1"),
        Datum::new_string("1.9"),
        Datum::Real(1.9),
        Datum::Decimal(Decimal::from_literal("1.9")),
        Datum::Null,
    ] {
        assert_values("TIDB_SHARD", &[value], &crate::NoColumns);
    }
}

#[test]
fn hash_warning_rows_refuse_without_emitting_native_warnings() {
    for (name, text) in [
        ("VITESS_HASH", "18446744073709551614"),
        ("TIDB_SHARD", "18446744073709551614"),
        ("VITESS_HASH", "18446744073709551616"),
    ] {
        let ctx = WarningContext::default();
        assert_values(name, &[Datum::new_string(text)], &ctx);
        assert!(ctx.0.borrow().is_empty(), "deleted {name} kernel warned");
    }
}

#[test]
fn vitess_hash_source_rows_are_explicitly_contracted() {
    for value in [
        Datum::Int(30_375_298_039),
        Datum::Int(1123),
        Datum::Int(30_573_721_600),
        Datum::Int(116),
        Datum::Int(1),
        Datum::Int(0),
        Datum::UInt(u64::MAX),
        Datum::Null,
    ] {
        assert_values("VITESS_HASH", &[value], &crate::NoColumns);
    }
}

#[cfg(feature = "tikv-expr")]
#[test]
fn any_value_scalar_source_rows_execute_only_in_tikv() {
    for (expr, expected) in [
        ("any_value(null)", "NULL"),
        ("any_value(1234)", "INT:1234"),
        ("any_value(-153)", "INT:-153"),
        ("any_value(3.1415926)", "DEC:3.1415926"),
        ("any_value('Hello, World')", "STR:Hello, World"),
    ] {
        assert_eq!(super::engine_e(expr), expected, "{expr}");
        assert_misc_refusal(expr);
    }
}
