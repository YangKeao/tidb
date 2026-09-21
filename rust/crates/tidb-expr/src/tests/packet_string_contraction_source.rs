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

//! Contraction receipts for the seven source tests deleted with
//! `string_packet.rs`. The original input tables remain visible here, but no
//! test-only copy of the removed allocation, encoding, padding, or sort-key
//! kernels is retained.

use std::cell::RefCell;

use super::assert_packet_string_refusal;
use crate::{Columns, Datum, Decimal, EvalError};

const REMOVED: EvalError = EvalError::Unsupported(
    "native packet-limited string evaluation was removed; function unsupported",
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

struct PacketCtx {
    limit: u64,
    warnings: RefCell<Vec<(u16, String)>>,
}

impl PacketCtx {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            warnings: RefCell::new(Vec::new()),
        }
    }
}

impl Columns for PacketCtx {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn max_allowed_packet(&self) -> u64 {
        self.limit
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
}

#[test]
fn max_allowed_packet_overflow_rows_are_explicitly_contracted() {
    let ctx = PacketCtx::new(1_000);
    for (name, vals) in [
        ("SPACE", vec![Datum::Int(1_001)]),
        ("REPEAT", vec![Datum::new_string("a"), Datum::Int(1_001)]),
        (
            "LPAD",
            vec![
                Datum::new_string("a"),
                Datum::Int(251),
                Datum::new_string("x"),
            ],
        ),
        (
            "RPAD",
            vec![
                Datum::new_string("a"),
                Datum::Int(251),
                Datum::new_string("x"),
            ],
        ),
        ("TO_BASE64", vec![Datum::new_string("a".repeat(742))]),
    ] {
        assert_values(name, &vals, &ctx);
    }
    assert!(
        ctx.warnings.borrow().is_empty(),
        "deleted kernels cannot warn"
    );
}

#[test]
fn space_source_scalar_vectors_are_explicitly_contracted() {
    for input in [
        Datum::Int(0),
        Datum::Int(3),
        Datum::Int(16_777_217),
        Datum::Int(-1),
        Datum::new_string("abc"),
        Datum::new_string("3"),
        Datum::Real(1.2),
        Datum::Real(1.9),
        Datum::Decimal(Decimal::from_literal("2.5")),
        Datum::Real(2.5),
        Datum::Null,
    ] {
        assert_values("SPACE", &[input], &crate::NoColumns);
    }
    assert_values("SPACE", &[], &crate::NoColumns);
}

#[test]
fn pad_truncation_and_character_count_rows_are_explicitly_contracted() {
    for expr in [
        "lpad('hi', 1, '??')",
        "rpad('hi', 1, '??')",
        "lpad('hi', 5, '??')",
        "rpad('hi', 5, '??')",
        "lpad('好', 2, 'xy')",
        "lpad('hi', -1, '??')",
    ] {
        assert_packet_string_refusal(expr);
    }
}

#[test]
fn to_base64_wrap_boundaries_are_explicitly_contracted() {
    for byte_count in [57, 58, 114] {
        assert_values(
            "TO_BASE64",
            &[Datum::new_bytes(vec![b'a'; byte_count])],
            &crate::NoColumns,
        );
    }
}

#[test]
fn to_base64_null_is_explicitly_contracted() {
    assert_values("TO_BASE64", &[Datum::Null], &crate::NoColumns);
}

#[test]
fn to_base64_packet_limit_rows_are_explicitly_contracted() {
    const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for (input, limit) in [
        ("abc".to_owned(), 3),
        (ALPHABET.to_owned(), 88),
        (ALPHABET.repeat(3), 258),
    ] {
        let ctx = PacketCtx::new(limit);
        assert_values("TO_BASE64", &[Datum::new_string(input)], &ctx);
        assert!(ctx.warnings.borrow().is_empty());
    }
}

#[test]
fn ci_weight_string_source_shapes_are_explicitly_contracted() {
    for collation in [
        "utf8mb4_general_ci",
        "utf8mb4_unicode_ci",
        "utf8mb4_0900_ai_ci",
    ] {
        for (text, suffix) in [
            ("aAÁàãăâ", ""),
            ("中", ""),
            ("a", " as char(5)"),
            ("a ", " as char(5)"),
            ("中", " as char(5)"),
            ("中 ", " as char(5)"),
            ("a", " as binary(1)"),
            ("ab", " as binary(1)"),
            ("a", " as binary(5)"),
            ("a ", " as binary(5)"),
            ("中", " as binary(1)"),
            ("中", " as binary(2)"),
            ("中", " as binary(3)"),
            ("中", " as binary(5)"),
        ] {
            assert_packet_string_refusal(&format!(
                "hex(weight_string('{text}' collate {collation}{suffix}))"
            ));
        }
    }
}
