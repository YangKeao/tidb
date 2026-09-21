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

//! Contraction receipts for the source vectors formerly owned by
//! `builtin_ext/string2.rs` and the SUBSTRING/FORMAT/EXPORT_SET helpers removed
//! from `string_fn.rs`. Inputs remain visible without retaining a test-only
//! implementation of the deleted kernels.

use std::cell::RefCell;

#[cfg(feature = "tikv-expr")]
use super::engine_e;
use crate::{Columns, Datum, Decimal, EvalError};

const REMOVED: EvalError = EvalError::Unsupported(
    "native string2 evaluation was removed; TiKV engine required or function unsupported",
);

fn s(value: &str) -> Datum {
    Datum::new_string(value)
}

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
fn substring_alias_and_boundary_rows_are_explicitly_contracted() {
    for (name, vals) in [
        ("SUBSTRING", vec![s("Sakila"), Datum::Int(2)]),
        ("SUBSTR", vec![s("Sakila"), Datum::Int(-2)]),
        ("MID", vec![s("Sakila"), Datum::Int(2), Datum::Int(3)]),
        ("SUBSTRING", vec![s("Sakila"), Datum::Int(0), Datum::Int(2)]),
        (
            "SUBSTRING",
            vec![s("Sakila"), Datum::Int(1), Datum::Int(i64::MAX)],
        ),
        (
            "SUBSTRING",
            vec![Datum::new_bytes(vec![0xff]), Datum::Int(1)],
        ),
        ("SUBSTRING", vec![Datum::Null, Datum::Int(1)]),
    ] {
        assert_values(name, &vals, &crate::NoColumns);
    }
}

#[test]
fn locate_and_find_in_set_collation_rows_are_explicitly_contracted() {
    for (name, vals) in [
        ("LOCATE", vec![s("bar"), s("foobarbar")]),
        ("LOCATE", vec![s("bar"), s("foobarbar"), Datum::Int(5)]),
        (
            "LOCATE",
            vec![
                Datum::new_bytes(vec![0xff]),
                Datum::new_bytes(vec![0, 0xff]),
            ],
        ),
        ("FIND_IN_SET", vec![s("b"), s("a,b,c")]),
        ("FIND_IN_SET", vec![s("a,b"), s("a,b,c")]),
        (
            "FIND_IN_SET",
            vec![
                Datum::new_bytes(vec![0xff]),
                Datum::new_bytes(vec![0, b',', 0xff]),
            ],
        ),
        ("FIND_IN_SET", vec![Datum::Null, s("a,b")]),
    ] {
        assert_values(name, &vals, &crate::NoColumns);
    }
}

#[test]
fn format_rows_refuse_before_numeric_coercion_or_warning() {
    let ctx = WarningContext::default();
    for vals in [
        vec![Datum::Decimal(Decimal::from_literal("2.5")), Datum::Int(0)],
        vec![
            Datum::Decimal(Decimal::from_literal("1234567.891")),
            Datum::Int(2),
        ],
        vec![s("12abc"), Datum::Int(2)],
        vec![Datum::Int(1234), Datum::Int(2), s("de_DE")],
        vec![Datum::Int(1234), Datum::Int(2), Datum::Null],
    ] {
        assert_values("FORMAT", &vals, &ctx);
    }
    assert!(ctx.0.borrow().is_empty(), "deleted FORMAT cannot warn");
}

#[test]
fn export_set_arity_bit_and_null_rows_are_explicitly_contracted() {
    for vals in [
        vec![Datum::Int(5), s("Y"), s("N")],
        vec![Datum::Int(9), s("Y"), s("N"), s("@")],
        vec![Datum::Int(5), s("Y"), s("N"), s(","), Datum::Int(0)],
        vec![Datum::Int(5), s("Y"), s("N"), s(","), Datum::Int(100)],
        vec![Datum::Null, s("Y"), s("N")],
        vec![Datum::Int(5), Datum::Null, s("N")],
        vec![Datum::Int(5), s("Y"), s("N"), Datum::Null],
    ] {
        assert_values("EXPORT_SET", &vals, &crate::NoColumns);
    }
}

#[test]
fn trim_and_translate_native_rows_are_explicitly_contracted() {
    for (name, vals) in [
        ("LTRIM", vec![s("  a  ")]),
        ("RTRIM", vec![s("  a  ")]),
        ("LTRIM", vec![s("   ")]),
        ("RTRIM", vec![s("")]),
        ("LTRIM", vec![Datum::new_bytes(vec![b' ', 0xff])]),
        ("TRANSLATE", vec![s("12345"), s("123"), s("abc")]),
        ("TRANSLATE", vec![s("中文测试"), s("中文"), s("AB")]),
        ("TRANSLATE", vec![Datum::Null, s("a"), s("b")]),
    ] {
        assert_values(name, &vals, &crate::NoColumns);
    }
}

#[cfg(feature = "tikv-expr")]
#[test]
fn trim_rows_execute_only_in_tikv() {
    for (expr, answer) in [
        ("ltrim('  a  ')", "STR:a  "),
        ("rtrim('  a  ')", "STR:  a"),
        ("ltrim('   ')", "STR:"),
        ("rtrim('')", "STR:"),
        ("ltrim(null)", "NULL"),
    ] {
        assert_eq!(engine_e(expr), answer, "TiKV engine: {expr}");
    }
}
