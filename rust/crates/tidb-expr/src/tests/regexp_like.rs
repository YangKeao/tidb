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

//! Rewritten-expression coverage for `REGEXP_LIKE`.

use super::{e as removed_native_e, engine_declines, engine_e};
use crate::scalar_function::ScalarFunction;
use crate::{EvalError, NoColumns};
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};

fn assert_native_refusal(expr: &str) {
    assert_eq!(
        removed_native_e(expr),
        "Unsupported(\"native regexp evaluation was removed; TiKV engine required\")",
        "{expr}"
    );
}

#[test]
fn every_regexp_name_refuses_both_native_scalar_boundaries() {
    const REFUSAL: EvalError =
        EvalError::Unsupported("native regexp evaluation was removed; TiKV engine required");
    for name in [
        "regexp",
        "rlike",
        "regexp_like",
        "regexp_substr",
        "regexp_instr",
        "regexp_replace",
    ] {
        let scalar = ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            Vec::new(),
        );
        assert_eq!(
            scalar.eval(&NoColumns, tidb_chunk::row::Row::empty()),
            Err(REFUSAL),
            "ScalarFunction::{name}"
        );
        assert_eq!(
            crate::func::eval_func_values_in(name, &[], &NoColumns),
            Some(Err(REFUSAL)),
            "value dispatcher::{name}"
        );
    }
}

/// Scalar rows from `pkg/expression/builtin_regexp_test.go:210`
/// (`TestRegexpLike`). Independent expected values run through TiKV; direct AST
/// evaluation must return the exact no-native-fallback refusal.
#[test]
fn regexp_like_source_vectors_reach_the_rewritten_evaluator() {
    for (expr, want) in [
        ("regexp_like('a', '^$')", "INT:0"),
        ("regexp_like('a', 'a')", "INT:1"),
        ("regexp_like('b', 'a')", "INT:0"),
        ("regexp_like('aA', 'aA')", "INT:1"),
        ("regexp_like('a', '.')", "INT:1"),
        ("regexp_like('ab', '^.$')", "INT:0"),
        ("regexp_like('b', '..')", "INT:0"),
        ("regexp_like('aab', '.ab')", "INT:1"),
        ("regexp_like('abcd', '.*')", "INT:1"),
        ("regexp_like('abc', 'AbC')", "INT:0"),
        ("regexp_like('abc', 'AbC', 'i')", "INT:1"),
        ("regexp_like('123\n321', '23$')", "INT:0"),
        ("regexp_like('123\n321', '23$', 'm')", "INT:1"),
        ("regexp_like('good\nday', '^day', 'm')", "INT:1"),
        ("regexp_like('\n', '.')", "INT:0"),
        ("regexp_like('\n', '.', 's')", "INT:1"),
        ("regexp_like('abc', 'aBc', 'ic')", "INT:0"),
        ("regexp_like('abc', 'aBc', 'ci')", "INT:1"),
        ("regexp_like(NULL, 'a')", "NULL"),
        ("regexp_like('a', NULL)", "NULL"),
        ("regexp_like('a', 'a', NULL)", "NULL"),
    ] {
        assert_eq!(engine_e(expr), want, "TiKV: {expr}");
        assert_native_refusal(expr);
    }

    for invalid in [
        "regexp_like('a', '')",
        "regexp_like('a', '(')",
        "regexp_like('a', '(*')",
        "regexp_like('a', '[a')",
        "regexp_like('a', '\\\\')",
        "regexp_like('abc', 'abc', 'p')",
        "regexp_like('abc', 'abc', 'cpi')",
    ] {
        assert!(engine_declines(invalid), "{invalid}");
        assert_native_refusal(invalid);
    }
}

/// Positional regexp functions from `pkg/expression/builtin_regexp_test.go`.
/// Their independent expected values prove TiKV receives the source argument
/// casts while direct AST evaluation proves native fallback is absent.
#[test]
fn positional_regexp_source_vectors_reach_the_rewritten_evaluator() {
    for (expr, want) in [
        ("regexp_substr('abc abd abe', 'ab.', 1, 2)", "STR:abd"),
        ("regexp_substr('你好啊', '好', 2)", "STR:好"),
        ("regexp_instr('abc abd abe', 'ab.', 3, 2)", "INT:9"),
        ("regexp_instr('你好啊', '好', 2)", "INT:2"),
        ("regexp_replace('abc abd', 'ab.', 'X')", "STR:X X"),
        ("regexp_replace('abc abd', 'ab.', 'X', 1, 2)", "STR:abc X"),
    ] {
        assert_eq!(engine_e(expr), want, "TiKV: {expr}");
        assert_native_refusal(expr);
    }
}
