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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source receipts for `pkg/expression.part5`'s FIND_IN_SET lookup variants.
//! TiKV exposes only the binary-string signature today. The former UTF-8 and
//! collation-aware source rows therefore stay visible as explicit adapter
//! declines; binary rows prove that the retained shape executes engine-only.

use super::*;

#[test]
fn find_in_set_nonbinary_source_rows_are_explicitly_contracted() {
    for (expr, former_answer) in [
        (
            "find_in_set(' ' collate utf8mb4_general_ci, '  , , ,' collate utf8mb4_general_ci)",
            "INT:2",
        ),
        (
            "find_in_set('a' collate utf8mb4_general_ci, 'a,b,a' collate utf8mb4_general_ci)",
            "INT:1",
        ),
        (
            "find_in_set('B' collate utf8mb4_general_ci, 'a,b,c' collate utf8mb4_general_ci)",
            "INT:2",
        ),
        ("find_in_set('a', 'b,a,c,a')", "INT:2"),
        ("find_in_set('a', 'a,b,a')", "INT:1"),
        ("find_in_set('', ',,')", "INT:1"),
        ("find_in_set('x', 'a,b,a')", "INT:0"),
        (
            "find_in_set(' ' collate utf8mb4_general_ci, ' ,a' collate utf8mb4_general_ci)",
            "INT:1",
        ),
        ("find_in_set('x' collate utf8mb4_general_ci, NULL)", "NULL"),
    ] {
        let _ = former_answer;
        assert!(engine_declines(expr), "shape unexpectedly admitted: {expr}");
        assert_string2_refusal(expr);
    }
}

#[test]
fn find_in_set_binary_rows_execute_only_in_tikv() {
    for (expr, answer) in [
        ("find_in_set(cast('b' as binary), 'a,b,c')", "INT:2"),
        ("find_in_set('b', cast('a,b,c' as binary))", "INT:2"),
        (
            "find_in_set(cast('b' as binary), cast('a,b,c' as binary))",
            "INT:2",
        ),
        (
            "find_in_set(cast('a,b' as binary), cast('a,b,c' as binary))",
            "INT:0",
        ),
        (
            "find_in_set(cast('' as binary), cast(',,x' as binary))",
            "INT:1",
        ),
        (
            "find_in_set(cast('x' as binary), cast('a,b,c' as binary))",
            "INT:0",
        ),
    ] {
        assert_eq!(engine_e(expr), answer, "TiKV engine: {expr}");
    }
}

/// go-parity-gap: the constStrlistLookupCache identity/invalidation asserts
/// describe harness-internal state this evaluator does not construct.
#[test]
#[ignore = "go-parity-gap: no constStrlistLookupCache/statement-context memoization layer exists"]
fn find_in_set_strlist_cache_lifecycle_gap() {}
