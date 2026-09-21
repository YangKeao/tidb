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

use std::any::Any;
use std::collections::BTreeSet;

use tidb_ast::{Expr, Visitable, Visitor};
use tidb_expr::{expression::Expression, rewriter::rewrite_expr};

pub const MISC_REMOVED: &str =
    "native miscellaneous evaluation was removed; TiKV engine required or function unsupported";
pub const STRING2_REMOVED: &str =
    "native string2 evaluation was removed; TiKV engine required or function unsupported";
pub const INET_REMOVED: &str =
    "native INET conversion evaluation was removed; TiKV engine required";

#[derive(Default)]
struct FunctionCollector {
    names: BTreeSet<String>,
    binary_find_in_set: bool,
    nonbinary_find_in_set: bool,
}

impl Visitor for FunctionCollector {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(expr) = node.downcast_mut::<Expr>() {
            match expr {
                Expr::Func { name, .. } => {
                    let upper_name = name.to_ascii_uppercase();
                    let is_find_in_set = upper_name == "FIND_IN_SET";
                    if is_find_in_set {
                        if let Ok(Expression::ScalarFunction(function)) = rewrite_expr(expr) {
                            if function.derived_collation() == tidb_datatype::Collation::Binary {
                                self.binary_find_in_set = true;
                            } else {
                                self.nonbinary_find_in_set = true;
                            }
                        }
                    }
                    self.names.insert(upper_name);
                }
                Expr::Regexp { .. } => {
                    self.names.insert("REGEXP".to_owned());
                }
                Expr::WeightString { .. } => {
                    self.names.insert("WEIGHT_STRING".to_owned());
                }
                _ => {}
            }
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}

fn collect_functions(sql: &str) -> Option<FunctionCollector> {
    let mut stmt = tidb_parser::parse(sql).ok()?;
    let mut collector = FunctionCollector::default();
    stmt.accept(&mut collector);
    Some(collector)
}

pub fn parsed_function_names(sql: &str) -> Option<BTreeSet<String>> {
    Some(collect_functions(sql)?.names)
}

pub fn requires_string2_engine(sql: &str) -> bool {
    let Some(collector) = collect_functions(sql) else {
        return false;
    };
    collector.binary_find_in_set
        || [
            "SUBSTRING",
            "SUBSTR",
            "MID",
            "ASCII",
            "BIT_LENGTH",
            "UPPER",
            "UCASE",
            "LOWER",
            "LCASE",
            "LEFT",
            "RIGHT",
            "REVERSE",
            "REPLACE",
            "STRCMP",
            "LTRIM",
            "RTRIM",
        ]
        .iter()
        .any(|name| collector.names.contains(*name))
}

pub fn requires_inet_engine(sql: &str) -> bool {
    let Some(collector) = collect_functions(sql) else {
        return false;
    };
    ["INET_ATON", "INET_NTOA", "INET6_ATON", "INET6_NTOA"]
        .iter()
        .any(|name| collector.names.contains(*name))
}

pub fn expected_removed_marker(sql: &str) -> Option<&'static str> {
    let collector = collect_functions(sql)?;
    let has = |candidates: &[&str]| {
        candidates
            .iter()
            .any(|name| collector.names.contains(*name))
    };
    if has(&[
        "RAND", "ABS", "SIGN", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE", "SQRT", "POW",
        "POWER", "EXP", "LN", "LOG", "LOG2", "LOG10", "PI", "SIN", "COS", "TAN", "ASIN", "ACOS",
        "ATAN", "ATAN2", "COT", "RADIANS", "DEGREES", "CONV", "CRC32",
    ]) {
        return Some("native math evaluation was removed; TiKV engine required");
    }
    if has(&[
        "MD5",
        "SHA",
        "SHA1",
        "SHA2",
        "SM3",
        "RANDOM_BYTES",
        "PASSWORD",
        "VALIDATE_PASSWORD_STRENGTH",
        "ENCODE",
        "DECODE",
        "COMPRESS",
        "AES_ENCRYPT",
        "AES_DECRYPT",
        "UNCOMPRESS",
        "UNCOMPRESSED_LENGTH",
    ]) {
        return Some("native crypto evaluation was removed; TiKV engine required");
    }
    if has(&[
        "VEC_DIMS",
        "VEC_L1_DISTANCE",
        "VEC_L2_DISTANCE",
        "VEC_NEGATIVE_INNER_PRODUCT",
        "VEC_COSINE_DISTANCE",
        "VEC_L2_NORM",
        "VEC_FROM_TEXT",
        "VEC_AS_TEXT",
    ]) {
        return Some("native vector evaluation was removed; TiKV engine required");
    }
    if has(&["JSON_DEPTH", "JSON_STORAGE_FREE", "JSON_STORAGE_SIZE"]) {
        return Some("native JSON depth/storage evaluation was removed; TiKV engine required");
    }
    if has(&["INET_ATON", "INET_NTOA", "INET6_ATON", "INET6_NTOA"]) {
        return Some(INET_REMOVED);
    }
    if has(&[
        "REGEXP",
        "RLIKE",
        "REGEXP_LIKE",
        "REGEXP_SUBSTR",
        "REGEXP_INSTR",
        "REGEXP_REPLACE",
    ]) {
        return Some("native regexp evaluation was removed; TiKV engine required");
    }
    if has(&[
        "REPEAT",
        "SPACE",
        "LPAD",
        "RPAD",
        "TO_BASE64",
        "WEIGHT_STRING",
        "CONCAT",
        "CONCAT_WS",
        "INSERT_FUNC",
        "MAKE_SET",
        "FROM_BASE64",
    ]) {
        return Some("native packet-limited string evaluation was removed; function unsupported");
    }
    if has(&[
        "SUBSTRING",
        "SUBSTR",
        "MID",
        "ASCII",
        "BIT_LENGTH",
        "UPPER",
        "UCASE",
        "LOWER",
        "LCASE",
        "LEFT",
        "RIGHT",
        "REVERSE",
        "REPLACE",
        "STRCMP",
        "FORMAT",
        "EXPORT_SET",
        "LTRIM",
        "RTRIM",
        "TRANSLATE",
    ]) || collector.nonbinary_find_in_set
    {
        return Some(STRING2_REMOVED);
    }
    if has(&[
        "UUID",
        "UUID_V4",
        "UUID_V7",
        "NAME_CONST",
        "IS_UUID",
        "UUID_VERSION",
        "UUID_TIMESTAMP",
        "UUID_TO_BIN",
        "BIN_TO_UUID",
        "TIDB_SHARD",
        "TIDB_DECODE_KEY",
        "VITESS_HASH",
    ]) {
        return Some(MISC_REMOVED);
    }
    None
}

#[test]
fn markers_come_from_parsed_function_nodes_not_text() {
    assert_eq!(
        expected_removed_marker("select abs(1), 'uuid()', /* sha2(x, 256) */ 2"),
        Some("native math evaluation was removed; TiKV engine required")
    );
    assert_eq!(
        expected_removed_marker("select 'uuid()', 1 /* abs(2) */"),
        None
    );
    assert_eq!(
        expected_removed_marker("select 1 regexp '1'"),
        Some("native regexp evaluation was removed; TiKV engine required")
    );
    assert!(requires_string2_engine(
        "select find_in_set(cast('b' as binary), 'a,b')"
    ));
    assert!(!requires_string2_engine("select find_in_set('b', 'a,b')"));
    let collated = "select find_in_set(cast('b' as binary), 'a,b' collate utf8mb4_general_ci)";
    assert!(!requires_string2_engine(collated));
    assert_eq!(expected_removed_marker(collated), Some(STRING2_REMOVED));
    for sql in [
        "select inet_aton('1.2.3.4')",
        "select inet_ntoa(0)",
        "select inet6_aton('::1')",
        "select inet6_ntoa(unhex('00000000'))",
    ] {
        assert!(requires_inet_engine(sql), "{sql}");
        assert_eq!(expected_removed_marker(sql), Some(INET_REMOVED), "{sql}");
    }
    assert!(!requires_inet_engine("select 'inet_aton(1.2.3.4)'"));
    for sql in [
        "select ascii('a')",
        "select bit_length('a')",
        "select upper('a')",
        "select ucase('a')",
        "select lower('A')",
        "select lcase('A')",
        "select left('abc', 1)",
        "select right('abc', 1)",
        "select reverse('abc')",
        "select replace('abc', 'a', 'x')",
        "select strcmp('a', 'b')",
    ] {
        assert!(requires_string2_engine(sql), "{sql}");
        assert_eq!(expected_removed_marker(sql), Some(STRING2_REMOVED), "{sql}");
    }
    assert!(!requires_string2_engine(
        "select 'upper(a)', 1 /* ascii(x) */"
    ));
    for sql in ["select instr('abc', 'b')", "select locate('b', 'abc')"] {
        assert!(!requires_string2_engine(sql), "{sql}");
        assert_eq!(expected_removed_marker(sql), None, "{sql}");
    }
    for sql in [
        "select concat('a', 'b')",
        "select concat_ws(',', 'a', 'b')",
        "select insert('abc', 2, 1, 'x')",
        "select make_set(1, 'a')",
        "select from_base64('YQ==')",
    ] {
        assert_eq!(
            expected_removed_marker(sql),
            Some("native packet-limited string evaluation was removed; function unsupported"),
            "{sql}"
        );
    }
    // Without a column resolver the derived collation is unproven; do not let
    // the generic contraction marker hide a binary-column routing regression.
    assert_eq!(
        expected_removed_marker("select find_in_set(c, 'a,b')"),
        None
    );
}
