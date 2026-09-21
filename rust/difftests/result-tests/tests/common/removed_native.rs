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

pub const MISC_REMOVED: &str =
    "native miscellaneous evaluation was removed; TiKV engine required or function unsupported";

#[derive(Default)]
struct FunctionCollector {
    names: BTreeSet<String>,
}

impl Visitor for FunctionCollector {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(expr) = node.downcast_mut::<Expr>() {
            match expr {
                Expr::Func { name, .. } => {
                    self.names.insert(name.to_ascii_uppercase());
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

pub fn parsed_function_names(sql: &str) -> Option<BTreeSet<String>> {
    let mut stmt = tidb_parser::parse(sql).ok()?;
    let mut collector = FunctionCollector::default();
    stmt.accept(&mut collector);
    Some(collector.names)
}

pub fn expected_removed_marker(sql: &str) -> Option<&'static str> {
    let names = parsed_function_names(sql)?;
    let has = |candidates: &[&str]| candidates.iter().any(|name| names.contains(*name));
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
    ]) {
        return Some("native packet-limited string evaluation was removed; function unsupported");
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
}
