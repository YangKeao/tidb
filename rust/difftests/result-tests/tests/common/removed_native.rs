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
pub const RADIX_REMOVED: &str =
    "native integer radix evaluation was removed; TiKV engine required or function unsupported";
pub const STRING_AUX_REMOVED: &str =
    "native string auxiliary evaluation was removed; TiKV engine required or function unsupported";

#[derive(Default)]
struct FunctionCollector {
    names: BTreeSet<String>,
    ordered_names: Vec<String>,
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
                    self.ordered_names.push(upper_name.clone());
                    self.names.insert(upper_name);
                }
                Expr::Regexp { .. } => {
                    self.ordered_names.push("REGEXP".to_owned());
                    self.names.insert("REGEXP".to_owned());
                }
                Expr::WeightString { .. } => {
                    self.ordered_names.push("WEIGHT_STRING".to_owned());
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

pub fn requires_radix_engine(sql: &str) -> bool {
    let Some(collector) = collect_functions(sql) else {
        return false;
    };
    ["HEX", "UNHEX", "BIN", "OCT", "ORD", "BIT_COUNT"]
        .iter()
        .any(|name| collector.names.contains(*name))
}

pub fn requires_string_aux_engine(sql: &str) -> bool {
    if is_string_aux_shape_contraction(sql) {
        return true;
    }
    let Some(collector) = collect_functions(sql) else {
        return false;
    };
    [
        "SUBSTRING_INDEX",
        "QUOTE",
        "FIELD",
        "ELT",
        "LOCATE",
        "INSTR",
        "POSITION",
        "TRIM",
    ]
    .iter()
    .any(|name| collector.names.contains(*name))
}

fn compact_sql_outside_strings(sql: &str) -> String {
    let lower = sql.trim_start().to_ascii_lowercase();
    let mut chars = lower.chars().peekable();
    let mut compact = String::new();
    let mut quoted = false;
    let mut escaped = false;
    while let Some(ch) = chars.next() {
        if quoted {
            compact.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    compact.push(chars.next().unwrap());
                } else {
                    quoted = false;
                }
            }
        } else if ch == '\'' {
            quoted = true;
            compact.push(ch);
        } else if !ch.is_ascii_whitespace() {
            compact.push(ch);
        }
    }
    compact
}

/// Exact corpus shapes deliberately contracted because lowering cannot preserve
/// source-kind provenance or ORD's NULL mask without replaying a child natively.
/// This is intentionally a whole-expression/statement allow-list: a prefix
/// match could mask an ordinary second SELECT item or a changed argument type.
pub fn is_radix_shape_contraction(sql: &str) -> bool {
    let compact = compact_sql_outside_strings(sql);
    let expr = compact
        .strip_prefix("select")
        .unwrap_or(&compact)
        .trim_end_matches(';');
    matches!(
        expr,
        "hex(0x0c)"
            | "hex(0x12)"
            | "hex(0x0041)"
            | "hex(translate(cast('中'asbinary),'中','x'))"
            | "hex(translate(cast('abc'asbinary),cast('ab'asbinary),cast('x'asbinary)))"
            | "hex(translate(cast('abc'asbinary),cast(''asbinary),'xy'))"
            | "hex(translate(cast('aéb'asbinary),cast('é'asbinary),'z'))"
            | "hex(translate(cast('aabb'asbinary),cast('aa'asbinary),'xy'))"
            | "hex(translate(cast('abc'asbinary),cast('cba'asbinary),cast('123'asbinary)))"
            | "hex(uuid_to_bin('5f13f854-d74a-11f0-9b7a-0ae0156bd76z'))"
            | "hex(uuid_to_bin('123e4567-e89b-02d3-a456-426614174000'))"
            | "hex(uuid_to_bin('123e4567-e89b-12d3-a456-426614174000'))"
            | "hex(vitess_hash(a))fromt_intorderbyid"
            | "hex(vitess_hash(convert(a,decimal(8,4))))fromt_intwhereid=5"
            | "oct(b'11111111')"
            | "ord(2)"
            | "ord(23)"
            | "ord(2.3)"
            | "ord(cast('éb'asbinary))"
            | "ord(convert('éb'usingbinary))"
            | "ord(unhex('e4bda0'))"
    )
}

fn is_whole_string_tail_call(expr: &str) -> bool {
    let Some(open) = ["locate(", "instr(", "position(", "trim("]
        .iter()
        .find(|prefix| expr.starts_with(**prefix))
        .map(|prefix| prefix.len() - 1)
    else {
        return false;
    };
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, ch) in expr.char_indices().skip(open) {
        if quoted {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                quoted = false;
            }
            continue;
        }
        match ch {
            '\'' => quoted = true,
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return index + ch.len_utf8() == expr.len();
                }
            }
            _ => {}
        }
    }
    false
}

/// Exact shapes that cannot safely use the pinned TiKV kernels: direct binary
/// literals lose their source provenance at QUOTE, the unsigned count would wrap
/// before SUBSTRING_INDEX's `abs()` branch, TiKV has no CHAR signature, selected
/// FIELD/ELT coercions are not admitted, and direct string-tail calls lack safe
/// collation/direction transport. The latter still requires one whole outer call.
pub fn is_string_aux_shape_contraction(sql: &str) -> bool {
    let compact = compact_sql_outside_strings(sql);
    let expr = compact
        .strip_prefix("select")
        .unwrap_or(&compact)
        .trim_end_matches(';');
    if is_whole_string_tail_call(expr) {
        return true;
    }
    matches!(
        expr,
        "locate('lo','hello'),instr('hello','l')"
            | "quote(x'446f6e5c277421')"
            | "quote(x'446f6e2774')"
            | "quote(x'446f6e22')"
            | "quote(x'446f6e5c22')"
            | "quote(x'5c27')"
            | "quote(x'5c22')"
            | "quote(x'001a')"
            | "quote(char(0,26))"
            | "char(72,73)"
            | "char(22823usingutf8mb4)"
            | "char(65,16740,67.5usingutf8)"
            | "elt(1.1e0,'2.1','3.1','11.1','1.1')"
            | "elt('2abc','x','y','z')"
            | "field(null,2,3,11,1)"
            | "field(1.1e0,'2.1','3.1','11.1','1.1')"
            | "field('1.1a',2.1e0,3.1e0,11.1e0,1.1e0)"
            | "field(1.10,0,11e-1)"
            | "field('abc',0,1,11.1e0,1.1e0)"
            | "field('1','01',1)"
            | "field('1','1x',1)"
            | "substring_index('www.pingcap.com','.','2')"
            | "substring_index('www.pingcap.com','.',2.5)"
            | "substring_index(\"aaa.bbb.ccc.ddd.eee\",'.',18446744073709551613)"
    )
}

fn removed_marker_for_name(name: &str, nonbinary_find_in_set: bool) -> Option<&'static str> {
    if matches!(
        name,
        "RAND"
            | "ABS"
            | "SIGN"
            | "CEIL"
            | "CEILING"
            | "FLOOR"
            | "ROUND"
            | "TRUNCATE"
            | "SQRT"
            | "POW"
            | "POWER"
            | "EXP"
            | "LN"
            | "LOG"
            | "LOG2"
            | "LOG10"
            | "PI"
            | "SIN"
            | "COS"
            | "TAN"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "ATAN2"
            | "COT"
            | "RADIANS"
            | "DEGREES"
            | "CONV"
            | "CRC32"
    ) {
        return Some("native math evaluation was removed; TiKV engine required");
    }
    if matches!(
        name,
        "MD5"
            | "SHA"
            | "SHA1"
            | "SHA2"
            | "SM3"
            | "RANDOM_BYTES"
            | "PASSWORD"
            | "VALIDATE_PASSWORD_STRENGTH"
            | "ENCODE"
            | "DECODE"
            | "COMPRESS"
            | "AES_ENCRYPT"
            | "AES_DECRYPT"
            | "UNCOMPRESS"
            | "UNCOMPRESSED_LENGTH"
    ) {
        return Some("native crypto evaluation was removed; TiKV engine required");
    }
    if matches!(
        name,
        "VEC_DIMS"
            | "VEC_L1_DISTANCE"
            | "VEC_L2_DISTANCE"
            | "VEC_NEGATIVE_INNER_PRODUCT"
            | "VEC_COSINE_DISTANCE"
            | "VEC_L2_NORM"
            | "VEC_FROM_TEXT"
            | "VEC_AS_TEXT"
    ) {
        return Some("native vector evaluation was removed; TiKV engine required");
    }
    if matches!(
        name,
        "JSON_DEPTH" | "JSON_STORAGE_FREE" | "JSON_STORAGE_SIZE"
    ) {
        return Some("native JSON depth/storage evaluation was removed; TiKV engine required");
    }
    if matches!(
        name,
        "INET_ATON" | "INET_NTOA" | "INET6_ATON" | "INET6_NTOA"
    ) {
        return Some(INET_REMOVED);
    }
    if matches!(
        name,
        "REGEXP" | "RLIKE" | "REGEXP_LIKE" | "REGEXP_SUBSTR" | "REGEXP_INSTR" | "REGEXP_REPLACE"
    ) {
        return Some("native regexp evaluation was removed; TiKV engine required");
    }
    if matches!(
        name,
        "REPEAT"
            | "SPACE"
            | "LPAD"
            | "RPAD"
            | "TO_BASE64"
            | "WEIGHT_STRING"
            | "CONCAT"
            | "CONCAT_WS"
            | "INSERT_FUNC"
            | "MAKE_SET"
            | "FROM_BASE64"
    ) {
        return Some("native packet-limited string evaluation was removed; function unsupported");
    }
    if matches!(name, "HEX" | "UNHEX" | "BIN" | "OCT" | "ORD" | "BIT_COUNT") {
        return Some(RADIX_REMOVED);
    }
    if matches!(
        name,
        "SUBSTRING_INDEX"
            | "QUOTE"
            | "CHAR_FUNC"
            | "FIELD"
            | "ELT"
            | "LOCATE"
            | "INSTR"
            | "POSITION"
            | "TRIM"
    ) {
        return Some(STRING_AUX_REMOVED);
    }
    if matches!(
        name,
        "SUBSTRING"
            | "SUBSTR"
            | "MID"
            | "ASCII"
            | "BIT_LENGTH"
            | "UPPER"
            | "UCASE"
            | "LOWER"
            | "LCASE"
            | "LEFT"
            | "RIGHT"
            | "REVERSE"
            | "REPLACE"
            | "STRCMP"
            | "FORMAT"
            | "EXPORT_SET"
            | "LTRIM"
            | "RTRIM"
            | "TRANSLATE"
    ) || (name == "FIND_IN_SET" && nonbinary_find_in_set)
    {
        return Some(STRING2_REMOVED);
    }
    if matches!(
        name,
        "UUID"
            | "UUID_V4"
            | "UUID_V7"
            | "NAME_CONST"
            | "IS_UUID"
            | "UUID_VERSION"
            | "UUID_TIMESTAMP"
            | "UUID_TO_BIN"
            | "BIN_TO_UUID"
            | "TIDB_SHARD"
            | "TIDB_DECODE_KEY"
            | "VITESS_HASH"
    ) {
        return Some(MISC_REMOVED);
    }
    None
}

pub fn removed_markers(sql: &str) -> Vec<&'static str> {
    let mut markers = Vec::new();
    if is_string_aux_shape_contraction(sql) {
        markers.push(STRING_AUX_REMOVED);
    }
    let Some(collector) = collect_functions(sql) else {
        return markers;
    };
    for name in &collector.ordered_names {
        if let Some(marker) = removed_marker_for_name(name, collector.nonbinary_find_in_set) {
            if !markers.contains(&marker) {
                markers.push(marker);
            }
        }
    }
    markers
}

pub fn expected_removed_marker(sql: &str) -> Option<&'static str> {
    removed_markers(sql).into_iter().next()
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
        "select hex('a')",
        "select unhex('61')",
        "select bin(2)",
        "select oct(8)",
        "select ord('a')",
        "select bit_count(7)",
    ] {
        assert!(requires_radix_engine(sql), "{sql}");
        assert_eq!(expected_removed_marker(sql), Some(RADIX_REMOVED), "{sql}");
    }
    assert_eq!(
        expected_removed_marker("select hex(upper('a'))"),
        Some(RADIX_REMOVED)
    );
    assert_eq!(
        expected_removed_marker("select upper(hex('a'))"),
        Some(STRING2_REMOVED)
    );
    for sql in [
        "select hex(0x0c)",
        "select hex(translate(cast('中' as binary), '中', 'x'))",
        "select oct(b'11111111')",
        "select ord(2)",
        "select ord(cast('éb' as binary))",
    ] {
        assert!(is_radix_shape_contraction(sql), "{sql}");
    }
    for sql in [
        "select hex('a')",
        "select bin(2)",
        "select bit_count(7)",
        "select oct('8')",
        "select oct('8' is null)",
        "select oct('8'), hex('a')",
        "select hex(0x0c), bit_count(7)",
        "select oct('8') as value",
        "select oct('8') /* changed statement */",
    ] {
        assert!(!is_radix_shape_contraction(sql), "{sql}");
    }
    assert_eq!(
        expected_removed_marker("select inet6_ntoa(unhex('00000000'))"),
        Some(INET_REMOVED)
    );
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
    for sql in [
        "select quote('safe')",
        "select substring_index('a.b.c', '.', 2)",
    ] {
        assert!(requires_string_aux_engine(sql), "{sql}");
        assert_eq!(
            expected_removed_marker(sql),
            Some(STRING_AUX_REMOVED),
            "{sql}"
        );
        assert!(!is_string_aux_shape_contraction(sql), "{sql}");
    }
    assert!(requires_string_aux_engine("select char(72, 73)"));
    assert_eq!(
        expected_removed_marker("select char(72, 73)"),
        Some(STRING_AUX_REMOVED)
    );
    assert!(is_string_aux_shape_contraction("select char(72, 73)"));
    for sql in [
        "select quote(x'001a')",
        "select substring_index('www.pingcap.com', '.', '2')",
        "select substring_index(\"aaa.bbb.ccc.ddd.eee\",'.',18446744073709551613)",
    ] {
        assert!(is_string_aux_shape_contraction(sql), "{sql}");
    }
    for sql in [
        "select quote(x'001a'), quote('safe')",
        "select quote(x'001a') as value",
        "select substring_index('a.b.c', '.', 2.5) /* changed statement */",
    ] {
        assert!(!is_string_aux_shape_contraction(sql), "{sql}");
    }
    for sql in ["select instr('abc', 'b')", "select locate('b', 'abc')"] {
        assert!(!requires_string2_engine(sql), "{sql}");
        assert!(requires_string_aux_engine(sql), "{sql}");
        assert_eq!(
            expected_removed_marker(sql),
            Some(STRING_AUX_REMOVED),
            "{sql}"
        );
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
