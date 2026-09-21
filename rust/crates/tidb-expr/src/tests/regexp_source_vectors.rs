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

#![cfg(feature = "tikv-expr")]

//! Go-source regexp vectors evaluated exclusively by the embedded TiKV engine.
//!
//! These rows were migrated intact from `builtin_ext::regexp` when native
//! `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_REPLACE` evaluation was
//! removed. Values are supplied through one typed input row so invalid dynamic
//! patterns reach TiKV's runtime error boundary instead of constant-fold compile
//! refusal. Each engine call also proves that both native entry points reject
//! the function rather than silently providing a fallback.

use crate::column::Column;
use crate::expression::Expression;
use crate::rewriter::result_type::builtin_return_type;
use crate::scalar_function::ScalarFunction;
use crate::{Datum, EvalError, NoColumns};
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};

const NATIVE_REGEXP_REMOVED: EvalError =
    EvalError::Unsupported("native regexp evaluation was removed; TiKV engine required");

fn string(value: &str) -> Datum {
    Datum::new_string(value.to_owned())
}

fn arg_type(name: &str, index: usize, datum: &Datum) -> FieldType {
    let string_position = match name {
        "REGEXP_SUBSTR" => matches!(index, 0 | 1 | 4),
        "REGEXP_INSTR" => matches!(index, 0 | 1 | 5),
        "REGEXP_REPLACE" => matches!(index, 0 | 1 | 2 | 5),
        _ => panic!("unknown regexp source-vector name {name}"),
    };
    if string_position {
        assert!(matches!(datum, Datum::String(_) | Datum::Null));
        return FieldType::new(FieldTypeCode::VarString);
    }
    match datum {
        Datum::Int(_) | Datum::Null => FieldType::new(FieldTypeCode::LongLong),
        other => panic!("no regexp source-vector type mapping for {other:?}"),
    }
}

/// Builds the same typed scalar expression for all three checks. Native
/// evaluation must reject it at both public dispatch layers; only TiKV may
/// compile and execute the row. A compile refusal is a test failure rather
/// than permission to fall back.
fn call(name: &str, values: &[Datum]) -> Result<Datum, EvalError> {
    let lowered_name = name.to_ascii_lowercase();
    let field_types: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(index, datum)| arg_type(name, index, datum))
        .collect();
    let args: Vec<_> = field_types
        .iter()
        .enumerate()
        .map(|(index, field_type)| {
            let mut column = Column::new(index as i64, field_type.clone());
            column.index = index as i64;
            Expression::Column(column)
        })
        .collect();
    let return_type = builtin_return_type(&lowered_name, &args)
        .unwrap_or_else(|| panic!("{name}{values:?} has no inferred return type"));
    let scalar = ScalarFunction::new(CiString::new(&lowered_name), return_type, args);

    assert_eq!(
        scalar.eval(&NoColumns, tidb_chunk::row::Row::empty()),
        Err(NATIVE_REGEXP_REMOVED),
        "{name}{values:?} must not reach ScalarFunction's native fallback"
    );
    assert_eq!(
        crate::func::eval_func_values_in(name, values, &NoColumns),
        Some(Err(NATIVE_REGEXP_REMOVED)),
        "{name}{values:?} must not reach the values-native fallback"
    );

    let expression = Expression::ScalarFunction(scalar);
    let adapter = crate::tikv::TikvExpression::compile(
        &expression,
        crate::tikv::Context {
            flags: 482,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("TiKV compile errored for {name}{values:?}: {error:?}"))
    .unwrap_or_else(|| panic!("TiKV declined {name}{values:?}"));
    let mut input = tidb_chunk::chunk::Chunk::new_with_capacity(&field_types, 1);
    for (index, value) in values.iter().enumerate() {
        input.append_datum(index, value);
    }
    adapter
        .evaluate(&NoColumns, &input)?
        .pop()
        .ok_or(EvalError::Unsupported("TiKV regexp result is empty"))
}

fn assert_string(name: &str, values: &[Datum], expected: Option<&str>) {
    let actual = call(name, values).expect("source scalar row should evaluate");
    let expected = expected.map(string).unwrap_or(Datum::Null);
    assert_eq!(actual, expected, "{name}({values:?})");
}

fn assert_int(name: &str, values: &[Datum], expected: Option<i64>) {
    let actual = call(name, values).expect("source scalar row should evaluate");
    let expected = expected.map_or(Datum::Null, Datum::Int);
    assert_eq!(actual, expected, "{name}({values:?})");
}

fn assert_regexp_error(result: Result<Datum, EvalError>, expected_message: &str) {
    match result {
        Err(EvalError::ExternalEngine {
            code: 1139,
            message,
        }) => assert!(
            message.contains(expected_message),
            "regexp error {message:?} did not contain {expected_message:?}"
        ),
        other => {
            panic!("expected TiKV regexp error containing {expected_message:?}, got {other:?}")
        }
    }
}

#[test]
fn regexp_substr_matches_go_source_vectors() {
    // `pkg/expression/builtin_regexp_test.go:356 TestRegexpSubstr`.
    for (input, pattern, expected) in [
        ("abc", "bc", Some("bc")),
        ("你好", "好", Some("好")),
        ("a", "", None),
    ] {
        if pattern.is_empty() {
            assert!(call("REGEXP_SUBSTR", &[string(input), string(pattern)]).is_err());
        } else {
            assert_string("REGEXP_SUBSTR", &[string(input), string(pattern)], expected);
        }
    }
    assert_string("REGEXP_SUBSTR", &[string("abc"), Datum::Null], None);
    assert_string("REGEXP_SUBSTR", &[Datum::Null, string("bc")], None);
    assert_string("REGEXP_SUBSTR", &[Datum::Null, Datum::Null], None);

    for (input, pattern, pos, expected) in [
        ("abc", "bc", 2, Some("bc")),
        ("你好", "好", 2, Some("好")),
        ("abc", "bc", 3, None),
        ("你好啊", "好", 3, None),
        ("", "^$", 1, Some("")),
    ] {
        assert_string(
            "REGEXP_SUBSTR",
            &[string(input), string(pattern), Datum::Int(pos)],
            expected,
        );
    }
    for (input, pattern, pos) in [
        ("abc", "bc", -1),
        ("abc", "bc", 4),
        ("", "bc", 0),
        ("", "^$", 2),
    ] {
        assert!(call(
            "REGEXP_SUBSTR",
            &[string(input), string(pattern), Datum::Int(pos)]
        )
        .is_err());
    }
    assert_string(
        "REGEXP_SUBSTR",
        &[string(""), string("^$"), Datum::Null],
        None,
    );
    assert_string(
        "REGEXP_SUBSTR",
        &[Datum::Null, string("^$"), Datum::Null],
        None,
    );
    assert_string(
        "REGEXP_SUBSTR",
        &[string(""), Datum::Null, Datum::Null],
        None,
    );

    for (input, pattern, pos, occurrence, expected) in [
        ("abc abd abe", "ab.", 1, 1, Some("abc")),
        ("abc abd abe", "ab.", 1, 0, Some("abc")),
        ("abc abd abe", "ab.", 1, -1, Some("abc")),
        ("abc abd abe", "ab.", 1, 2, Some("abd")),
        ("abc abd abe", "ab.", 3, 1, Some("abd")),
        ("abc abd abe", "ab.", 3, 2, Some("abe")),
        ("abc abd abe", "ab.", 6, 1, Some("abe")),
        ("abc abd abe", "ab.", 6, 100, None),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, Some("嗯嗯")),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 2, Some("嗯好")),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 1, Some("嗯呐")),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 2, None),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 100, None),
    ] {
        assert_string(
            "REGEXP_SUBSTR",
            &[
                string(input),
                string(pattern),
                Datum::Int(pos),
                Datum::Int(occurrence),
            ],
            expected,
        );
    }
    assert_string(
        "REGEXP_SUBSTR",
        &[string(""), string("^$"), Datum::Int(1), Datum::Null],
        None,
    );
    assert_string(
        "REGEXP_SUBSTR",
        &[Datum::Null, string("^$"), Datum::Int(1), Datum::Null],
        None,
    );

    for (input, pattern, pos, occurrence, match_type, expected) in [
        ("abc", "ab.", 1, 1, "", Some("abc")),
        ("abc", "aB.", 1, 1, "i", Some("abc")),
        ("good\nday", "od", 1, 1, "m", Some("od")),
        ("\n", ".", 1, 1, "s", Some("\n")),
        ("abc", "ab.", 1, 1, "p", None),
    ] {
        let result = call(
            "REGEXP_SUBSTR",
            &[
                string(input),
                string(pattern),
                Datum::Int(pos),
                Datum::Int(occurrence),
                string(match_type),
            ],
        );
        if match_type == "p" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), expected.map_or(Datum::Null, string));
        }
    }
    assert_string(
        "REGEXP_SUBSTR",
        &[
            string("abc"),
            string("ab."),
            Datum::Int(1),
            Datum::Int(1),
            Datum::Null,
        ],
        None,
    );
}

#[test]
fn regexp_instr_matches_go_source_vectors() {
    // `pkg/expression/builtin_regexp_test.go:611 TestRegexpInStr`.
    for (input, pattern, expected) in [
        ("abc", "bc", Some(2)),
        ("你好", "好", Some(2)),
        ("", "^$", Some(1)),
    ] {
        assert_int("REGEXP_INSTR", &[string(input), string(pattern)], expected);
    }
    assert!(call("REGEXP_INSTR", &[string("a"), string("")]).is_err());
    assert_int("REGEXP_INSTR", &[Datum::Null, string("bc")], None);
    assert_int("REGEXP_INSTR", &[string("abc"), Datum::Null], None);

    for (input, pattern, pos, expected) in [
        ("abc", "bc", 2, Some(2)),
        ("你好", "好", 2, Some(2)),
        ("abc", "bc", 3, Some(0)),
        ("你好啊", "好", 3, Some(0)),
        ("", "^$", 1, Some(1)),
    ] {
        assert_int(
            "REGEXP_INSTR",
            &[string(input), string(pattern), Datum::Int(pos)],
            expected,
        );
    }
    for (input, pattern, pos) in [("abc", "bc", -1), ("abc", "bc", 4), ("", "bc", 0)] {
        assert!(call(
            "REGEXP_INSTR",
            &[string(input), string(pattern), Datum::Int(pos)]
        )
        .is_err());
    }

    for (input, pattern, pos, occurrence, expected) in [
        ("abc abd abe", "ab.", 1, 1, 1),
        ("abc abd abe", "ab.", 1, 0, 1),
        ("abc abd abe", "ab.", 1, -1, 1),
        ("abc abd abe", "ab.", 1, 2, 5),
        ("abc abd abe", "ab.", 3, 1, 5),
        ("abc abd abe", "ab.", 3, 2, 9),
        ("abc abd abe", "ab.", 6, 1, 9),
        ("abc abd abe", "ab.", 6, 100, 0),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, 1),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 2, 4),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 1, 7),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 2, 0),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 100, 0),
    ] {
        assert_int(
            "REGEXP_INSTR",
            &[
                string(input),
                string(pattern),
                Datum::Int(pos),
                Datum::Int(occurrence),
            ],
            Some(expected),
        );
    }
    for (input, pattern, pos, occurrence, return_option, expected) in [
        ("abc abd abe", "ab.", 1, 1, 0, 1),
        ("abc abd abe", "ab.", 1, 1, 1, 4),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, 0, 1),
        ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, 1, 3),
        ("", "^$", 1, 1, 0, 1),
        ("", "^$", 1, 1, 1, 1),
    ] {
        assert_int(
            "REGEXP_INSTR",
            &[
                string(input),
                string(pattern),
                Datum::Int(pos),
                Datum::Int(occurrence),
                Datum::Int(return_option),
            ],
            Some(expected),
        );
    }
    assert!(call(
        "REGEXP_INSTR",
        &[
            string("abc"),
            string("ab."),
            Datum::Int(1),
            Datum::Int(1),
            Datum::Int(2),
        ]
    )
    .is_err());

    for (input, pattern, ret, match_type, expected) in [
        ("abc", "ab.", 0, "", Some(1)),
        ("abc", "aB.", 0, "i", Some(1)),
        ("good\nday", "od$", 0, "m", Some(3)),
        ("good\nday", "oD$", 0, "mi", Some(3)),
        ("\n", ".", 0, "s", Some(1)),
        ("abc", "ab.", 0, "p", None),
    ] {
        let result = call(
            "REGEXP_INSTR",
            &[
                string(input),
                string(pattern),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(ret),
                string(match_type),
            ],
        );
        if match_type == "p" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), expected.map_or(Datum::Null, Datum::Int));
        }
    }
    assert_int(
        "REGEXP_INSTR",
        &[
            string("abc"),
            string("ab."),
            Datum::Null,
            Datum::Int(1),
            Datum::Int(0),
            Datum::Null,
        ],
        None,
    );
}

#[test]
fn regexp_replace_matches_go_source_vectors() {
    // `pkg/expression/builtin_regexp_test.go:923 TestRegexpReplace`.
    let url1 =
        "https://go.mail/folder-1/online/ru-en/#lingvo/#1О 50000&price_ashka/rav4/page=/check.xml";
    let url2 = "http://saint-peters-total=меньше 1000-rublyayusche/catalogue/kolasuryat-v-2-kadyirovka-personal/serial_id=0&input_state/apartments/mokrotochki.net/upravda.ru/yandex.ru/GameMain.aspx?mult]/on/orders/50195&text=мыс и орелка в Балаш смотреть онлайн бесплатно в хорошем камбалакс&lr=20030393833539353862643188&op_promo=C-Teaser_id=06d162.html";
    let url_pat = r"^https?://(?:www\.)?([^/]+)/.*$";
    for (input, pattern, replacement, expected) in [
        ("abc abd abe", "ab.", "cz", "cz cz cz"),
        ("你好 好的", "好", "逸", "你逸 逸的"),
        ("", "^$", "123", "123"),
        (
            "stackoverflow",
            "(.{5})(.*)",
            r"\\+\2+\1+\2+\1\",
            r"\+overflow+stack+overflow+stack",
        ),
        (
            "fooabcdefghij fooABCDEFGHIJ",
            "foo(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)",
            r"\\\9\\\8-\7\\\6-\5\\\4-\3\\\2-\1\\",
            r"\i\h-g\f-e\d-c\b-a\ \I\H-G\F-E\D-C\B-A\",
        ),
        ("fool food foo", "foo(.?)", r"\0+\1", "fool+l food+d foo+"),
        (url1, url_pat, r"a\12\13", "ago.mail2go.mail3"),
        (
            url2,
            url_pat,
            r"aaa\1233",
            "aaasaint-peters-total=меньше 1000-rublyayusche233",
        ),
        ("abc", r"\d*", "d", "dadbdcd"),
        ("abc", r"\d*$", "d", "abcd"),
        ("我们", r"\d*", "d", "d我d们d"),
    ] {
        assert_string(
            "REGEXP_REPLACE",
            &[string(input), string(pattern), string(replacement)],
            Some(expected),
        );
    }
    assert_string(
        "REGEXP_REPLACE",
        &[Datum::Null, string("bc"), string("x")],
        None,
    );
    assert_string(
        "REGEXP_REPLACE",
        &[string("abc"), Datum::Null, Datum::Null],
        None,
    );
    assert!(call("REGEXP_REPLACE", &[string("a"), string(""), string("a")]).is_err());

    for (input, pattern, replacement, pos, expected) in [
        ("abc", "ab.", "cc", 1, "cc"),
        ("abc", "bc", "cc", 3, "abc"),
        ("你好", "好", "的", 2, "你的"),
        ("你好啊", "好", "的", 3, "你好啊"),
        ("", "^$", "cc", 1, "cc"),
        ("seafood fool", "foo(.?)", "123", 3, "sea123 123"),
        ("seafood fool", "foo(.?)", "123", 5, "seafood 123"),
        ("seafood fool", "foo(.?)", "123", 10, "seafood fool"),
        ("seafood fool", "foo(.?)", r"z\12", 3, "seazd2 zl2"),
        ("seafood fool", "foo(.?)", r"z\12", 5, "seafood zl2"),
    ] {
        assert_string(
            "REGEXP_REPLACE",
            &[
                string(input),
                string(pattern),
                string(replacement),
                Datum::Int(pos),
            ],
            Some(expected),
        );
    }
    for (input, pattern, pos) in [
        ("", "^$", 2),
        ("", "^&", 0),
        ("abc", "bc", -1),
        ("abc", "bc", 4),
    ] {
        assert!(call(
            "REGEXP_REPLACE",
            &[string(input), string(pattern), string("a"), Datum::Int(pos)]
        )
        .is_err());
    }

    for (input, pattern, replacement, pos, occurrence, expected) in [
        ("abc abd", "ab.", "cc", 1, 1, "cc abd"),
        ("abc abd", "ab.", "cc", 1, 2, "abc cc"),
        ("abc abd", "ab.", "cc", 1, 0, "cc cc"),
        ("abc abd abe", "ab.", "cc", 3, 2, "abc abd cc"),
        ("abc abd abe", "ab.", "cc", 3, 10, "abc abd abe"),
        ("你好 好啊", "好", "的", 1, 1, "你的 好啊"),
        ("你好 好啊", "好", "的", 3, 1, "你好 的啊"),
        ("seafood fool", "foo(.?)", "123", 1, 1, "sea123 fool"),
        ("seafood fool", "foo(.?)", "123", 1, 2, "seafood 123"),
        ("seafood fool", "foo(.?)", "123", 1, 10, "seafood fool"),
        ("seafood fool", "foo(.?)", r"z\12", 1, 1, "seazd2 fool"),
        ("seafood fool", "foo(.?)", r"z\12", 1, 2, "seafood zl2"),
        ("", "^$", "cc", 1, 1, "cc"),
        ("", "^$", "cc", 1, 2, ""),
        ("", "^$", "cc", 1, -1, "cc"),
        ("abc", r"\d*", "p", 1, 2, "apbc"),
        ("abc", r"\d*$", "p", 1, 1, "abcp"),
        ("我们", r"\d*", "p", 1, 2, "我p们"),
    ] {
        assert_string(
            "REGEXP_REPLACE",
            &[
                string(input),
                string(pattern),
                string(replacement),
                Datum::Int(pos),
                Datum::Int(occurrence),
            ],
            Some(expected),
        );
    }

    for (input, pattern, replacement, occurrence, match_type, expected) in [
        ("abc", "ab.", "cc", 0, "", "cc"),
        ("abc", "aB.", "cc", 0, "i", "cc"),
        ("good\nday", "od$", "cc", 0, "m", "gocc\nday"),
        ("good\nday", "oD$", "cc", 0, "mi", "gocc\nday"),
        ("Good\nday", "a(B)", r"a\12", 0, "msi", "Good\nday"),
        ("Good\nday", ".", "cc", 3, "ci", "Goccd\nday"),
        ("seafood fool", "foo(.?)", "的", 2, "m", "seafood 的"),
        ("abc abd abe", "(.)", "cc", 4, "cii", "abcccabd abe"),
        ("\n", ".", "cc", 0, "s", "cc"),
        ("好的 好滴 好~", ".", "的", 0, "msi", "的的的的的的的的"),
    ] {
        assert_string(
            "REGEXP_REPLACE",
            &[
                string(input),
                string(pattern),
                string(replacement),
                Datum::Int(1),
                Datum::Int(occurrence),
                string(match_type),
            ],
            Some(expected),
        );
    }
    assert!(call(
        "REGEXP_REPLACE",
        &[
            string("abc"),
            string("ab."),
            string("cc"),
            Datum::Int(1),
            Datum::Int(0),
            string("p"),
        ]
    )
    .is_err());
    assert_string(
        "REGEXP_REPLACE",
        &[
            string("abc"),
            string("ab."),
            Datum::Null,
            Datum::Int(1),
            Datum::Int(0),
            Datum::Null,
        ],
        None,
    );
}

#[test]
fn regexp_source_helpers_keep_capture_replacement_contract() {
    assert_regexp_error(
        call("REGEXP_SUBSTR", &[string("a"), string("(")]),
        "Invalid regexp pattern",
    );
    assert_regexp_error(
        call(
            "REGEXP_INSTR",
            &[
                string("a"),
                string("a"),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(2),
            ],
        ),
        "Invalid regexp return option: 2",
    );
    assert_regexp_error(
        call(
            "REGEXP_REPLACE",
            &[string("abc"), string("(a)"), string(r"\2")],
        ),
        "Substitution number is out of range: 2",
    );
}
