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

//! Positional regular-expression builtins, ported from
//! `pkg/expression/builtin_regexp.go`.
//!
//! This module retains scalar preparation and packing for
//! `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_REPLACE`; shared TiKV leaves
//! own their matching and replacement algorithms, without a C4 handoff here.
//! TiDB's Go
//! signatures additionally select collations, issue statement warnings, and
//! expose vectorized/DAG paths. This evaluator accepts evaluated UTF-8 scalar
//! values; its scalar-function caller supplies the source's context-keyed
//! compiled-pattern and replacement-instruction caches.

use regex::Regex;
pub(crate) use tidb_query_expr::NativeReplacementPart as ReplacementPart;
use tidb_query_expr::{
    regexp_instr_match, regexp_replace_matches, regexp_replacement_parts, regexp_substr_match,
    regexp_trim_at, RegexpPolicyError, RegexpReplacementEncoding,
};

use super::BuiltinFuncCache;
use crate::coerce::coerce_str;
use crate::regexp::{compile_regexp, CachedRegexp};
use crate::{Datum, EvalError};

const INVALID_INDEX: &str = "Index out of bounds in regular expression search";
const INVALID_RETURN_OPTION: &str =
    "Incorrect arguments to regexp_instr: return_option must be 1 or 0";
const INVALID_SUBSTITUTION: &str = "Substitution number is out of range";

/// Dispatches this family's scalar builtins; `None` means another family may
/// own the name or the caller should return the normal unsupported-function
/// error.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("REGEXP_SUBSTR", 2..=5) => Some(regexp_substr(vals)),
        ("REGEXP_INSTR", 2..=6) => Some(regexp_instr(vals)),
        ("REGEXP_REPLACE", 3..=6) => Some(regexp_replace(vals)),
        _ => None,
    }
}

/// Values-only regexp dispatch with Go's statement-context memoization. The
/// caller supplies the build-time constness decisions because this tier has
/// already evaluated the argument expressions.
pub(crate) fn dispatch_with_cache(
    name: &str,
    vals: &[Datum],
    context_id: u64,
    cache_pattern: bool,
    cache_replacement: bool,
    regexp_cache: &BuiltinFuncCache<CachedRegexp>,
    replacement_cache: &BuiltinFuncCache<Vec<ReplacementPart>>,
) -> Option<Result<Datum, EvalError>> {
    let compile = |pattern: &str, match_type: &str| {
        if !cache_pattern {
            return compile_regexp(pattern, match_type);
        }
        let cached = regexp_cache.get_or_init_cache(context_id, || {
            Ok::<_, EvalError>(CachedRegexp {
                result: compile_regexp(pattern, match_type),
            })
        });
        match cached {
            Ok(cached) => cached.result.clone(),
            Err(error) => Err(error),
        }
    };
    let resolve_replacement = |value: &str| {
        if !cache_replacement {
            return Ok(replacement_parts(value));
        }
        let cached = replacement_cache
            .get_or_init_cache(context_id, || Ok::<_, EvalError>(replacement_parts(value)));
        match cached {
            Ok(cached) => Ok(cached.as_ref().clone()),
            Err(error) => Err(error),
        }
    };

    match (name, vals.len()) {
        ("REGEXP_SUBSTR", 2..=5) => Some(regexp_substr_with_compiler(vals, compile)),
        ("REGEXP_INSTR", 2..=6) => Some(regexp_instr_with_compiler(vals, compile)),
        ("REGEXP_REPLACE", 3..=6) => Some(regexp_replace_with_compiler(
            vals,
            compile,
            resolve_replacement,
        )),
        _ => None,
    }
}

fn integer(value: &Datum) -> Result<Option<i64>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value)),
        Datum::UInt(value) => i64::try_from(*value)
            .map(Some)
            .map_err(|_| EvalError::IntOverflow),
        _ => Err(EvalError::Unsupported(
            "regular-expression integer argument coercion",
        )),
    }
}

/// Preserve native static diagnostics from actual typed leaf failures. Wire
/// callers retain their own dynamic diagnostic payloads for the same causes.
fn regexp_policy_error(error: RegexpPolicyError) -> EvalError {
    EvalError::Unsupported(match error {
        RegexpPolicyError::InvalidMatchType(_) => "Invalid match type",
        RegexpPolicyError::InvalidPosition { .. } => INVALID_INDEX,
        RegexpPolicyError::InvalidSubstitution(_) => INVALID_SUBSTITUTION,
        RegexpPolicyError::InvalidReplacementUtf8(_) => "invalid UTF-8 regexp replacement",
    })
}

fn optional_string(value: &Datum) -> Result<Option<String>, EvalError> {
    coerce_str(value)
}

fn required_string(value: &Datum) -> Result<Option<String>, EvalError> {
    optional_string(value)
}

fn regexp_substr(vals: &[Datum]) -> Result<Datum, EvalError> {
    regexp_substr_with_compiler(vals, compile_regexp)
}

fn regexp_substr_with_compiler(
    vals: &[Datum],
    compile: impl FnOnce(&str, &str) -> Result<Regex, EvalError>,
) -> Result<Datum, EvalError> {
    let (Some(text), Some(pattern)) = (required_string(&vals[0])?, required_string(&vals[1])?)
    else {
        return Ok(Datum::Null);
    };
    let pos = if vals.len() >= 3 {
        let Some(pos) = integer(&vals[2])? else {
            return Ok(Datum::Null);
        };
        pos
    } else {
        1
    };
    let occurrence = if vals.len() >= 4 {
        let Some(occurrence) = integer(&vals[3])? else {
            return Ok(Datum::Null);
        };
        occurrence
    } else {
        1
    };
    let match_type = if vals.len() == 5 {
        let Some(match_type) = optional_string(&vals[4])? else {
            return Ok(Datum::Null);
        };
        match_type
    } else {
        String::new()
    };

    let (_, trimmed) = regexp_trim_at(&text, pos).map_err(regexp_policy_error)?;
    let regexp = compile(&pattern, &match_type)?;
    let matched = regexp_substr_match(&regexp, trimmed, occurrence).map(str::to_owned);
    Ok(matched.map_or(Datum::Null, Datum::new_string))
}

fn regexp_instr(vals: &[Datum]) -> Result<Datum, EvalError> {
    regexp_instr_with_compiler(vals, compile_regexp)
}

fn regexp_instr_with_compiler(
    vals: &[Datum],
    compile: impl FnOnce(&str, &str) -> Result<Regex, EvalError>,
) -> Result<Datum, EvalError> {
    let (Some(text), Some(pattern)) = (required_string(&vals[0])?, required_string(&vals[1])?)
    else {
        return Ok(Datum::Null);
    };
    let pos = if vals.len() >= 3 {
        let Some(pos) = integer(&vals[2])? else {
            return Ok(Datum::Null);
        };
        pos
    } else {
        1
    };
    let occurrence = if vals.len() >= 4 {
        let Some(occurrence) = integer(&vals[3])? else {
            return Ok(Datum::Null);
        };
        occurrence
    } else {
        1
    };
    let return_option = if vals.len() >= 5 {
        let Some(return_option) = integer(&vals[4])? else {
            return Ok(Datum::Null);
        };
        if return_option != 0 && return_option != 1 {
            return Err(EvalError::Unsupported(INVALID_RETURN_OPTION));
        }
        return_option
    } else {
        0
    };
    let match_type = if vals.len() == 6 {
        let Some(match_type) = optional_string(&vals[5])? else {
            return Ok(Datum::Null);
        };
        match_type
    } else {
        String::new()
    };

    let (_, trimmed) = regexp_trim_at(&text, pos).map_err(regexp_policy_error)?;
    let regexp = compile(&pattern, &match_type)?;
    Ok(Datum::Int(regexp_instr_match(
        &regexp,
        trimmed,
        pos,
        occurrence,
        return_option,
    )))
}

/// Keep the replacement-cache API while the shared leaf owns tokenization.
pub(crate) fn replacement_parts(replacement: &str) -> Vec<ReplacementPart> {
    regexp_replacement_parts(replacement.as_bytes())
}

fn regexp_replace(vals: &[Datum]) -> Result<Datum, EvalError> {
    regexp_replace_with_compiler(vals, compile_regexp, |replacement| {
        Ok(replacement_parts(replacement))
    })
}

fn regexp_replace_with_compiler(
    vals: &[Datum],
    compile: impl FnOnce(&str, &str) -> Result<Regex, EvalError>,
    resolve_replacement: impl FnOnce(&str) -> Result<Vec<ReplacementPart>, EvalError>,
) -> Result<Datum, EvalError> {
    let (Some(text), Some(pattern), Some(replacement)) = (
        required_string(&vals[0])?,
        required_string(&vals[1])?,
        required_string(&vals[2])?,
    ) else {
        return Ok(Datum::Null);
    };
    let pos = if vals.len() >= 4 {
        let Some(pos) = integer(&vals[3])? else {
            return Ok(Datum::Null);
        };
        pos
    } else {
        1
    };
    let occurrence = if vals.len() >= 5 {
        let Some(occurrence) = integer(&vals[4])? else {
            return Ok(Datum::Null);
        };
        occurrence
    } else {
        0
    };
    let match_type = if vals.len() == 6 {
        let Some(match_type) = optional_string(&vals[5])? else {
            return Ok(Datum::Null);
        };
        match_type
    } else {
        String::new()
    };

    let (byte, trimmed) = regexp_trim_at(&text, pos).map_err(regexp_policy_error)?;
    let regexp = compile(&pattern, &match_type)?;
    let parts = resolve_replacement(&replacement)?;
    regexp_replace_matches(
        &text[..byte],
        trimmed,
        &regexp,
        &parts,
        occurrence,
        RegexpReplacementEncoding::NativeUtf8,
    )
    .map(Datum::new_string)
    .map_err(regexp_policy_error)
}

#[cfg(test)]
mod tests {
    use super::{dispatch, regexp_instr, regexp_replace, regexp_substr};
    use crate::{Datum, EvalError};

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_owned())
    }

    fn call(name: &str, values: &[Datum]) -> Result<Datum, EvalError> {
        dispatch(name, values).expect("regexp name/arity should dispatch")
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
        let url1 = "https://go.mail/folder-1/online/ru-en/#lingvo/#1О 50000&price_ashka/rav4/page=/check.xml";
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
        assert!(matches!(
            regexp_substr(&[string("a"), string("(")]),
            Err(EvalError::Unsupported("invalid regular expression pattern"))
        ));
        assert!(matches!(
            regexp_instr(&[
                string("a"),
                string("a"),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(2),
            ]),
            Err(EvalError::Unsupported(
                "Incorrect arguments to regexp_instr: return_option must be 1 or 0"
            ))
        ));
        assert!(matches!(
            regexp_replace(&[string("abc"), string("(a)"), string(r"\2"),]),
            Err(EvalError::Unsupported(
                "Substitution number is out of range"
            ))
        ));
    }
}
