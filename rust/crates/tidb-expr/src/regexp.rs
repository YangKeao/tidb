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

//! `[NOT] REGEXP`/`RLIKE` pattern matching — this workspace's first
//! external dependency, the `regex` crate (see the workspace
//! `Cargo.toml`'s own doc comment for why it's a high-fidelity match
//! for real TiDB's own Go-`regexp`-package-based implementation, not
//! an arbitrary choice). Shared by the operator and named-function evaluators.

use regex::Regex;
use tidb_query_expr::{
    NativeRegexpCompileError, NativeRegexpError, NativeRegexpInvocation, RegexpPolicyError,
};

use crate::tikv::{evaluate_regexp_in, EvaluatedArgs, EvaluatedBytesResult, RegexpFunction};
use crate::{Columns, EvalError, NoColumns};

/// The kernel and frontend retain the same context-owned success/error value.
pub(crate) use tidb_query_expr::NativeCachedRegexp as CachedRegexp;

fn compile_error(error: &NativeRegexpCompileError) -> EvalError {
    match error {
        NativeRegexpCompileError::EmptyPattern => {
            EvalError::Unsupported("empty regular expression pattern")
        }
        NativeRegexpCompileError::InvalidMatchType(_) => {
            EvalError::Unsupported("Invalid match type")
        }
        NativeRegexpCompileError::InvalidPattern(_) => {
            EvalError::Unsupported("invalid regular expression pattern")
        }
    }
}

/// Render only a typed cause from the matching invocation, never its Display text.
pub(crate) fn native_regexp_error(error: &NativeRegexpError) -> EvalError {
    match error {
        NativeRegexpError::Compile(error) => compile_error(error),
        NativeRegexpError::Policy(RegexpPolicyError::InvalidMatchType(flag)) => {
            compile_error(&NativeRegexpCompileError::InvalidMatchType(*flag))
        }
        NativeRegexpError::Policy(RegexpPolicyError::InvalidPosition { .. }) => {
            EvalError::Unsupported("Index out of bounds in regular expression search")
        }
        NativeRegexpError::Policy(RegexpPolicyError::InvalidSubstitution(_)) => {
            EvalError::Unsupported("Substitution number is out of range")
        }
        NativeRegexpError::Policy(RegexpPolicyError::InvalidReplacementUtf8(_)) => {
            EvalError::Unsupported("invalid UTF-8 regexp replacement")
        }
        NativeRegexpError::InvalidReturnOption(_) => EvalError::Unsupported(
            "Incorrect arguments to regexp_instr: return_option must be 1 or 0",
        ),
    }
}

/// Compatibility compiler entry for the original cache/test API. SQL execution
/// resolves this same shared compiler only at the kernel's actual demand point.
pub(crate) fn compile_regexp(pattern: &str, match_type: &str) -> Result<Regex, EvalError> {
    tidb_query_expr::compile_native_regexp(pattern, match_type)
        .map_err(|error| compile_error(&error))
}

/// Go `getRegexpMatchType`'s collation-derived initial flag. Explicit match
/// type flags are appended so the rightmost user flag keeps precedence.
pub(crate) fn regexp_match_type_with_collation(
    match_type: &str,
    collation: tidb_datatype::Collation,
) -> String {
    let initial = if tidb_datatype::is_ci_collation(collation.name()) {
        "i"
    } else {
        ""
    };
    format!("{initial}{match_type}")
}

/// Returns a compiled pattern, memoizing the result when Go's signature says
/// both pattern-bearing arguments are constant within the statement context.
/// The `Result` itself is cached so an invalid pattern is reported repeatedly
/// without recompiling, matching `regexpMemorizedSig.memorizedErr`.
pub(crate) fn get_cached_regexp(
    cache: &crate::builtin_ext::BuiltinFuncCache<CachedRegexp>,
    context_id: u64,
    cache_enabled: bool,
    pattern: &str,
    match_type: &str,
) -> Result<Regex, EvalError> {
    if !cache_enabled {
        return compile_regexp(pattern, match_type);
    }
    let cached = cache.get_or_init_cache(context_id, || {
        Ok::<_, EvalError>(CachedRegexp {
            result: tidb_query_expr::compile_native_regexp(pattern, match_type),
        })
    })?;
    cached
        .result
        .as_ref()
        .map(Clone::clone)
        .map_err(compile_error)
}

/// `REGEXP_LIKE(expr, pat[, match_type])` over the seed evaluator's UTF-8
/// scalar value domain. Callers provide only the source `match_type`; the
/// scalar-function layer supplies statement-context caching where the Go
/// signature permits it.
pub(crate) fn regexp_like(text: &str, pattern: &str, match_type: &str) -> Result<bool, EvalError> {
    regexp_like_in(&NoColumns, text, pattern, match_type)
}

pub(crate) fn regexp_like_in(
    ctx: &dyn Columns,
    text: &str,
    pattern: &str,
    match_type: &str,
) -> Result<bool, EvalError> {
    let value = evaluate_regexp_in(RegexpFunction::Like, ctx, || {
        Ok(EvaluatedArgs::RegexpLike {
            invocation: NativeRegexpInvocation::new(
                &crate::builtin_ext::BuiltinFuncCache::default(),
                &crate::builtin_ext::BuiltinFuncCache::default(),
                0,
                false,
                false,
            ),
            text: text.as_bytes().to_vec(),
            pattern: pattern.as_bytes().to_vec(),
            match_type: match_type.as_bytes().to_vec(),
        })
    })?;
    EvaluatedBytesResult::Int(value).into_nonnull_bool()
}

/// Whether `text` matches `pattern` anywhere within it — a genuine
/// substring/partial match, NOT full-string anchoring — case-SENSITIVE,
/// matching the seed evaluator's `utf8mb4_bin` convention. Empty and
/// malformed patterns are surfaced as `Unsupported`, matching TiDB's source
/// runtime errors rather than allowing the regex crate's empty-pattern default.
pub(crate) fn regexp_match(text: &str, pattern: &str) -> Result<bool, EvalError> {
    regexp_match_in(&NoColumns, text, pattern)
}

pub(crate) fn regexp_match_in(
    ctx: &dyn Columns,
    text: &str,
    pattern: &str,
) -> Result<bool, EvalError> {
    regexp_match_with_collation_in(ctx, text, pattern, crate::ops::DERIVATION_FREE_COLLATION)
}

/// [`regexp_match`] under the collation the expression derivation aggregated
/// over both operands (Go `deriveCollation`'s `ast.Regexp` arm).
///
/// Go does NOT compare through a collator here -- `getRegexpMatchType`
/// (`pkg/expression/builtin_regexp.go`) seeds the match-type flag set with
/// `flagI` when `collate.IsCICollation(collation)`, so a case-insensitive
/// collation is expressed to RE2 as the `i` flag. Captured from TiDB:
/// `'ABC' COLLATE utf8mb4_general_ci REGEXP 'abc'` is 1 where the
/// `utf8mb4_bin` form is 0.
///
/// A user-supplied `match_type` can still override this: an explicit `c` flag
/// deletes `i` again, which the shared compiler's left-to-right flag scan
/// reproduces because the seeded `i` is passed as the leading flag.
pub(crate) fn regexp_match_with_collation(
    text: &str,
    pattern: &str,
    collation: tidb_datatype::Collation,
) -> Result<bool, EvalError> {
    regexp_match_with_collation_in(&NoColumns, text, pattern, collation)
}

pub(crate) fn regexp_match_with_collation_in(
    ctx: &dyn Columns,
    text: &str,
    pattern: &str,
    collation: tidb_datatype::Collation,
) -> Result<bool, EvalError> {
    let match_type = regexp_match_type_with_collation("", collation);
    regexp_like_in(ctx, text, pattern, &match_type)
}

/// `[NOT] REGEXP` matching for the statistics TopN-assisted estimation path
/// (`GetSelectivityByFilter`): stored values and patterns arrive as raw
/// bytes, the estimator's own gate only reaches binary-collation string
/// columns (Go's new-collation refusal), so the case-sensitive default
/// matcher is exact, and any invalid UTF-8 operand or malformed pattern
/// answers `None` — the by-value form of Go's error fallback, which declines
/// the whole estimation. This is a pure shared helper, not a pooled SDK route:
/// it provides no worker-scope/budget guarantee and never masks a resource error.
pub fn regexp_match_bin_collation(text: &[u8], pattern: &[u8]) -> Option<bool> {
    tidb_query_expr::regexp_match_bin_collation_native(text, pattern)
}

/// `REGEXP_LIKE` applies the expression's derived collation before its
/// user-supplied match type. A later `c`/`i` therefore wins, as Go's
/// `getRegexpMatchType` requires.
pub(crate) fn regexp_like_with_collation(
    text: &str,
    pattern: &str,
    match_type: &str,
    collation: tidb_datatype::Collation,
) -> Result<bool, EvalError> {
    regexp_like_with_collation_in(&NoColumns, text, pattern, match_type, collation)
}

pub(crate) fn regexp_like_with_collation_in(
    ctx: &dyn Columns,
    text: &str,
    pattern: &str,
    match_type: &str,
    collation: tidb_datatype::Collation,
) -> Result<bool, EvalError> {
    let match_type = regexp_match_type_with_collation(match_type, collation);
    regexp_like_in(ctx, text, pattern, &match_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regexp_like_source_scalar_rows() {
        // These first nine rows are copied from Go's
        // `pkg/expression/builtin_like_test.go:64 TestRegexp`.  Keep the
        // table in source order: this is the `[NOT] REGEXP` (not the newer
        // three-argument `REGEXP_LIKE`) contract that the parser dispatcher
        // below exposes.
        let rows = [
            ("a", "^$", "", false),
            ("a", "a", "", true),
            ("b", "a", "", false),
            ("aA", "aA", "", true),
            ("a", ".", "", true),
            ("ab", "^.$", "", false),
            ("b", "..", "", false),
            ("aab", ".ab", "", true),
            ("abcd", ".*", "", true),
            ("abc", "AbC", "", false),
            ("abc", "AbC", "i", true),
            ("123\n321", "23$", "", false),
            ("123\n321", "23$", "m", true),
            ("good\nday", "^day", "m", true),
            ("\n", ".", "", false),
            ("\n", ".", "s", true),
            ("abc", "aBc", "ic", false),
            ("abc", "aBc", "ci", true),
        ];
        for (text, pattern, match_type, expected) in rows {
            assert_eq!(
                regexp_like(text, pattern, match_type).unwrap(),
                expected,
                "REGEXP_LIKE({text:?}, {pattern:?}, {match_type:?})"
            );
        }
    }

    /// The malformed-pattern rows from Go's `TestRegexp` must fail while
    /// compiling the pattern, rather than being treated as a non-match.  The
    /// production `regexp_match` path is deliberately exercised here instead
    /// of testing the `regex` dependency directly.
    #[test]
    fn regexp_source_malformed_patterns() {
        for pattern in ["(", "(*", "[a", "\\"] {
            assert!(
                matches!(
                    regexp_match("", pattern),
                    Err(EvalError::Unsupported("invalid regular expression pattern"))
                ),
                "pattern {pattern:?}"
            );
        }
    }

    #[test]
    fn regexp_like_rejects_empty_invalid_pattern_and_match_type() {
        assert!(matches!(
            regexp_like("a", "", ""),
            Err(EvalError::Unsupported("empty regular expression pattern"))
        ));
        assert!(matches!(
            regexp_like("a", "[a", ""),
            Err(EvalError::Unsupported("invalid regular expression pattern"))
        ));
        for pattern in ["(", "(*", "\\"] {
            assert!(matches!(
                regexp_like("", pattern, ""),
                Err(EvalError::Unsupported("invalid regular expression pattern"))
            ));
        }
        assert!(matches!(
            regexp_like("abc", "abc", "p"),
            Err(EvalError::Unsupported("Invalid match type"))
        ));
        assert!(matches!(
            regexp_like("abc", "abc", "cpi"),
            Err(EvalError::Unsupported("Invalid match type"))
        ));
    }
}
