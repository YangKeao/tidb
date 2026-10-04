# JSON_SEARCH — json-search-73

Round75 follows timestamp-add-72. JSON_SEARCH adds one functional family: **215/245**, strict0;30 eligible remain and6 more are needed for221. Previous214 family objects remain unchanged.

## Shared ownership

TiKV native_json_search.rs owns path selection, string-leaf walking, stable global deduplication, result shape and native_json_format output. It reuses the existing path DTO/parser, array range and ECMAScript identifier helpers. Only string leaves match; literal '*' keys remain distinct from wildcard legs. Array legs do not auto-wrap scalar values as EXTRACT does. Recursive selection preserves current-node-then-child order; one mode really stops at its first hit. Zero hits yield SQL NULL, one yields a formatted JSON string, multiple yield a formatted JSON array. Native returns Datum::String, not Datum::Json or a bare path.

The shared datatype pattern tokenizer/matcher gains explicit TrailingEscape::PrefixLiteral. This preserves an existing native compatibility behavior: a dangling escape matches its current character without requiring the target suffix to be empty. Unicode escape scalars, escape-before-wildcard precedence and case-sensitive rune matching remain intact. Ordinary LIKE Literal/Reject behavior does not change; the old local memo matcher is deleted rather than copied.

## Demand and transport

Original preparation order remains document parse, mode coercion/3150, pattern coercion, escape, then all demanded paths in order. Mode uses shared ASCII case-insensitive one/all classification without trimming. One mode does not skip parsing a bad later path. A NULL/error path stops demand of its suffix. Empty, omitted or explicit NULL escape means default backslash; it is not result NULL. Invalid escape length keeps the original Unsupported error.

The sole new JsonSearchSerdeNative profile uses existing Bytes3: actual serialized serde document, ordered parsed-path packet and a search specification of one/all bit, u32LE Unicode escape and original UTF8 pattern. No host match or ready result is carried. Both boundaries validate all-present main inputs and the existing path format; reusing the path decoder does not execute EXTRACT. Only a genuinely observed document/mode/pattern/path NULL uses the existing JsonOutputNullNative recipe. Main no-hit NULL is computed by TiKV, not fabricated as a NULL witness. Spec allocation failure is infrastructure failure, never business NULL. No new carrier, result kind, driver, binding, cause type or factory allowance.

AST/typed callers keep eager child evaluation before leaf coercion. Existing normal scalar SQL registration remains; no new parser alias, mask, PB/catalog/legacy or aggregate admission is introduced. The bounded read-only admission review found no additional native entry needing migration.

## Validation

[Eight serialized Cargo receipts](../logs/json-search-summary.txt):6 nonzero green,1 retained/resolved new SQL expectation failure and1 unchanged full-expression baseline failure. CPP pattern6/core-and-wrapper2/local325+1ignored; native JSON_SEARCH4/LIKE61+9ignored/SQLretry2 pass. All7 new tests eventually pass; filters overlap and ignored tests are not passing.

SQL14fixedcases in both modes yield28 value/NULL observations plus String/VarString(-1,-1)/connectionCI metadata and warning checks. Two standalone errors preserve3150 invalid mode and1105 execution-time invalid path;8 direct zero-slot probes cover values, no-hit and actual NULL. The first test incorrectly expected nominal3143 for a column-sourced bad path. Source inspection of driver/errors/exec.rs141–160 proves the existing EXEC mapper deliberately uses1105/evaluation origin; only that new assertion/comment changed. The parser still raises JsonError::InvalidPath, not Unsupported. Initial RED is retained; no production or old oracle was altered.

Full expression1568/4old/94ignored retains an identical failure section/list against timestamp-add-72 after only numeric panic-thread IDs become THREAD (SHA256411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95). Unistore is unchanged/not rerun. CPP98/native353 original test bodies are byte-identical. Fifteen sources (8CPP/7native),1 new module and7 new tests pass pinned formatting and both diff checks. No dependency/lock/generated/Go/Bazel/fixture changes. Fixed source-derived expectations only; no provider recording.

## Limits and publication

Seven exclusive writers implement the bounded slice; parent owns integration, formatting, gates, guides, manifests and pushes. Two unused native path helper reexports are removed after verifying no remaining consumers. TiKV publishes first, followed by TiDB pinning its exact commit and the byte-identical three-Plan hash. No force push or PR; the pre-existing local client-differential BUILD.bazel stays excluded.

This batch does not claim full temporal parser/type migration, RAND state-capability closure, password Unicode/policy migration, lexer/digest or plan codec/proto migration, or JSON_SUM_CRC32 ARRAY-cast admission. Strict completion, M6/default-NoColumns, whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, allocator/fault/physical heap/peak/OOM/zero-copy/performance and earlier compatibility gaps remain unverified. No whole-package transcreation or PR-readiness claim.
