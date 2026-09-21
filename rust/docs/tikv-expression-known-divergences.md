# Known TiKV / Go expression divergences (deferred)

This is the "small inconsistencies" list: behaviors where the reused TiKV kernel
or its metadata differs from Go TiDB, found while broadening the local adapter.
They are treated as **bugs to fix later**, not as reasons to stop adapting.

They are not the same thing as the pre-existing failures of this working tree
(see the end of this file). Nothing here blocks the current branch: each entry
either has a boundary workaround or keeps the function native.

Legend for `Kind`:

* `semantics` — the kernel returns a different value/result than Go.
* `diagnostic` — same value, different error/warning behavior.
* `context` — the kernel needs session state TiKV does not have a seam for.
* `robustness` — the kernel panics or indexes unsafely on inputs the adapter can
  produce.
* `encoding` — the two sides disagree on the stored representation.
* `type` — the pinned engine has no evaluation type/codec for the SQL type.

| # | Function / signature | Kind | Symptom | Evidence | Current handling |
| --- | --- | --- | --- | --- | --- |
| 1 | `UuidVersion`, `UuidTimestamp` (`uuid_version`, `uuid_timestamp`) | semantics | Malformed UUID strings are accepted; Go raises error 1411 | `integration_diff` `expression/uuid` topics appeared only with the engine enabled | Excluded from local admission (`blocked_name`) |
| 2 | `Ord` (`Ord`) | semantics | `ORD(NULL)` yields 0; Go yields NULL (TiKV's own `test_ord` pins 0) | differential fixture `ord_bytes`/`ord_utf8` | Leaf-only `IF(StringIsNull(x), NULL, ORD(x))` wrapper |
| 3 | `GreatestInt`, `LeastInt` | semantics | Unsigned integers compared as raw `i64`; values above `i64::MAX` sort as negative | differential fixture `greatest_uint`/`least_uint` | Exact Decimal compare + `CastDecimalAsInt` |
| 4 | `JSON_SET`, `JSON_INSERT`, `JSON_REPLACE` | semantics | SQL NULL base maps to JSON `null` in TiKV; Go returns SQL NULL | source review of `impl_json.rs` | Base must be declared/known non-NULL |
| 5 | `JSON_MERGE` | diagnostic | Go emits a deprecation warning on a non-NULL result; TiKV does not | Go `builtin_json.go`; local lowering | `json_merge` stays native; only `json_merge_preserve` admitted |
| 6 | `JSON_QUOTE` | semantics | Control bytes emitted as `\a`/`\v`, which is invalid JSON | source review of `impl_json.rs` | Only immutable valid UTF-8 literals without controls |
| 7 | `WeekWithoutMode` | context | Hardcodes mode 0; TiDB uses session `default_week_format` | source review of `impl_time.rs` | Mode-less `week` stays native; explicit-mode admitted |
| 8 | `CONCAT`, `CONCAT_WS`, `INSERT`, `FROM_BASE64`, `MAKE_SET` | context | TiKV has no `max_allowed_packet` seam; TiDB warns/errors and truncates | `context.rs` `max_allowed_packet` contract | Retained native |
| 8a | `REPEAT`, `SPACE`, `LPAD`, `RPAD`, `TO_BASE64`, `WEIGHT_STRING` | removal | Native packet/weight kernels were physically deleted without a verified engine context/wire path | contraction receipts and boundary tests | Structured unsupported error; no fallback |
| 9 | `CAST(... AS JSON)` of temporal values | encoding | TiDB embeds bare `CoreTime`; TiKV embeds `Time` with type/FSP nibble | source review of both JSON codecs | Temporal JSON (root or nested) refused |
| 10 | `RoundWithFrac*`, `Truncate*` | robustness | Extreme fractional digits build `Inf`/`NaN` and panic in `NotNan` (`TRUNCATE(0.0,309)`, `ROUND(1.0,-400)`, `ROUND(f64::MAX,-308)*0`) | bounded `catch_unwind` witnesses in `safety_tests.rs` | Digit preflight + checked RPN entry |
| 11 | `VecL2Distance`, `VecL2Norm`, `VecNegativeInnerProduct`, `VecL1Distance` | robustness | Finite inputs can produce `Inf`, and a following arithmetic node panics | `VecL2Distance([3e38],[-3e38])*0` witness | Checked RPN entry validates produced REALs |
| 12 | `CastStringAsReal` and REAL result metadata | robustness | `flen`/`decimal` combinations underflow `truncate_f64` or trip an assertion | native panic controls | REAL metadata preflight (`-1` or `0..=254`, `flen >= decimal`) |
| 13 | `ToBinary`, `LikeSig` mapper arms | robustness | Child index accessed before the arity check, so a malformed tree panics | native panic controls | Arity preflight before the mapper |
| 14 | regexp `raw_varg` validators | robustness | Validators check count/return only, not argument types; `as_bytes`/`as_int` can panic | source review of generated validators | Facade type/shape preflight |
| 15 | ENUM literal decoding | robustness | `elems[value - 1]` indexed unchecked against schema elems | source review | Facade bounds check |
| 16 | Decimal zero representation | robustness | `digit_bounds` computes `word_count - 1` on a zero-word value (debug underflow, unsafe release indexing) | debug witness | Canonicalize to `int_cnt = 1`, preserving scale/sign; reject nonzero inactive words |
| 17 | Mutable `Json` / `VectorFloat32` backing | robustness | Values can be constructed/invalidated outside the checked constructors | source review | Ingress validation before execution |
| 18 | `SET` evaluation type | type | Pinned engine's `EvalType` has no usable `Set` codec (current TiKV master does list `SetRef`) | inventory `--self-check` | `SET`/geometry/array columns stay native |

## Pre-existing failures of this working tree (not adapter regressions)

These reproduce identically with the `tikv-expr` feature disabled, so they are
not caused by the adapter. They are recorded separately to avoid mixing them
into the list above.

| Target | Observation |
| --- | --- |
| `difftest-result-tests --test expr_diff` | 2 `EXPORT_SET` string-argument divergences |
| `difftest-result-tests --test table_diff` | 7 of 1,942 in-domain statements diverged |
| `difftest-result-tests --test join_shape` | Stale ratchet constant: observed `(269,223,97,93,4)` vs recorded `(246,168,90,86,5)` |
| `difftest-result-tests --test integration_diff` | 142 of ~10,252 statements; divergence set byte-identical across native/copying/borrowed |
