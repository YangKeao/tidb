# TiKV expression corpus re-pointing plan

Work list for re-pointing the 33 Go-test source ports in
`rust/crates/tidb-expr/src/tests/*_source.rs` at the local TiKV engine, as
required by `tikv-expression-removal-checklist.md` section 3. Read-only
analysis: no file was edited except this document; no cargo/go command was run.

Branch `feat/tikv-expression-coverage`, HEAD `b755409`. Admission authority:
`rust/crates/tidb-expr/src/tikv/admission.rs` (`ADMISSION_ROWS`), differential
pattern to imitate: `rust/crates/tidb-expr/tests/tikv_coverage.rs`.

## 0. Correction to the stated admission counts

The task and two prose docs state **232 admitted / 152 excluded**. That is the
count at commit `4e1c7c7` (the commit that introduced the table). At HEAD
`b755409` `ADMISSION_ROWS` has **384 rows = 230 admitted + 154 excluded**.
`from_days` and `json_array_append` were moved from `Admitted` to `Excluded` in
`6c85f8c` ("keep three engine divergences out of the adapter's answers"), and
`tikv-expression-coverage.json` already records `admission_admitted: 230`,
`admission_excluded: 154`. `tikv-expression-coverage.md` section 1.1 and
`tikv-expression-removal-checklist.md` section 3 repeat the stale 232/152.
All numbers below use the HEAD table.

## 0.1 Headline numbers

| Item | Count |
| --- | --- |
| `*_source.rs` files | 33 |
| `#[test]` functions | 413 |
| Admission rows | 384 (230 admitted, 154 excluded) |
| Admitted names with >=1 source-port test | 141 |
| Admitted names with no source-port test but covered by `tests/tikv_coverage.rs` | 89 |
| Admitted names covered only elsewhere in the crate | 0 |
| Admitted names with no coverage anywhere in `tidb-expr` | 0 |
| Excluded names exercised by at least one source-port test | 103 |
| Source-port tests whose subject set includes an excluded name | 140 |
| Files ranked (a) Expression/SQL-text level | 22 |
| Files ranked (b) native-helper level | 2 |
| Files ranked (c) no evaluator path (structure / gap stubs) | 9 |

Key structural finding: **no `*_source.rs` file calls `string_fn::` or `ops::`**
directly (verified: `grep -n 'string_fn\|ops::' *_source.rs` finds one comment
mention only). The native-helper call sites the task warns about
(`string_fn::length`, `ops::...`) live in the *non-source* test modules
(`tests/mod.rs`, `tests/math.rs`, `builtin_compare.rs::tests`, ...). Inside the
33 ports the only direct native-helper calls are `time_fn::dispatch` /
`time_fn::add_sub` / `calendar::date_diff`, `cast::eval_cast`,
`wrap_cast::*`, `crypto::dispatch`, `builtin_ext::{json,json2,string2,info,...}`
dispatch, `compare2::inet_aton_go_vectors`, `like::like_match_with_collation` and
`extract::filter_out_in_place`. That makes the corpus substantially more
re-pointable than the premise assumes: it is overwhelmingly SQL text and
`ScalarFunction` calls, not kernel calls.

## 0.2 How the mapping was determined

1. `ADMISSION_ROWS` in `tikv/admission.rs` is parsed for `name`, `decision`,
   `Family`, `required_eval_types`, `Shape` and `exclusion_reason`. The name
   universe (384) is the lookup table for subject names.
2. Each `#[test]` block is isolated by brace matching from its `#[test]`
   attribute, including multi-line `#[ignore]` attributes.
3. Because many tests delegate to file-local helper functions, each test body is
   textually expanded with the bodies of file-local `fn`s it calls (3 levels,
   e.g. `json_call`, `eval_info`, `rewrite`, `eval_as`). Without this step a test
   such as `builtin_info_json_math_source.rs::json_type` looks name-free.
4. Subject names are taken from string literals inside the expanded body: a
   literal equal (case-insensitively) to an admission name, or a call head
   `name(` inside a literal. This catches `e("abs(-5)")`, `e("BIT_COUNT(8)")`,
   `call("vec_l2_norm", ...)`, `json_call("JSON_TYPE", ...)` and
   `format!("{function}({argument})")` tables whose entries are name literals.
   Dynamic names built from prefixes (the fixture's `date_add_*`) are expanded
   separately for `tikv_coverage.rs`.
5. Native helpers are the `module::function` paths whose module is a native
   kernel/dispatch module. They are reported verbatim; no SQL name is guessed
   from a helper except the four explicit mappings `calendar::date_format` ->
   `date_format`, `add_sub::add_sub_time` -> `addtime`,
   `compare2::inet_aton_go_vectors` -> `inet_aton`,
   `like::like_match_with_collation` -> `like`.
6. Tier flags: `A` = `e(`/`v(`/`e_with(` AST-value tier; `C` = `chunk_e(`
   (parse -> `rewriter::rewrite_expr` -> `Expression::eval`); `E` = explicit
   `Expression`/`ScalarFunction` construction or `rewrite_expr(...).eval(...)`;
   `S` = `EvaluatorSuite`; `B` = native-helper call and no evaluator-tier call;
   `X` = neither (PB/codec/rewriter/context/gap work).
7. Fixture coverage is extracted from `tests/tikv_coverage.rs` via `call("name")`,
   `for name in [...]` arrays and the `date_add_`/`date_sub_` prefix loops.
   Rest-of-crate coverage is extracted with `CiString::new("name")`, `call("name")`,
   `"name" =>` match arms and `func("name"`.

Limitations: the literal heuristic can miss a name that is only ever built
dynamically, and `X` tests need individual reading. Alias handling:
`json_memberof`~`json_member_of`, `rlike`~`regexp`, `isfalse_with_null`~`isfalse`,
`istrue_with_null`~`istrue`, `position`~`locate`/`instr`, `casewhen`~`case`.

---

## 1. Per-file test inventory

Tier legend: `A` AST-value SQL text, `C` chunk/rewriter SQL text, `E` explicit
`Expression`, `S` `EvaluatorSuite`, `B` native helper only, `X` structural/gap.
`(ign)` marks an `#[ignore]`d test.

### `advisory_get_lock_integration_source.rs`

Rank **(a)**; 2 tests: a=2, b=0, c=0, gap=0.

Go source: GO PORT of `pkg/expression/integration_test/integration_test.go:1446` `TestGetLock` (batch part10) -- the per-call semantics of the advisory-lock builtins. The Go harness drives a real unistore-backed session whose `SessionVars` owns the lock map (`pkg/executor/simple.go`, `func (s *session)` lock helpers behind `getLock`/`releaseLock`). The Rust evaluator carries those calls as [`crate::context::Columns`] hooks, so this port evaluates the same SQL against one shared session stub that records acquisitions/releases -- identical behavior table, no real server. Assertions kept from Go (with their sources): - `get_lock` with ONE argument fails with `ErrWrongParamcountToNativeFct` (1582) (`integration_test.go:1458-1461`); - timeout `0` acquires immediately and `-10` clamps to the max with the warning `Truncated incorrect get_lock value: '-10'`

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_get_lock_call_semantics_table` | E | &mdash; | `get_lock`, `release_all_locks`, `release_lock`, `row` | &mdash; |
| `test_get_lock_rejects_bad_names_with_3057` | E | &mdash; | `get_lock`, `release_lock` | &mdash; |

### `aggregation_arithmetic_cast_source.rs`

Rank **(a)**; 51 tests: a=41, b=0, c=2, gap=8.

Go source: Batch b066 ports of `pkg/expression.part1` (`func Test*` items 1–60 on `origin/master`, sorted by file path then line). Each test re-derives its intent from the Go source it exercises. The slice spans `pkg/expression/aggregation/*_test.go`, top-level `bench_test.go`, `builtin_arithmetic*_test.go` and the first fourteen functions of `builtin_cast_test.go`. Aggregation DESCRIPTOR tests whose home already exists (`aggregation/tests.rs`) are listed in the receipt as verified pre-existing ports; this module adds what was missing.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_arithmetic_plus` | E | `plus` | &mdash; | &mdash; |
| `test_decimal_err_overflow` | E | `div`, `minus`, `mul`, `plus` | &mdash; | &mdash; |
| `test_arithmetic_overflow_error_message_with_column_name` | E | `mul`, `plus` | &mdash; | &mdash; |
| `test_real_arithmetic_overflow_error_message` | E | `mul` | &mdash; | &mdash; |
| `test_vectorized_builtin_arithmetic_func` | E | `div`, `intdiv`, `minus`, `mod` | &mdash; | &mdash; |
| `test_vectorized_decimal_err_overflow` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_agg_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_agg_func_sum_int_to_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_agg_func_max_min_count_to_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_distinct` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_distinct` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_check_agg_push_down_max_min_count` | E | &mdash; | &mdash; | &mdash; |
| `test_base_func_infer_max_min_count_ret_type` | E | &mdash; | &mdash; | &mdash; |
| `test_cast_functions_char_and_binary` | E | &mdash; | `cast_binary`, `cast_char` | &mdash; |
| `test_cast_functions_string_to_unsigned_and_signed` | E | &mdash; | `cast_signed`, `cast_unsigned` | &mdash; |
| `test_cast_functions_neg_int_as_unsigned_warns_8031` | E | &mdash; | `cast_unsigned` | &mdash; |
| `test_cast_functions_time_to_decimal_saturates` | E | `cast_decimal` | &mdash; | &mdash; |
| `test_cast_functions_uint_to_wide_decimal` | E | `cast_decimal` | &mdash; | &mdash; |
| `test_cast_functions_bad_string_as_decimal_reads_zero_silently` | E | `cast_decimal` | &mdash; | &mdash; |
| `test_cast_functions_bad_string_as_decimal_warns_1292` | E | `cast_decimal` | &mdash; | &mdash; |
| `test_cast_functions_int_as_char_zero_width` | E | &mdash; | `cast_char` | &mdash; |
| `test_cast_func_sig_as_decimal` | E | `cast_decimal` | &mdash; | &mdash; |
| `test_cast_func_sig_as_int` | E | &mdash; | `cast_signed` | &mdash; |
| `test_cast_signed_to_bit_returns_zero_padded_bytes` | E | &mdash; | `cast_signed` | &mdash; |
| `test_cast_func_sig_as_real` | E | `cast_double` | &mdash; | &mdash; |
| `cast_decimal_as_real_propagates_child_metadata` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_real` |
| `test_cast_func_sig_as_string` | E | &mdash; | `cast_char` | &mdash; |
| `test_cast_func_sig_as_string_truncates_at_flen` | E | &mdash; | `cast_char` | &mdash; |
| `test_cast_func_sig_as_time` | E | &mdash; | `cast_date`, `cast_datetime` | &mdash; |
| `test_cast_func_sig_as_duration` | E | &mdash; | `cast_time` | &mdash; |
| `test_cast_func_sig_null_and_hybrid` | E | &mdash; | `cast_char`, `cast_signed` | &mdash; |
| `test_cast_json_as_decimal_sig` | E | `cast_decimal` | &mdash; | &mdash; |
| `test_wrap_with_cast_as_types_classes_numeric_rows` | E | &mdash; | &mdash; | &mdash; |
| `test_wrap_with_cast_as_types_classes_real_to_decimal_keeps_fraction` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_decimal` |
| `test_wrap_with_cast_as_types_classes_enum_row` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_int` |
| `test_wrap_with_cast_as_string_binary_literal_warns_invalid_utf8` | E | &mdash; | &mdash; | &mdash; |
| `test_wrap_with_cast_as_types_classes_temporal_rows` | E | &mdash; | &mdash; | &mdash; |
| `test_wrap_with_cast_as_types_classes_unsigned_extras` | E | &mdash; | &mdash; | &mdash; |
| `test_wrap_with_cast_as_types_classes_uint_as_time` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_time` |
| `test_wrap_with_cast_as_time` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_time` |
| `test_wrap_with_cast_as_duration` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_duration` |
| `test_cast_duration_as_year_yields_the_current_year` | X | &mdash; | &mdash; | &mdash; |
| `test_cast_duration_as_year_honors_concat_mode` | X | &mdash; | &mdash; | &mdash; |
| `test_wrap_with_cast_as_string` | E | `to_binary` | &mdash; | `wrap_cast::wrap_with_cast_as_string` |
| `test_wrap_with_cast_as_json_passes_json_columns_through` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_json` |
| `test_cast_binary_string_as_json_sig` | E | &mdash; | `cast_json` | &mdash; |
| `test_cast_const_as_decimal_field_type` | E | &mdash; | &mdash; | `wrap_cast::wrap_with_cast_as_decimal` |
| `test_cast_as_char_field_type` | E | &mdash; | &mdash; | &mdash; |
| `test_cast_string_as_decimal_sig_with_unsigned_flag_in_union` | E | &mdash; | &mdash; | `wrap_cast::build_cast_to_in_union` |
| `test_cast_array_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_cast_int_as_int_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `builtin_info_json_math_source.rs`

Rank **(b)**; 60 tests: a=18, b=28, c=11, gap=3.

Go source: Batch b068 ports of `pkg/expression.part3` (`func Test*` items 121–180 on `origin/master`, sorted by file path then line). Each test re-derives its intent from the Go source it exercises.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `current_role` | E | &mdash; | `current_role` | &mdash; |
| `connection_id` | E | &mdash; | `connection_id` | &mdash; |
| `version` | E | &mdash; | `version` | &mdash; |
| `bench_mark` | C+E | `cast`, `json_array` | `benchmark` | `json2::dispatch` |
| `charset` | E | &mdash; | `charset` | &mdash; |
| `coercibility` | E | &mdash; | `coercibility` | &mdash; |
| `collation` | E | &mdash; | `collation` | &mdash; |
| `row_count` | E | &mdash; | `row_count` | &mdash; |
| `tidb_version` | E | &mdash; | `tidb_version` | &mdash; |
| `last_insert_id` | E | &mdash; | `last_insert_id` | &mdash; |
| `format_bytes` | B | &mdash; | `format_bytes` | `info::dispatch` |
| `format_nano_time` | B | &mdash; | `format_nano_time` | `info::dispatch` |
| `vectorized_builtin_info_func` | E | &mdash; | `benchmark`, `connection_id`, `current_role`, `last_insert_id`, `row_count`, `tidb_version`, `version` | &mdash; |
| `benchmark_vectorized_builtin_info_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `json_type` | B | `json_type` | &mdash; | `json2::dispatch` |
| `json_quote` | B | `json_quote` | &mdash; | `json2::dispatch` |
| `json_unquote` | B | `json_unquote` | &mdash; | `json2::dispatch` |
| `json_sum_crc32` | B | &mdash; | &mdash; | `json2::dispatch` |
| `json_extract` | B | `json_extract` | &mdash; | `json2::dispatch` |
| `json_set_insert_replace` | B | `json_insert`, `json_replace`, `json_set` | &mdash; | `json2::dispatch` |
| `json_merge` | B | &mdash; | `json_merge` | `json2::dispatch` |
| `json_merge_preserve` | B | `json_merge_preserve` | &mdash; | `json2::dispatch` |
| `json_array` | B | `json_array` | &mdash; | `json2::dispatch` |
| `json_object` | B | `json_object` | &mdash; | `json2::dispatch` |
| `json_remove` | B | `json_remove` | &mdash; | `json2::dispatch` |
| `json_member_of` | B | `json_member_of` | &mdash; | `json2::dispatch` |
| `json_contains` | B | `json_contains` | &mdash; | `json2::dispatch` |
| `json_overlaps` | B | &mdash; | `json_overlaps` | `json2::dispatch` |
| `json_contains_path` | A+C+E | `abs`, `ceil`, `ceiling`, `exp`, `floor`, `json_depth`, `json_keys`, `json_length`, `json_merge_patch`, `json_type`, `json_valid`, `log`, `log10`, `log2`, `pow`, `regexp_like`, `round`, `truncate` | `json_array_append`, `json_array_insert`, `json_contains_path`, `json_pretty`, `json_schema_valid`, `json_search`, `json_storage_free`, `json_storage_size`, `rand`, `time` | `json2::dispatch` |
| `json_length` | B | `json_length` | &mdash; | `json2::dispatch` |
| `json_keys` | B | `json_keys` | &mdash; | `json2::dispatch` |
| `json_depth` | B | `json_depth` | &mdash; | `json2::dispatch` |
| `json_array_append` | B | &mdash; | `json_array_append` | `json2::dispatch` |
| `json_search` | B | &mdash; | `json_search` | `json2::dispatch` |
| `json_array_insert` | B | &mdash; | `json_array_insert` | `json2::dispatch` |
| `json_valid` | B | `json_valid` | &mdash; | `json2::dispatch` |
| `json_storage_free` | A+C+E | `abs`, `ceil`, `ceiling`, `exp`, `floor`, `json_keys`, `json_length`, `json_merge_patch`, `json_type`, `log`, `log10`, `log2`, `pow`, `regexp_like`, `round`, `truncate` | `json_pretty`, `json_schema_valid`, `json_storage_free`, `json_storage_size`, `rand`, `time` | `json2::dispatch` |
| `json_storage_size` | B | `json_merge_patch` | `json_pretty`, `json_storage_size` | `json2::dispatch` |
| `json_pretty` | B | &mdash; | `json_pretty` | `json2::dispatch` |
| `json_merge_patch` | B | `json_merge_patch` | &mdash; | `json2::dispatch` |
| `json_schema_valid` | B | &mdash; | `json_schema_valid` | `json2::dispatch` |
| `json_schema_valid_cache` | E | &mdash; | `json_schema_valid` | &mdash; |
| `vectorized_builtin_json_func` | B | `json_keys`, `json_length`, `json_type` | &mdash; | `json2::dispatch` |
| `benchmark_vectorized_builtin_json_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `like` | A+C | &mdash; | &mdash; | &mdash; |
| `regexp` | A | &mdash; | &mdash; | &mdash; |
| `ci_like` | X | &mdash; | &mdash; | &mdash; |
| `vectorized_builtin_like_func` | A+C | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_like_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `abs` | X | `abs` | &mdash; | &mdash; |
| `ceil` | X | `ceil`, `ceiling` | &mdash; | &mdash; |
| `exp` | X | `exp` | &mdash; | &mdash; |
| `floor` | X | `floor` | &mdash; | &mdash; |
| `log` | X | `log` | &mdash; | &mdash; |
| `log2` | X | `log2` | &mdash; | &mdash; |
| `log10` | X | `log10` | &mdash; | &mdash; |
| `rand` | E | &mdash; | `rand` | &mdash; |
| `pow` | X | `pow` | &mdash; | &mdash; |
| `round` | X | `round` | &mdash; | &mdash; |
| `truncate` | X | `truncate` | &mdash; | &mdash; |

### `builtin_math_misc_op_source.rs`

Rank **(a)**; 34 tests: a=21, b=1, c=6, gap=6.

Go source: Source-first ports of `pkg/expression.part4` (`func Test*` items 181–240 on `origin/master`, sorted by file path then line): the tail of `builtin_math_test.go` (`TestCRC32` .. `TestCot`), the shared `builtin_math_vec_test.go` map harnesses, the whole `builtin_miscellaneous_test.go` family and its vectorized sibling, `builtin_op_test.go` and `builtin_op_vec_test.go`, and `builtin_other_test.go::TestBitCount`. Every expectation was re-derived from the Go source on `origin/master`, not from earlier notes; families already pinned by earlier batches are cited in the receipt rather than duplicated row-for-row here.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `crc32_gbk_charset_connection_rows` | E | `crc32` | &mdash; | &mdash; |
| `conv_digit_overflow_above_u64_errors_with_the_digits` | E | `conv` | &mdash; | &mdash; |
| `conv_source_table_type_and_valid_prefix_rows` | A+E | `conv` | &mdash; | &mdash; |
| `math_string_coercion_raises_one_truncate_warning_each` | X | `acos`, `asin`, `atan`, `cos`, `degrees`, `radians`, `sin`, `tan` | &mdash; | &mdash; |
| `cot_zero_overflows_as_double_error` | A | `cot` | &mdash; | &mdash; |
| `math_overflow_errors_render_the_source_expression` | E | `cot`, `exp`, `pow` | &mdash; | &mdash; |
| `pi_is_the_exact_f64_constant` | X | `pi` | &mdash; | &mdash; |
| `vectorized_builtin_math_eval_one_vec` | A+C+E | `abs`, `acos`, `asin`, `atan`, `ceil`, `cos`, `cot`, `crc32`, `degrees`, `exp`, `floor`, `log`, `log10`, `log2`, `pi`, `pow`, `radians`, `round`, `sign`, `sin`, `sqrt`, `tan`, `truncate` | `rand` | &mdash; |
| `vectorized_builtin_math_func` | A+C | `exp`, `pow`, `round`, `sign`, `truncate` | &mdash; | &mdash; |
| `vectorized_builtin_math_func_for_rand` | E | &mdash; | `rand` | &mdash; |
| `benchmark_vectorized_builtin_math_eval_one_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_math_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `uuid_generation_v1_v4_v7_shapes` | X | &mdash; | `uuid`, `uuid_v4`, `uuid_v7` | &mdash; |
| `vectorized_builtin_miscellaneous_eval_one_vec` | B | `any_value`, `inet6_aton`, `inet_aton`, `inet_ntoa`, `is_ipv4`, `is_ipv4_mapped`, `is_ipv6` | `is_uuid`, `name_const`, `uuid_timestamp`, `uuid_to_bin`, `uuid_version` | `compare2::inet_aton_go_vectors` |
| `vectorized_builtin_miscellaneous_func` | X | &mdash; | `bin_to_uuid`, `uuid_to_bin` | &mdash; |
| `sleep_vectorized_incorrect_argument_levels` | E | &mdash; | `sleep` | &mdash; |
| `sleep_vectorized_timing_strict_real_duration_and_kill_signal` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_miscellaneous_eval_one_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_miscellaneous_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `unary_minus_source_table` | E | `unaryminus` | &mdash; | &mdash; |
| `unary_minus_ret_type_flen_follows_go_sign_reservation` | E | `unaryminus` | &mdash; | &mdash; |
| `logic_and_source_table` | E | `and` | &mdash; | &mdash; |
| `logic_or_source_table` | E | `or` | &mdash; | &mdash; |
| `logic_xor_source_table` | E | `xor` | &mdash; | &mdash; |
| `bit_or_bit_and_complete_tables` | E | `bitand`, `bitor` | &mdash; | &mdash; |
| `bit_neg_source_rows` | E | `bitneg` | &mdash; | &mdash; |
| `unary_not_every_input_domain` | E | `not` | &mdash; | &mdash; |
| `is_true_or_false_full_signature_table` | E | `isfalse`, `istrue` | &mdash; | &mdash; |
| `shift_parameter_count_boundaries` | X | `leftshift`, `rightshift` | &mdash; | &mdash; |
| `bit_xor_parameter_count_boundaries` | X | `bitxor` | &mdash; | &mdash; |
| `builtin_unary_minus_int_sig_columns` | E | &mdash; | &mdash; | &mdash; |
| `vectorized_builtin_op_func` | C+E | `isfalse`, `isnull`, `istrue`, `not` | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_op_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `bit_count_source_table` | A | `bit_count` | &mdash; | &mdash; |

### `builtin_string_time_source.rs`

Rank **(a)**; 57 tests: a=43, b=5, c=1, gap=8.

Go source: Batch b071 ports of `pkg/expression.part6`: `func Test*` items 301–360 on `origin/master`, sorted by file path then line. Each test re-derives its intent from the Go source it exercises (`builtin_string_test.go`, `builtin_string_vec_test.go`, `builtin_string_vec_generated_test.go`, `builtin_test.go` and `builtin_time_test.go`). Items whose tables are fully carried by pre-existing crate tests are cited in `rust/testport/receipts/b071.md` rather than duplicated here.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_field` | A+C | `field` | &mdash; | &mdash; |
| `test_lpad` | A | &mdash; | `lpad` | &mdash; |
| `test_rpad` | A | &mdash; | `rpad` | &mdash; |
| `test_rpad_sig` | E | &mdash; | `rpad` | &mdash; |
| `test_insert_binary_sig` | E | &mdash; | `insert_func` | &mdash; |
| `test_load_file` | A+C | &mdash; | `load_file` | &mdash; |
| `test_make_set` | A+C | &mdash; | `make_set` | &mdash; |
| `test_oct` | A+C | `oct` | &mdash; | &mdash; |
| `test_insert_func_table` | A | &mdash; | `insert_func` | &mdash; |
| `test_from_base64` | A | &mdash; | `from_base64` | &mdash; |
| `test_from_base64_sig` | X | &mdash; | `from_base64` | &mdash; |
| `test_ord_charset_table` | A+E | `ord` | &mdash; | &mdash; |
| `test_elt` | A+C | `elt` | &mdash; | &mdash; |
| `test_quote` | E | `quote` | &mdash; | &mdash; |
| `test_to_base64` | C | &mdash; | `to_base64` | &mdash; |
| `test_to_base64_gbk_session_rows` | C+E | &mdash; | `to_base64` | &mdash; |
| `test_to_base64_sig_packet_boundaries` | E | &mdash; | `to_base64` | &mdash; |
| `test_string_right` | A+C | `right` | &mdash; | &mdash; |
| `test_weight_string_forms` | C+E | `hex` | `weight_string` | &mdash; |
| `test_weight_string_binary_cut_warning` | E | &mdash; | `weight_string` | &mdash; |
| `test_ci_weight_string_table` | C | `hex` | `weight_string` | &mdash; |
| `test_translate_tables` | A+C | `hex`, `unhex` | `translate` | &mdash; |
| `test_format_values_and_number_side_truncate_warnings` | A+E | &mdash; | `format` | `builtin_ext::string2` |
| `test_format_precision_side_truncate_warning_counts` | E | &mdash; | `format` | &mdash; |
| `test_format_with_locale` | C | &mdash; | `format` | &mdash; |
| `test_vectorized_generated_builtin_string_eval_one_vec` | A+C | `field` | &mdash; | &mdash; |
| `test_vectorized_generated_builtin_string_func` | A+C | `field` | &mdash; | &mdash; |
| `test_vectorized_builtin_string_eval_one_vec` | A | `instr`, `locate`, `substring_index`, `unhex` | `insert_func`, `lpad`, `translate` | &mdash; |
| `test_vectorized_builtin_string_func` | A | `instr`, `locate`, `substring_index`, `unhex` | `insert_func`, `lpad`, `translate` | &mdash; |
| `test_vectorized_builtin_string_eval_one_vec_2` | A+C | `bin`, `elt`, `isnull`, `oct`, `quote` | `format`, `from_base64`, `make_set`, `to_base64` | &mdash; |
| `test_vectorized_builtin_string_func_2` | A+C | `bin`, `elt`, `isnull`, `oct`, `quote` | `format`, `from_base64`, `make_set`, `to_base64` | &mdash; |
| `benchmark_vectorized_builtin_string_eval_one_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_string_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_string_eval_one_vec_2` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_builtin_string_func_2` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_generated_builtin_string_eval_one_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_generated_builtin_string_func` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_is_null_func` | A+E | `isnull` | &mdash; | &mdash; |
| `test_lock` | E | &mdash; | `get_lock`, `release_lock` | &mdash; |
| `test_builtin_func_cache_concurrency` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_builtin_func_cache_lifecycle` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_date_delimiter_table` | A+C | `date` | &mdash; | &mdash; |
| `test_date_zero_value_mode_rows` | C | `date` | &mdash; | &mdash; |
| `test_clock_parts_and_invalid_time_warning` | A+C | `hour`, `microsecond`, `minute`, `second` | `time` | `time_fn::dispatch` |
| `test_time_values_and_result_type` | A+C+E | &mdash; | `time` | &mdash; |
| `test_day_of_month_zero_date_rows` | C | `dayofmonth` | &mdash; | &mdash; |
| `test_date_format_zero_year_x_token` | B | `date_format` | &mdash; | `calendar::date_format` |
| `test_now_utc_timestamp_fixed_clock` | B | &mdash; | `now`, `utc_timestamp` | `time_fn::dispatch` |
| `test_add_time_sig_value_tables` | C+E | `addtime` | &mdash; | &mdash; |
| `test_add_time_duration_operand_tables` | E | `addtime` | &mdash; | &mdash; |
| `test_sub_time_duration_operand_tables` | E | `subtime` | &mdash; | &mdash; |
| `test_sub_time_sig_value_tables` | C+E | `subtime` | &mdash; | &mdash; |
| `test_add_sub_time_issue_56861_typed_tables` | E | `addtime`, `subtime` | &mdash; | `add_sub::add_sub_time` |
| `test_from_unixtime_utc_fixed` | B | `from_unixtime` | &mdash; | `time_fn::calendar`, `time_fn::dispatch` |
| `test_from_unixtime_real_uses_go_shortest_decimal_before_rounding` | B | `from_unixtime` | &mdash; | `time_fn::dispatch` |
| `test_current_date_current_time_utc_time_clocks` | B | &mdash; | `curdate`, `current_time`, `utc_time` | `time_fn::dispatch` |
| `locate_with_position_matches_go_three_args_signature` | E | `instr`, `locate` | &mdash; | &mdash; |

### `builtin_time_calendars_source.rs`

Rank **(a)**; 22 tests: a=11, b=0, c=11, gap=0.

Go source: Remaining `pkg/expression/builtin_time_test.go` rows (the alphabetical tail after part6's `TestUTCTime` item: `TestUTCDate` … `TestCurrentTso` at origin/master lines 1779-3723) that sibling carriers do not already pin. Where an earlier carrier exists (`time_fn::tests`, `time_fn/convert_tz`, `time_fn/session_tz`, `tests/go_time_values`) its rows were re-read from `origin/master` and are cited in the batch receipt rather than duplicated here.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `utc_date_answers_the_utc_statement_date` | X | &mdash; | `utc_date` | &mdash; |
| `yearweek_source_rows_pin_zero_month_null_and_boundary_years` | X | `yearweek` | &mdash; | &mdash; |
| `timestamp_diff_flag_block_rows_stay_null` | X | `day`, `timestampdiff` | &mdash; | &mdash; |
| `unix_timestamp_value_table_under_utc` | X | `unix_timestamp` | &mdash; | &mdash; |
| `unix_timestamp_compact_numeric_and_zero_date_rows_match_master` | C | `unix_timestamp` | &mdash; | &mdash; |
| `date_arith_day_month_year_overflow_tables_match_master` | A | &mdash; | `date_add`, `date_sub` | &mdash; |
| `timestamp_delimited_argument_rows_match_master` | A | &mdash; | `timestamp` | &mdash; |
| `timestamp_compact_string_rows_match_master` | A | &mdash; | `timestamp` | &mdash; |
| `timestamp_numeric_integer_and_decimal_rows_match_master` | A | &mdash; | `timestamp` | &mdash; |
| `timestamp_float_rows_match_master` | A | &mdash; | `timestamp` | &mdash; |
| `timestamp_fraction_only_decimal_rows_match_master` | A | &mdash; | `timestamp` | &mdash; |
| `maketime_integer_second_master_rows_overflow_garbage_and_null_arguments` | C | `maketime` | &mdash; | &mdash; |
| `timestamp_add_delimited_rows_match_master` | A | &mdash; | `timestampadd` | &mdash; |
| `timestamp_add_numeric_date_arguments_match_master` | A | &mdash; | `timestampadd` | &mdash; |
| `period_invalid_period_reject_the_call` | X | `period_add`, `period_diff` | &mdash; | &mdash; |
| `time_format_hour_family_rows_match_master` | X | &mdash; | `time_format` | &mdash; |
| `with_time_zone_clock_builtins_render_the_session_zone` | X | &mdash; | `curdate`, `current_time`, `curtime`, `sysdate` | &mdash; |
| `tidb_parse_tso_master_vectors_under_utc` | X | &mdash; | `tidb_parse_tso` | &mdash; |
| `tidb_parse_tso_logical_consecutive_tso_counters` | X | &mdash; | `tidb_parse_tso_logical` | &mdash; |
| `tidb_bounded_staleness_safets_windows_and_monotonicity` | X | &mdash; | `tidb_bounded_staleness` | &mdash; |
| `str_datetime_add_duration_warns_once_with_frozen_arg_text` | A | `addtime` | &mdash; | &mdash; |
| `current_tso_reports_session_transaction_tso` | X | &mdash; | `tidb_current_tso` | &mdash; |

### `builtin_vectorized_time_infra_source.rs`

Rank **(a)**; 14 tests: a=4, b=2, c=2, gap=6.

Go source: GO PORTS of the vectorized-harness slices assigned to this batch: `builtin_time_vec_generated_test.go` (:11718), `builtin_time_vec_test.go` (:567-:610 VecMonth), `builtin_vec_vec_test.go` (:201), and `builtin_vectorized_test.go` (:104-:194 Benchmarks, :564 DoubleRow2Vec, :589 DoubleVec2Row, :744/:758 MockDouble Benchmarks, :775 VectorizedCheck, :804 Float32ColVec, :836 VecEvalBool, :857 RowBasedFilterAndVectorizedFilter). Go drives these through `vecExprBenchCase` tables filled by random data generators and compared across Go's SEPARATE vectorized/row evaluators. This crate has one evaluator per tier, so the same contracts are pinned deterministically: columnar input chunks agree cell-by-cell with the scalar answers every source row demands, and null bits travel with them.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `vectorized_generated_time_unit_families_match_the_row_answers` | A+C | &mdash; | `date_add`, `date_sub` | &mdash; |
| `vectorized_time_harness_representative_cases_match_scalar_answers` | B | `timediff` | `sec_to_time` | `calendar::date_diff` |
| `vectorized_time_format_empty_format_returns_null` | X | &mdash; | `time_format` | &mdash; |
| `vec_month_zero_dates_stay_warning_free_in_both_flag_modes` | X | `month` | &mdash; | &mdash; |
| `elementwise_plus_over_columns_matches_the_mock_contract` | E+S | `plus` | &mdash; | &mdash; |
| `mock_vec_plus_int_parallel_allocator_race` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `double_evaluation_reproduces_the_projected_column_exactly` | E+S | &mdash; | &mdash; | &mdash; |
| `benchmark_mock_double_row_and_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vectorized_check_predicates_over_constants_columns_and_correlated` | E | &mdash; | `setvar` | &mdash; |
| `vectorized_builtin_vec_families_match_master_shapes` | B | `vec_as_text`, `vec_dims`, `vec_l2_norm` | &mdash; | `vec::dispatch` |
| `float32_col_vectorization` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vec_eval_bool_matches_row_eval_bool` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `row_based_filter_and_vectorized_filter_agree` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_vectorized_harnesses` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `compare_control_source.rs`

Rank **(a)**; 17 tests: a=8, b=0, c=5, gap=4.

Go source: GO PORTS of `pkg/expression/builtin_compare_test.go`, `pkg/expression/builtin_control_test.go`, and the vectorized harness tests belonging to those families -- plus `#[ignore]` stubs recording each part those ports cannot reach. Shape vocabulary used by [`shape`]: `lt(col(Some(Long)), Const:INT:1)` mirrors what Go's `Expression.StringWithCtx` prints for the same tree (`columns` keep their type code, constants their datum label).

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_compare_function_with_refine` | X | `cast_decimal`, `cast_double`, `eq`, `ge`, `gt`, `le`, `lt`, `ne`, `nulleq`, `unaryminus` | &mdash; | &mdash; |
| `ast_rewrite_refines_integer_constant_before_comparison_casts` | X | `lt` | &mdash; | &mdash; |
| `refine_exceptional_folds_are_not_modeled` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_compare` | A+C | `cast`, `cast_decimal`, `lt` | `cast_json` | &mdash; |
| `test_case_when` | C | `cast` | &mdash; | &mdash; |
| `test_if_typed_conditions` | A+C | `cast`, `if` | &mdash; | &mdash; |
| `test_ifnull_typed_pairs` | C | `cast`, `ifnull` | &mdash; | &mdash; |
| `test_coalesce` | C | `cast`, `coalesce` | `time` | &mdash; |
| `test_coalesce_fraction_promotion` | C | `cast`, `coalesce` | `time` | &mdash; |
| `test_interval_func` | C | `cast`, `interval` | &mdash; | &mdash; |
| `test_greatest_least_func` | C | `cast`, `greatest`, `least` | &mdash; | &mdash; |
| `test_issue46475` | X | `cast`, `coalesce` | &mdash; | &mdash; |
| `test_refine_args_with_nullable_column` | X | `cast`, `eq` | &mdash; | &mdash; |
| `test_refine_args_with_cast_enum` | X | `cast`, `eq` | &mdash; | &mdash; |
| `vectorized_builtin_compare_harness_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vectorized_generated_builtin_compare_harness_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vectorized_generated_builtin_control_harness_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `compare_time_builtin_rows_source.rs`

Rank **(a)**; 7 tests: a=7, b=0, c=0, gap=0.

Go source: GO PORT of the statement-free rows of `pkg/expression/integration_test/integration_test.go` `TestCompareBuiltin` (`integration_test.go:2661`) and `TestTimeBuiltin` (`integration_test.go:2866`) (batch part10): every `select` in those tests whose arguments are literals rather than table columns. The Go harness runs them through a full session (`tk.MustQuery`); each row reaches `builtinCompareSigForConstantArgs`-style dispatch unchanged by the surrounding executor, so the constant rewrite tier evaluated here pins the identical builtin behavior. Column-shaped and timezone-mutation subtests of the two Go tests live outside this crate's surface.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_compare_builtin_coalesce_rows` | A+C | `cast`, `coalesce` | &mdash; | &mdash; |
| `test_compare_builtin_nullif_rows` | C+E | `hex` | `nullif` | &mdash; |
| `test_compare_builtin_interval_rows` | C+E | `interval` | &mdash; | &mdash; |
| `test_compare_builtin_greatest_least_literal_rows` | C | `cast`, `greatest`, `least` | &mdash; | &mdash; |
| `test_compare_builtin_decimal_uint_boundary_rows` | E | &mdash; | &mdash; | &mdash; |
| `test_compare_builtin_row_constructor_rows` | C | &mdash; | `row` | &mdash; |
| `test_time_builtin_date_year_makedate_literal_rows` | C+E | `date`, `makedate`, `year` | &mdash; | &mdash; |

### `constant_test_go_tables_source.rs`

Rank **(a)**; 14 tests: a=10, b=0, c=0, gap=4.

Go source: GO PORTS of `pkg/expression/constant_test.go` on `origin/master`: `TestConstantFolding` (:198), `TestConstantFoldingCharsetConvert` (:274), plus the deferred/propagation members whose carrier is absent. Go drives each condition through `FoldConstant(ctx, expr)` and asserts the `StringWithCtx` rendering. The renderings embed the casts `NewFunction` inference inserts (`cast(Column#0, double BINARY)`), so the ports assert the FOLD ITSELF structurally: which node collapsed to which constant, and which unfoldable function survived. Function construction goes through [`crate::expr_util::RealFunctionBuilder`] -- the crate's full `NewFunction` -- exactly where Go's test used `newFunction(ctx, ...)`.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `constant_folding_operator_arguments_reduce_in_place` | E | `greatest`, `lt`, `plus` | &mdash; | &mdash; |
| `constant_folding_keeps_rand_unfolded` | E | `eq` | `rand` | &mdash; |
| `constant_folding_isnull_and_unary_not_reduce` | E | `eq`, `isnull`, `not`, `plus` | &mdash; | &mdash; |
| `null_reject_conditions_survive_both_fold_modes` | E | `field` | `concat_ws` | &mdash; |
| `constant_folding_sees_through_internal_charset_transcodes` | E | `from_binary`, `length`, `to_binary` | `concat` | &mdash; |
| `constant_folding_charset_binary_result_matches_source` | E | &mdash; | `concat` | &mdash; |
| `test_constant_propagation` | E | `eq`, `ge`, `gt`, `in`, `lt`, `ne`, `or` | &mdash; | &mdash; |
| `test_constant_propagation_for_outer_join` | E | `eq`, `gt`, `not` | &mdash; | &mdash; |
| `test_deferred_param_not_null` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `deferred_constant_clone_preserves_the_deferred_expression` | E | `plus` | `rand` | &mdash; |
| `test_deferred_expr_not_null` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_get_type_thread_safe` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vectorized_constant_fills_whole_output_chunks` | E+S | &mdash; | &mdash; | &mdash; |
| `vectorized_constant_deferred_forms_fill_like_literals` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `context_override_values_source.rs`

Rank **(c)**; 1 tests: a=0, b=0, c=1, gap=0.

Go source: GO PORT of the part9 test `TestCtxWithHandleTruncateErrLevel` (`pkg/expression/exprctx/context_override_test.go:28`) and `TestExpressionMemeoryUsage` (`pkg/expression/expression_test.go:328`, also stubbed from `vectorizable_and_chunk_eval_source.rs`). The context wrapper is active; memory accounting remains a documented gap beside its receipt row.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_ctx_with_handle_truncate_err_level` | X | &mdash; | &mdash; | &mdash; |

### `convert_using_signature_source.rs`

Rank **(a)**; 3 tests: a=2, b=0, c=0, gap=1.

Go source: Source-first completion of `pkg/expression/builtin_string_test.go::TestConvert` (:850) on `origin/master`. The binary-literal value rows were already pinned by `tests::convert_using_invalid_binary_literal_is_null_in_both_evaluators`; this module pins the remaining halves — the build-time RESULT TYPE claims (charset + default collation + BINARY flag per target charset) and the unknown-charset error table — re-derived from `builtinConvertSig` and its function class (`pkg/expression/builtin_string.go`).

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `convert_using_result_type_carries_target_charset_metadata` | E | `ascii` | `convert` | &mdash; |
| `convert_using_unknown_charset_fails_before_evaluation` | A+C | &mdash; | `convert` | &mdash; |
| `convert_runtime_charset_mutation_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `crypto_encryption_source.rs`

Rank **(b)**; 19 tests: a=1, b=15, c=0, gap=3.

Go source: GO PORTS of `pkg/expression/builtin_encryption_test.go`'s row tables against `crate::builtin_ext::crypto`'s dispatch boundary. Every expected value below was copied from the Go source table; a value only appears here after checking it against the production code the row exercises. Session-shape notes: - Go switches the session's `character_set_connection` before building the constants (`cryptTests`.chs), so its string literals arrive at the builtin already GBK-encoded through `charset.Transform(OpEncode)`. Direct dispatch rows feed PRE-ENCODED byte datums, while the connection-aware rewrite regression exercises the same `to_binary` boundary (see `encoding_error_rows_follow_session_charset_conversion`). - Go selects the AES signature from `@@block_encryption_mode` at getFunction time. Rust reads the same statement snapshot through

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_sql_decode` | B | &mdash; | `decode` | `crypto::dispatch` |
| `test_sql_encode` | B | &mdash; | `encode` | `crypto::dispatch` |
| `test_aes_encrypt` | B | &mdash; | `aes_decrypt`, `aes_encrypt` | `crypto::dispatch` |
| `test_aes_decrypt` | B | &mdash; | `aes_decrypt` | `crypto::dispatch` |
| `test_sha1_hash` | B | `sha` | &mdash; | `crypto::dispatch` |
| `test_sha2_hash` | B | `sha2` | &mdash; | `crypto::dispatch` |
| `test_md5_hash` | B | `md5` | &mdash; | `crypto::dispatch` |
| `encoding_error_rows_follow_session_charset_conversion` | E | `md5` | `password` | &mdash; |
| `test_random_bytes` | B | &mdash; | `random_bytes` | `crypto::dispatch` |
| `test_compress_and_uncompress_length_framing` | B | `compress`, `uncompress` | &mdash; | `crypto::dispatch` |
| `test_uncompress` | B | `uncompress` | &mdash; | `crypto::dispatch` |
| `test_uncompress_length` | B | `uncompressed_length` | &mdash; | `crypto::dispatch` |
| `test_validate_password_strength` | B | &mdash; | `validate_password_strength` | `crypto::dispatch` |
| `test_password` | B | &mdash; | `password` | `crypto::dispatch` |
| `uncompress_rejects_payload_deeper_than_declared_length` | B | `uncompress` | &mdash; | `crypto::dispatch` |
| `uncompress_rejects_handcrafted_payload_larger_than_declared_length` | B | `uncompress` | &mdash; | `crypto::dispatch` |
| `vectorized_builtin_encryption_harness_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `uncompress_memory_tracker_gaps` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `uncompress_overlong_declared_length_vectorized_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `distsql_pb_roundtrip_gap_source.rs`

Rank **(c)**; 4 tests: a=0, b=0, c=0, gap=4.

Go source: `pkg/expression/distsql_builtin_test.go` on `origin/master` ports. The whole `PBToExpr` direction (`pkg/expression/distsql_builtin.go`) -- TiPB wire shapes decoded back into expression trees, including collation ID normalization and per-signature scalar reconstruction -- is unported here; this crate only BUILDS TiPB predicates (`pb_predicate.rs`).

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_pb_to_expr` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_eval` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_pb_to_expr_with_new_collation` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_pb_to_scalar_func_expr` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `evaluator_go_tables_source.rs`

Rank **(a)**; 9 tests: a=4, b=0, c=3, gap=2.

Go source: GO PORTS from `pkg/expression/evaluator_test.go` on `origin/master`: `TestExtract` (:488), `TestMod` (:606), the representable unary-operator kinds of `TestUnaryOp` (:534), `TestSleep` (:102), and the optional-eval-props shape of `TestOptionalProp` (:626). The four binary operator suites (`TestBinopComparison`, `:190`; `TestBinopLogic`, `:268`; `TestBinopBitop`, `:307`; `TestBinopNumeric`, `:344`) are carried whole by sibling module [`super::evaluator_binop`] and are not duplicated here.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `extract_master_unit_table_matches_source` | A | `day`, `hour`, `microsecond`, `minute`, `month`, `quarter`, `second`, `week`, `year` | `extract` | &mdash; |
| `extract_composite_units_over_fractional_strings_match_source` | A | &mdash; | `extract` | &mdash; |
| `mod_source_rows` | A | `mod` | &mdash; | &mdash; |
| `unary_op_kind_rows_match_source` | X | `not` | &mdash; | &mdash; |
| `unary_minus_temporal_and_decimal_operands_match_source` | X | &mdash; | &mdash; | &mdash; |
| `unary_minus_hybrid_and_binary_literal_kinds_match_source` | X | &mdash; | &mdash; | &mdash; |
| `sleep_errctx_levels_and_null_arguments_follow_the_caller` | E | &mdash; | `sleep` | &mdash; |
| `test_sleep_timing_and_kill_signal_halves` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_optional_prop` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `expr_to_pb_lowering_gap_source.rs`

Rank **(c)**; 20 tests: a=0, b=0, c=0, gap=20.

Go source: GO PORT record for the `pkg/expression/expr_to_pb_test.go` slice owned by this batch (`TestConstant2Pb` :44 ... `TestMetadata` :2046). Every one of these tests drives Go's full `PushDownExprs` + `ExpressionsToPBList` pipeline and asserts EXACT TiPB JSON serializations (negative collation ids for new collations, per-signature ScalarFuncSig codes, InUnion proto bytes). That lowering (`pkg/expression/expr_to_pb.go`) is deliberately unported in this workspace; the slices that DO exist here are policy-level admission tables (`pushdown_catalog.rs`, `infer_pushdown.rs`) and the integer/string-predicate TiPB shapes in `pb_predicate.rs`. Each test is therefore recorded as an explicit ignored stub naming what WOULD be exercised. The sibling module [`super::expr_to_pb_switcher_source`] carries part9's three tail tests of the same file with the same rationale.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_constant_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_column_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_compare_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_like_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_arithmetical_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_date_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_logical_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_bitwise_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_control_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_other_func_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_json_push_down_to_flash` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_expr_push_down_to_flash` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_expr_only_push_down_to_flash` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_expr_push_down_to_tikv` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_expr_only_push_down_to_tikv` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_group_by_item_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_sort_by_item_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_push_collation_down` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_new_collations_enabled` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_metadata` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `expr_to_pb_switcher_source.rs`

Rank **(c)**; 3 tests: a=0, b=0, c=0, gap=3.

Go source: GO PORTS of `pkg/expression/expr_to_pb_test.go` items 481-483 of the part9 slice: `TestPushDownSwitcher` (:2087), `TestPanicIfPbCodeUnspecified` (:2183) and `TestProjectionColumn2Pb` (:2204). All three drive Go's full `ExpressionsToPBList` / `PbConverter.ExprToPB` pipeline, which is deliberately unported in this workspace; each is recorded here as an `#[ignore]` stub with its go-parity-gap reason and an anchor naming what WOULD be exercised.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_push_down_switcher` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_panic_if_pb_code_unspecified` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_projection_column_2_pb` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `expression_null_const_source.rs`

Rank **(a)**; 6 tests: a=6, b=0, c=0, gap=0.

Go source: GO PORTS of `pkg/expression/expression_test.go`'s constant-node table tests: `TestConstant` (:135), `TestIsBinaryLiteral` (:157) and `TestConstLevel` (:173), plus the unportable fragments recorded as `#[ignore]` stubs with their go-parity-gap reasons. Go builds every case through `newFunctionWithMockCtx`, which runs the real `NewFunction`; a node's level therefore reflects what CONSTRUCTION produces, not just the tree shape. This port mirrors that by using [`crate::expr_util::RealFunctionBuilder`] for the registered function names Go uses (`abs`, `plus`, `rand`, `uuid`, `getparam`) and direct node construction only where Go's own walk leaves an unfolded node in place.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_constant_core_invariants` | E | &mdash; | &mdash; | &mdash; |
| `test_constant_decorrelate_and_propagate_type_fragments` | E | &mdash; | &mdash; | &mdash; |
| `test_scalar_function_decorrelate_rebuilds_arguments` | E | `plus` | &mdash; | &mdash; |
| `test_correlated_decorrelate_requires_schema` | E | &mdash; | &mdash; | &mdash; |
| `test_is_binary_literal_kind_membership` | E | &mdash; | &mdash; | &mdash; |
| `test_const_level_case_table` | E | `abs`, `plus`, `unix_timestamp` | `getparam`, `rand`, `uuid` | &mdash; |

### `expression_with_null_source.rs`

Rank **(a)**; 6 tests: a=4, b=0, c=1, gap=1.

Go source: GO PORTS of the `EvaluateExprWithNull` family from `pkg/expression/expression_test.go` (`TestEvaluateExprWithNull`, `TestEvaluateExprWithNullMeetError`, `TestEvaluateExprWithNullAndParameters`, `TestEvaluateExprWithNullNoChangeRetType`). # Which Go machinery each stage maps onto - Construction/rebuild: [`MasterFunctionBuilder`] composes this workspace's two ported halves of Go's `NewFunction`: the registry/typing layer ([`crate::new_function::new_function`]) followed by the public `FoldConstant` port ([`crate::expr_util::fold_constant_with`], carrying Go's `specialFoldHandler` set -- `Ifnull`/`If`/`Case`/`IsNull`). Composition is needed because `new_function` itself drives only the rewriter-tier constant fold, which folds all-constant calls but omits the special handlers master applies inside every `NewFunction` (`scalar_function.go:357` -> `FoldConstant`, whose handler dispatch sits at `constant_fold.go:172`).

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `evaluate_expr_with_null_neutralizes_only_schema_columns` | E | `ifnull` | &mdash; | &mdash; |
| `evaluate_expr_with_null_meets_rebuild_error` | E | `ifnull` | &mdash; | &mdash; |
| `evaluate_expr_with_null_folds_a_column_against_a_literal` | E | `lt` | &mdash; | &mdash; |
| `evaluate_expr_with_null_parameter_marker_and_json_flag_halves` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `the_walk_requires_the_folding_builder_to_reduce_rebuilt_nodes` | E | `ifnull` | &mdash; | &mdash; |
| `parse_to_json_flag_is_the_documented_flag_bit` | X | &mdash; | &mdash; | &mdash; |

### `filter_extract_dnf_source.rs`

Rank **(c, structure)**; 1 test. Auto-tier flags it `A` because it builds
expressions, but its subject is a rewriter transform that survives deletion,
so it needs no engine re-pointing.

Go source: GO PORT of `pkg/expression/integration_test/integration_test.go:1766` `TestFilterExtractFromDNF` (batch part10). The Go test builds `select * from t where <expr>` through the full planner, runs `expression.PushDownNot` (`util.go:1141`) over the selection's conditions, then `expression.ExtractFiltersFromDNFs` (`util.go:1204`), sorts by `HashCode`, and pins `StringifyExpressionsWithCtx` output (`expression.go:1334`). The Rust side carries the two pure transforms in this crate (`expr_util::push_not::push_down_not`, `expr_util::normal_form::extract_filters_from_dnfs`), so each Go case is rebuilt here as an already-resolved tree over the same three columns (`test.t.a/b/c`) — the plan-building steps the Go harness performs are what this evaluator cannot see, not the transforms under test.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_filter_extract_from_dnf_case_table` | E | `and`, `eq`, `gt`, `lt`, `or` | &mdash; | &mdash; |

### `find_in_set_lookup_source.rs`

Rank **(a)**; 4 tests: a=3, b=0, c=0, gap=1.

Go source: Source-first ports of `pkg/expression.part5`'s FIND_IN_SET lookup-variant tests on `origin/master`: `builtin_string_test.go::TestFindInSetConstStrlistLookup` (:1107), `::TestFindInSetVecFirstMatchNonConstStrlist` (:1173), and `::TestFindInSetConstOnlyInContextStrlistLookup` (:1218). Those Go tests assert two things together: the VALUE semantics of membership lookup (pad-space collations still distinguish trailing spaces because the signature keys with `KeyWithoutTrimRightSpace`, first member wins) and the internal `constStrlistLookupCache` lifecycle. The cache internals have no Rust counterpart; every observable VALUE behavior is pinned here.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `find_in_set_const_strlist_pad_space_lookup_value_rows` | C | `find_in_set` | &mdash; | &mdash; |
| `find_in_set_non_const_strlist_rows_evaluate_per_row` | A+C | `find_in_set` | &mdash; | &mdash; |
| `find_in_set_const_only_in_context_value_rows` | C | `find_in_set` | &mdash; | &mdash; |
| `find_in_set_strlist_cache_lifecycle_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `function_traits_source.rs`

Rank **(c)**; 2 tests: a=0, b=0, c=1, gap=1.

Go source: GO PORTS of `pkg/expression/function_traits_test.go`: `TestUnfoldableFuncs` (:24) and `TestIllegalFunctions4GeneratedColumns` (:40).

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `unfoldable_functions_contains_sysdate` | X | `abs`, `plus` | `getparam`, `getvar`, `getvar_string`, `rand`, `sysdate`, `uuid` | &mdash; |
| `test_illegal_functions_4_generated_columns_known_good_list` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `hash_group_key_codec_matrix_source.rs`

Rank **(c)**; 1 tests: a=0, b=0, c=1, gap=0.

Go source: GO PORT of `pkg/expression/util_test.go:358` `TestHashGroupKey` (batch part11 item 659). Go's invariant, for each of the seven eval types {int, real, decimal, string, timestamp, datetime, duration}: over a 1024-row generated column, every per-row key produced by `codec.HashGroupKey(tz, n, colBuf, bufs, ft)` (`pkg/util/codec/codec.go`) equals `codec.EncodeValue(tz, datum)` of that row's own value. This module pins the same equality through this workspace's transcreated halves — `tidb_codec::hash_group_key_in_timezone` versus `tidb_codec::encode_value_in_timezone` — with two documented substitutions: - DETERMINISM: Go fills the column through `fillColumnWithGener` + `newDefaultGener(0.2, eType)` (`pkg/expression/bench_test.go:1237`), a randomly seeded generator whose ~20% NULL ratio is mirrored here by a fixed value matrix with the same NULL density and boundary-value shape.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_hash_group_key_row_keys_equal_encode_value` | X | &mdash; | `timestamp` | &mdash; |

### `helper_current_timestamp_source.rs`

Rank **(a)**; 4 tests: a=4, b=0, c=0, gap=0.

Go source: GO PORT of `pkg/expression/helper_test.go::TestCurrentTimestampTimeZone` (`helper_test.go:157`) and the deterministic `NOW()` rows of `pkg/expression/integration_test/integration_test.go::TestTimestamp` (`integration_test.go:2585`) (batch part10). Go seeds the session `"timestamp"` sysvar with a fixed UTC second count (`helper.go:199` `getStmtTimestamp`) and re-renders CURRENT_TIMESTAMP in the statement zone (`helper.go:73` `getTimeCurrentTimeStamp`). The Rust evaluator carries that composition through its [`Columns::now`] clock and [`ColumnResolver::time_zone`]; this port pins both master rows: - `timestamp=1234`, `time_zone=+00:00` → `1970-01-01 00:20:34`; - `timestamp=1234`, `time_zone=+08:00` → `1970-01-01 08:20:34` (helper_test.go:169-180), i.e. changing ONLY the timezone changes the value, because the sysvar instant itself is UTC.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_current_timestamp_time_zone_case_table` | E | &mdash; | `now` | &mdash; |
| `test_timestamp_sysvar_renders_fixed_now_literal_rows` | E | &mdash; | `now` | &mdash; |
| `test_get_time_value_build_context_helper` | E | &mdash; | `current_timestamp` | &mdash; |
| `test_is_current_timestamp_expr_predicate` | E | &mdash; | `current_timestamp` | &mdash; |

### `ilike_info_cast_source.rs`

Rank **(a)**; 14 tests: a=8, b=0, c=0, gap=6.

Go source: GO PORTS of `pkg/expression/builtin_ilike_test.go`, `pkg/expression/builtin_info_test.go`, and the cast-vectorized specials from `pkg/expression/builtin_cast_vec_test.go`, plus `#[ignore]` stubs for everything those ports cannot reach on this tier.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_ilike` | E | &mdash; | `ilike` | &mdash; |
| `vectorized_builtin_ilike_harness_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vectorized_builtin_ilike_for_constants_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_database` | E | &mdash; | `database`, `found_rows`, `schema` | &mdash; |
| `test_found_rows` | E | &mdash; | `found_rows` | &mdash; |
| `test_user` | E | &mdash; | `found_rows`, `user` | &mdash; |
| `test_current_user` | E | &mdash; | `current_user`, `found_rows` | &mdash; |
| `test_current_resource_group` | E | &mdash; | `current_resource_group`, `found_rows` | &mdash; |
| `test_current_resource_group_ast_path` | E | &mdash; | `current_resource_group` | &mdash; |
| `test_embed_text_builtin` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_embed_text_builtin_null_and_errors` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_vectorized_cast_real_as_time` | C | `cast` | &mdash; | &mdash; |
| `vectorized_cast_string_as_decimal_union_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `vectorized_builtin_cast_harness_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `in_func_decimal_collation_source.rs`

Rank **(a)**; 4 tests: a=4, b=0, c=0, gap=0.

Go source: Source-first ports of `pkg/expression.part5`'s IN-family extras: `builtin_other_vec_test.go::TestInDecimal`, the collation tail row of `builtin_other_test.go::TestInFunc`, and the representable arms of the generated vectorized-IN harnesses (`builtin_other_vec_generated_test.go::TestVectorizedBuiltinOtherEvalOneVecGenerated` / `TestVectorizedBuiltinOtherFuncGenerated`). Expectations are re-derived from the Go sources on `origin/master`.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `in_decimal_across_scales_compares_numerically` | E | `in` | &mdash; | &mdash; |
| `in_func_collation_row_general_ci_folds_case_and_accents` | C | `in` | &mdash; | &mdash; |
| `generated_in_harness_int_string_decimal_arms_agree_across_tiers` | A+C | `in` | &mdash; | &mdash; |
| `generated_in_harness_temporal_duration_json_arms` | C | `cast`, `in` | &mdash; | &mdash; |

### `json_merge_patch_integration_source.rs`

Rank **(a)**; 2 tests: a=2, b=0, c=0, gap=0.

Go source: GO PORT of `pkg/expression/integration_test/integration_test.go` `TestBuiltinFuncJSONMergePatch_InColumn` (`integration_test.go:2342`) and `TestBuiltinFuncJSONMergePatch_InExpression` (`integration_test.go:2412`) (batch part10). The Go harness runs the two-argument call first through stored `j JSON` / `vc VARCHAR(5000)` columns and then through plain session parameters; both reach the same `builtinJSONMergePatchSig` evaluation. Here the In-column shape is evaluated over a one-row chunk whose columns carry exactly those field types, and the in-expression shape through the constant rewrite tier, so the string→document parse, the SQL-NULL truncation rules and the RFC 7396 merge are all code paths under test rather than duplicated logic. Expected documents are normalized exactly like Go's own assertion does: `types.ParseBinaryJSONFromString(tt.expected).String()` (`integration_test.go:2404`, `2507`) turns each expectation into canonical

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_builtin_func_json_merge_patch_in_column_table` | E | &mdash; | &mdash; | &mdash; |
| `test_builtin_func_json_merge_patch_in_expression_table` | E | &mdash; | &mdash; | &mdash; |

### `regexp_vec_cache_source.rs`

Rank **(a)**; 7 tests: a=5, b=0, c=0, gap=2.

Go source: Source-first ports of `pkg/expression.part5`'s vectorized regexp harnesses (`builtin_regexp_test.go::TestRegexpLikeVec/TestRegexpSubstrVec/ TestRegexpInStrVec/TestRegexpReplaceVec`), `builtin_regexp_vec_const_test.go::TestVectorizedBuiltinRegexpForConstants`, and the memoization contract `builtin_regexp_test.go::TestRegexpCache` pins on Go's side. The scalar value tables themselves were ported earlier (`crate::builtin_ext::regexp::tests`, `tests::regexp_like`); this module re-derives the HARNESS dimensions those Go tests add on top — their exact generator arrays swept as cross-tier agreements, plus the constant-pattern corpus invariant.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `regexp_like_vec_generator_matrix_agrees_across_tiers` | A+C | `regexp_like` | &mdash; | &mdash; |
| `regexp_substr_vec_generator_matrix_agrees_across_tiers` | A+C | `regexp_substr` | &mdash; | &mdash; |
| `regexp_instr_vec_generator_matrix_agrees_across_tiers` | A+C | `regexp_instr` | &mdash; | &mdash; |
| `regexp_replace_vec_generator_matrix_agrees_across_tiers` | A+C | `regexp_replace` | &mdash; | &mdash; |
| `regexp_constant_pattern_corpus_matches_scalar_evaluation` | A+C | `regexp_like` | &mdash; | &mdash; |
| `regexp_cache_identity_by_statement_context_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `regexp_and_other_vec_benchmark_gap` (ign) | -- | &mdash; | &mdash; | &mdash; |

### `scalar_function_semantics_source.rs`

Rank **(a)**; 9 tests: a=7, b=0, c=0, gap=2.

Go source: GO PORTS from `pkg/expression/scalar_function_test.go` (batch part11 items 617-623), read from `origin/master`. Every ported assertion re-derives its expectation from the Go production sources the tests exercise: `pkg/expression/scalar_function.go` (the `ScalarFunction`, its cached `hashcode`, and `ReHashCode` (`scalar_function.go:281`)), `pkg/expression/expression.go:1168` (`NewValuesFunc`) and `pkg/expression/constant.go:37` (`NewOne`). Construction note (same convention as `expr_util::tests`): Go's `newFunctionWithMockCtx` runs `NewFunctionInternal`, whose compare-class path wraps both operands of a comparison into casts over the common supertype inside `getFunction` (`builtin_compare.go`, `newBaseBuiltinFuncWithTp` -> `WrapWithCastAsReal`). For `lt(double-col, one)` the right constant is FOLDED to a plain `Constant{Float64(1)}` with `TypeDouble` before it lands in `GetArgs()`. The Rust tree is therefore

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_scalar_function` | E | `lt` | &mdash; | &mdash; |
| `new_values_func_sig_identity` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_scalar_function_equal_after_clean_hash_code` | E | `lt` | &mdash; | &mdash; |
| `test_expression_semantic_equal` | E | `and`, `eq`, `ge`, `gt`, `le`, `lt`, `mul`, `not`, `or`, `plus` | &mdash; | &mdash; |
| `test_column_substitute_grouping_cleans_hash_code` | E | &mdash; | `grouping` | &mdash; |
| `grouping_construction_requires_metadata` | E | &mdash; | `grouping` | &mdash; |
| `test_issue_23309` | E | `ne` | &mdash; | &mdash; |
| `test_scalar_funcs_2_exprs` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `test_scalar_function_hash64_equals` | E | `gt`, `lt` | &mdash; | &mdash; |

### `setvar_getvar_values_getparam_source.rs`

Rank **(a)**; 8 tests: a=8, b=0, c=0, gap=0.

Go source: Source-first ports of `pkg/expression.part5` session-variable functions: `builtin_other_test.go::TestSetVar/TestGetVar/TestTypeConversion/ TestSetVarFromColumn/TestGetParam`, `builtin_other_vec_test.go::TestGetParamVec` plus that file's SETVAR/GETVAR harness arms. Every expectation is re-derived from the Go source on `origin/master` (`pkg/expression/builtin_other.go`'s signatures), not from Rust comments.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `setvar_stores_session_value_and_returns_it` | E | &mdash; | `setvar` | &mdash; |
| `getvar_time_variable_signature` | E | &mdash; | `time` | &mdash; |
| `getvar_reads_typed_session_value_by_signature_kind` | E | &mdash; | `time` | &mdash; |
| `type_conversion_read_one_stored_int_through_decimal_and_real_signatures` | E | &mdash; | `time` | &mdash; |
| `setvar_from_column_snapshots_the_row_value` | E | &mdash; | `setvar` | &mdash; |
| `values_function_reads_the_current_insert_row` | E | &mdash; | `values` | &mdash; |
| `getparam_function_evaluation_matches_plan_cache_values` | E | &mdash; | `getparam` | &mdash; |
| `vectorized_builtin_other_func_representable_arms` | A+C+E | `bit_count` | `setvar`, `time` | &mdash; |

### `util_filter_condition_source.rs`

Rank **(c, structure)**; 1 test. Same reasoning: it drives the rewriter's
predicate-append loop (`crate::expr_util::extract`), not an evaluator kernel.

Go source: GO PORT of `pkg/expression/util_test.go:333` `TestFilter` (batch part11). Go's test drives `Filter(result, conditions, isLogicOrFunction)` (`util.go:Filter`, a predicate-append loop returning the KEPT conditions in input order) over three built expressions and requires exactly the one `or` condition to survive. This crate carries only the in-place twin of that function, `expr_util::extract::filter_out_in_place` (`util.go:FilterOutInPlace`), whose `filtered` half returns exactly the conditions `Filter` would have appended — same predicate, same elements. The port therefore expresses Go's expectation through the sibling carrier and additionally checks its complement. With a single matching condition the reverse-order collection inside `FilterOutInPlace` cannot change the outcome, so no ordering assumption is smuggled in.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_filter_keeps_exactly_the_predicate_matches` | E | `eq`, `or` | &mdash; | `extract::filter_out_in_place` |

### `vectorizable_and_chunk_eval_source.rs`

Rank **(a)**; 4 tests: a=2, b=0, c=1, gap=1.

Go source: GO PORTS of `pkg/expression/expression_test.go`'s vectorization tests: `TestVectorizable` (:196) and `TestEvalExpr` (:299), plus the unportable `TestExpressionMemeoryUsage` (:328) recorded as an `#[ignore]` stub with its go-parity-gap reason.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_vectorizable_over_go_expression_table` | E | &mdash; | `getvar`, `lastval`, `nextval`, `rand`, `setval`, `setvar` | &mdash; |
| `test_eval_expr_column_projected_through_both_modes_agrees` | E+S | &mdash; | &mdash; | &mdash; |
| `test_expression_memory_usage` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `duration_constructor_argument_order_is_pinned` | X | &mdash; | &mdash; | &mdash; |

### `vectorized_filter_consider_null_gap_source.rs`

Rank **(c)**; 3 tests: a=0, b=0, c=0, gap=3.

Go source: `pkg/expression/builtin_vectorized_test.go:878 TestVectorizedFilterConsiderNull` and its two `Benchmark*` siblings on `origin/master`. Go runs a randomized five-column/16-round harness comparing `VectorizedFilterConsiderNull` with vectorized evaluation forced OFF and ON, including over a random selection (`SetSel`), then checks the selected mask equals the second run ANDed with the unselected rows. The selection-buffer machinery (`VecEvalBool`, `rowBasedFilter`, selected/nulls buffers) is unported in this crate -- see sibling module [`super::builtin_vectorized_time_infra_source`] for the adjacent `TestVecEvalBool` / `TestRowBasedFilterAndVectorizedFilter` gap stubs from the same file family.

| `#[test] fn` | tier | admitted subject names | excluded subject names | native helpers |
| --- | --- | --- | --- | --- |
| `test_vectorized_filter_consider_null` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_float32_col_row` (ign) | -- | &mdash; | &mdash; | &mdash; |
| `benchmark_float32_col_vec` (ign) | -- | &mdash; | &mdash; | &mdash; |

---

## 2. Admitted-name cross-tabulation

### 2.1 Admitted names with at least one source-port test (141)

| admitted name | source-port files | representative tests |
| --- | --- | --- |
| `abs` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs`, `expression_null_const_source.rs`, `function_traits_source.rs` | `builtin_info_json_math_source.rs::abs`, `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `acos` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `addtime` | `builtin_string_time_source.rs`, `builtin_time_calendars_source.rs` | `builtin_string_time_source.rs::test_add_sub_time_issue_56861_typed_tables`, `builtin_string_time_source.rs::test_add_time_duration_operand_tables`, `builtin_string_time_source.rs::test_add_time_sig_value_tables` ... |
| `and` | `builtin_math_misc_op_source.rs`, `filter_extract_dnf_source.rs`, `scalar_function_semantics_source.rs` | `builtin_math_misc_op_source.rs::logic_and_source_table`, `filter_extract_dnf_source.rs::test_filter_extract_from_dnf_case_table`, `scalar_function_semantics_source.rs::test_expression_semantic_equal` |
| `any_value` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `ascii` | `convert_using_signature_source.rs` | `convert_using_signature_source.rs::convert_using_result_type_carries_target_charset_metadata` |
| `asin` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `atan` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `bin` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func_2` |
| `bit_count` | `builtin_math_misc_op_source.rs`, `setvar_getvar_values_getparam_source.rs` | `builtin_math_misc_op_source.rs::bit_count_source_table`, `setvar_getvar_values_getparam_source.rs::vectorized_builtin_other_func_representable_arms` |
| `bitand` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::bit_or_bit_and_complete_tables` |
| `bitneg` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::bit_neg_source_rows` |
| `bitor` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::bit_or_bit_and_complete_tables` |
| `bitxor` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::bit_xor_parameter_count_boundaries` |
| `cast` | `builtin_info_json_math_source.rs`, `compare_control_source.rs`, `compare_time_builtin_rows_source.rs`, `ilike_info_cast_source.rs`, `in_func_decimal_collation_source.rs` | `builtin_info_json_math_source.rs::bench_mark`, `compare_control_source.rs::test_case_when`, `compare_control_source.rs::test_coalesce` ... |
| `cast_decimal` | `aggregation_arithmetic_cast_source.rs`, `compare_control_source.rs` | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_decimal`, `aggregation_arithmetic_cast_source.rs::test_cast_functions_bad_string_as_decimal_reads_zero_silently`, `aggregation_arithmetic_cast_source.rs::test_cast_functions_bad_string_as_decimal_warns_1292` ... |
| `cast_double` | `aggregation_arithmetic_cast_source.rs`, `compare_control_source.rs` | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_real`, `compare_control_source.rs::test_compare_function_with_refine` |
| `ceil` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::ceil`, `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `ceiling` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::ceil`, `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free` |
| `coalesce` | `compare_control_source.rs`, `compare_time_builtin_rows_source.rs` | `compare_control_source.rs::test_coalesce`, `compare_control_source.rs::test_coalesce_fraction_promotion`, `compare_control_source.rs::test_issue46475` ... |
| `compress` | `crypto_encryption_source.rs` | `crypto_encryption_source.rs::test_compress_and_uncompress_length_framing` |
| `conv` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::conv_digit_overflow_above_u64_errors_with_the_digits`, `builtin_math_misc_op_source.rs::conv_source_table_type_and_valid_prefix_rows` |
| `cos` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `cot` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::cot_zero_overflows_as_double_error`, `builtin_math_misc_op_source.rs::math_overflow_errors_render_the_source_expression`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `crc32` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::crc32_gbk_charset_connection_rows`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `date` | `builtin_string_time_source.rs`, `compare_time_builtin_rows_source.rs` | `builtin_string_time_source.rs::test_date_delimiter_table`, `builtin_string_time_source.rs::test_date_zero_value_mode_rows`, `compare_time_builtin_rows_source.rs::test_time_builtin_date_year_makedate_literal_rows` |
| `date_format` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_date_format_zero_year_x_token` |
| `day` | `builtin_time_calendars_source.rs`, `evaluator_go_tables_source.rs` | `builtin_time_calendars_source.rs::timestamp_diff_flag_block_rows_stay_null`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `dayofmonth` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_day_of_month_zero_date_rows` |
| `degrees` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `div` | `aggregation_arithmetic_cast_source.rs` | `aggregation_arithmetic_cast_source.rs::test_decimal_err_overflow`, `aggregation_arithmetic_cast_source.rs::test_vectorized_builtin_arithmetic_func` |
| `elt` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_elt`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func_2` |
| `eq` | `compare_control_source.rs`, `constant_test_go_tables_source.rs`, `filter_extract_dnf_source.rs`, `scalar_function_semantics_source.rs`, `util_filter_condition_source.rs` | `compare_control_source.rs::test_compare_function_with_refine`, `compare_control_source.rs::test_refine_args_with_cast_enum`, `compare_control_source.rs::test_refine_args_with_nullable_column` ... |
| `exp` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::exp`, `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `field` | `builtin_string_time_source.rs`, `constant_test_go_tables_source.rs` | `builtin_string_time_source.rs::test_field`, `builtin_string_time_source.rs::test_vectorized_generated_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_generated_builtin_string_func` ... |
| `find_in_set` | `find_in_set_lookup_source.rs` | `find_in_set_lookup_source.rs::find_in_set_const_only_in_context_value_rows`, `find_in_set_lookup_source.rs::find_in_set_const_strlist_pad_space_lookup_value_rows`, `find_in_set_lookup_source.rs::find_in_set_non_const_strlist_rows_evaluate_per_row` |
| `floor` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::floor`, `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `from_binary` | `constant_test_go_tables_source.rs` | `constant_test_go_tables_source.rs::constant_folding_sees_through_internal_charset_transcodes` |
| `from_unixtime` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_from_unixtime_real_uses_go_shortest_decimal_before_rounding`, `builtin_string_time_source.rs::test_from_unixtime_utc_fixed` |
| `ge` | `compare_control_source.rs`, `constant_test_go_tables_source.rs`, `scalar_function_semantics_source.rs` | `compare_control_source.rs::test_compare_function_with_refine`, `constant_test_go_tables_source.rs::test_constant_propagation`, `scalar_function_semantics_source.rs::test_expression_semantic_equal` |
| `greatest` | `compare_control_source.rs`, `compare_time_builtin_rows_source.rs`, `constant_test_go_tables_source.rs` | `compare_control_source.rs::test_greatest_least_func`, `compare_time_builtin_rows_source.rs::test_compare_builtin_greatest_least_literal_rows`, `constant_test_go_tables_source.rs::constant_folding_operator_arguments_reduce_in_place` |
| `gt` | `compare_control_source.rs`, `constant_test_go_tables_source.rs`, `filter_extract_dnf_source.rs`, `scalar_function_semantics_source.rs` | `compare_control_source.rs::test_compare_function_with_refine`, `constant_test_go_tables_source.rs::test_constant_propagation`, `constant_test_go_tables_source.rs::test_constant_propagation_for_outer_join` ... |
| `hex` | `builtin_string_time_source.rs`, `compare_time_builtin_rows_source.rs` | `builtin_string_time_source.rs::test_ci_weight_string_table`, `builtin_string_time_source.rs::test_translate_tables`, `builtin_string_time_source.rs::test_weight_string_forms` ... |
| `hour` | `builtin_string_time_source.rs`, `evaluator_go_tables_source.rs` | `builtin_string_time_source.rs::test_clock_parts_and_invalid_time_warning`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `if` | `compare_control_source.rs` | `compare_control_source.rs::test_if_typed_conditions` |
| `ifnull` | `compare_control_source.rs`, `expression_with_null_source.rs` | `compare_control_source.rs::test_ifnull_typed_pairs`, `expression_with_null_source.rs::evaluate_expr_with_null_meets_rebuild_error`, `expression_with_null_source.rs::evaluate_expr_with_null_neutralizes_only_schema_columns` ... |
| `in` | `constant_test_go_tables_source.rs`, `in_func_decimal_collation_source.rs` | `constant_test_go_tables_source.rs::test_constant_propagation`, `in_func_decimal_collation_source.rs::generated_in_harness_int_string_decimal_arms_agree_across_tiers`, `in_func_decimal_collation_source.rs::generated_in_harness_temporal_duration_json_arms` ... |
| `inet6_aton` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `inet_aton` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `inet_ntoa` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `instr` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::locate_with_position_matches_go_three_args_signature`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func` |
| `intdiv` | `aggregation_arithmetic_cast_source.rs` | `aggregation_arithmetic_cast_source.rs::test_vectorized_builtin_arithmetic_func` |
| `interval` | `compare_control_source.rs`, `compare_time_builtin_rows_source.rs` | `compare_control_source.rs::test_interval_func`, `compare_time_builtin_rows_source.rs::test_compare_builtin_interval_rows` |
| `is_ipv4` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `is_ipv4_mapped` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `is_ipv6` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `isfalse` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::is_true_or_false_full_signature_table`, `builtin_math_misc_op_source.rs::vectorized_builtin_op_func` |
| `isnull` | `builtin_math_misc_op_source.rs`, `builtin_string_time_source.rs`, `constant_test_go_tables_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_op_func`, `builtin_string_time_source.rs::test_is_null_func`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2` ... |
| `istrue` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::is_true_or_false_full_signature_table`, `builtin_math_misc_op_source.rs::vectorized_builtin_op_func` |
| `json_array` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::bench_mark`, `builtin_info_json_math_source.rs::json_array` |
| `json_contains` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains` |
| `json_depth` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_depth` |
| `json_extract` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_extract` |
| `json_insert` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_set_insert_replace` |
| `json_keys` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_keys`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `json_length` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_length`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `json_member_of` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_member_of` |
| `json_merge_patch` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_merge_patch`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `json_merge_preserve` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_merge_preserve` |
| `json_object` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_object` |
| `json_quote` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_quote` |
| `json_remove` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_remove` |
| `json_replace` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_set_insert_replace` |
| `json_set` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_set_insert_replace` |
| `json_type` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::json_type` ... |
| `json_unquote` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_unquote` |
| `json_valid` | `builtin_info_json_math_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_valid` |
| `le` | `compare_control_source.rs`, `scalar_function_semantics_source.rs` | `compare_control_source.rs::test_compare_function_with_refine`, `scalar_function_semantics_source.rs::test_expression_semantic_equal` |
| `least` | `compare_control_source.rs`, `compare_time_builtin_rows_source.rs` | `compare_control_source.rs::test_greatest_least_func`, `compare_time_builtin_rows_source.rs::test_compare_builtin_greatest_least_literal_rows` |
| `leftshift` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::shift_parameter_count_boundaries` |
| `length` | `constant_test_go_tables_source.rs` | `constant_test_go_tables_source.rs::constant_folding_sees_through_internal_charset_transcodes` |
| `locate` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::locate_with_position_matches_go_three_args_signature`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func` |
| `log` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::log` ... |
| `log10` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::log10` ... |
| `log2` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::log2` ... |
| `lt` | `compare_control_source.rs`, `constant_test_go_tables_source.rs`, `expression_with_null_source.rs`, `filter_extract_dnf_source.rs`, `scalar_function_semantics_source.rs` | `compare_control_source.rs::ast_rewrite_refines_integer_constant_before_comparison_casts`, `compare_control_source.rs::test_compare`, `compare_control_source.rs::test_compare_function_with_refine` ... |
| `makedate` | `compare_time_builtin_rows_source.rs` | `compare_time_builtin_rows_source.rs::test_time_builtin_date_year_makedate_literal_rows` |
| `maketime` | `builtin_time_calendars_source.rs` | `builtin_time_calendars_source.rs::maketime_integer_second_master_rows_overflow_garbage_and_null_arguments` |
| `md5` | `crypto_encryption_source.rs` | `crypto_encryption_source.rs::encoding_error_rows_follow_session_charset_conversion`, `crypto_encryption_source.rs::test_md5_hash` |
| `microsecond` | `builtin_string_time_source.rs`, `evaluator_go_tables_source.rs` | `builtin_string_time_source.rs::test_clock_parts_and_invalid_time_warning`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `minus` | `aggregation_arithmetic_cast_source.rs` | `aggregation_arithmetic_cast_source.rs::test_decimal_err_overflow`, `aggregation_arithmetic_cast_source.rs::test_vectorized_builtin_arithmetic_func` |
| `minute` | `builtin_string_time_source.rs`, `evaluator_go_tables_source.rs` | `builtin_string_time_source.rs::test_clock_parts_and_invalid_time_warning`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `mod` | `aggregation_arithmetic_cast_source.rs`, `evaluator_go_tables_source.rs` | `aggregation_arithmetic_cast_source.rs::test_vectorized_builtin_arithmetic_func`, `evaluator_go_tables_source.rs::mod_source_rows` |
| `month` | `builtin_vectorized_time_infra_source.rs`, `evaluator_go_tables_source.rs` | `builtin_vectorized_time_infra_source.rs::vec_month_zero_dates_stay_warning_free_in_both_flag_modes`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `mul` | `aggregation_arithmetic_cast_source.rs`, `scalar_function_semantics_source.rs` | `aggregation_arithmetic_cast_source.rs::test_arithmetic_overflow_error_message_with_column_name`, `aggregation_arithmetic_cast_source.rs::test_decimal_err_overflow`, `aggregation_arithmetic_cast_source.rs::test_real_arithmetic_overflow_error_message` ... |
| `ne` | `compare_control_source.rs`, `constant_test_go_tables_source.rs`, `scalar_function_semantics_source.rs` | `compare_control_source.rs::test_compare_function_with_refine`, `constant_test_go_tables_source.rs::test_constant_propagation`, `scalar_function_semantics_source.rs::test_issue_23309` |
| `not` | `builtin_math_misc_op_source.rs`, `constant_test_go_tables_source.rs`, `evaluator_go_tables_source.rs`, `scalar_function_semantics_source.rs` | `builtin_math_misc_op_source.rs::unary_not_every_input_domain`, `builtin_math_misc_op_source.rs::vectorized_builtin_op_func`, `constant_test_go_tables_source.rs::constant_folding_isnull_and_unary_not_reduce` ... |
| `nulleq` | `compare_control_source.rs` | `compare_control_source.rs::test_compare_function_with_refine` |
| `oct` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_oct`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func_2` |
| `or` | `builtin_math_misc_op_source.rs`, `constant_test_go_tables_source.rs`, `filter_extract_dnf_source.rs`, `scalar_function_semantics_source.rs`, `util_filter_condition_source.rs` | `builtin_math_misc_op_source.rs::logic_or_source_table`, `constant_test_go_tables_source.rs::test_constant_propagation`, `filter_extract_dnf_source.rs::test_filter_extract_from_dnf_case_table` ... |
| `ord` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_ord_charset_table` |
| `period_add` | `builtin_time_calendars_source.rs` | `builtin_time_calendars_source.rs::period_invalid_period_reject_the_call` |
| `period_diff` | `builtin_time_calendars_source.rs` | `builtin_time_calendars_source.rs::period_invalid_period_reject_the_call` |
| `pi` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::pi_is_the_exact_f64_constant`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `plus` | `aggregation_arithmetic_cast_source.rs`, `builtin_vectorized_time_infra_source.rs`, `constant_test_go_tables_source.rs`, `expression_null_const_source.rs`, `function_traits_source.rs`, `scalar_function_semantics_source.rs` | `aggregation_arithmetic_cast_source.rs::test_arithmetic_overflow_error_message_with_column_name`, `aggregation_arithmetic_cast_source.rs::test_arithmetic_plus`, `aggregation_arithmetic_cast_source.rs::test_decimal_err_overflow` ... |
| `pow` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::pow` ... |
| `quarter` | `evaluator_go_tables_source.rs` | `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `quote` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_quote`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func_2` |
| `radians` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `regexp_instr` | `regexp_vec_cache_source.rs` | `regexp_vec_cache_source.rs::regexp_instr_vec_generator_matrix_agrees_across_tiers` |
| `regexp_like` | `builtin_info_json_math_source.rs`, `regexp_vec_cache_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `regexp_vec_cache_source.rs::regexp_constant_pattern_corpus_matches_scalar_evaluation` ... |
| `regexp_replace` | `regexp_vec_cache_source.rs` | `regexp_vec_cache_source.rs::regexp_replace_vec_generator_matrix_agrees_across_tiers` |
| `regexp_substr` | `regexp_vec_cache_source.rs` | `regexp_vec_cache_source.rs::regexp_substr_vec_generator_matrix_agrees_across_tiers` |
| `right` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_string_right` |
| `rightshift` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::shift_parameter_count_boundaries` |
| `round` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::round` ... |
| `second` | `builtin_string_time_source.rs`, `evaluator_go_tables_source.rs` | `builtin_string_time_source.rs::test_clock_parts_and_invalid_time_warning`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `sha` | `crypto_encryption_source.rs` | `crypto_encryption_source.rs::test_sha1_hash` |
| `sha2` | `crypto_encryption_source.rs` | `crypto_encryption_source.rs::test_sha2_hash` |
| `sign` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_func` |
| `sin` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `sqrt` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `substring_index` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func` |
| `subtime` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_add_sub_time_issue_56861_typed_tables`, `builtin_string_time_source.rs::test_sub_time_duration_operand_tables`, `builtin_string_time_source.rs::test_sub_time_sig_value_tables` |
| `tan` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::math_string_coercion_raises_one_truncate_warning_each`, `builtin_math_misc_op_source.rs::vectorized_builtin_math_eval_one_vec` |
| `timediff` | `builtin_vectorized_time_infra_source.rs` | `builtin_vectorized_time_infra_source.rs::vectorized_time_harness_representative_cases_match_scalar_answers` |
| `timestampdiff` | `builtin_time_calendars_source.rs` | `builtin_time_calendars_source.rs::timestamp_diff_flag_block_rows_stay_null` |
| `to_binary` | `aggregation_arithmetic_cast_source.rs`, `constant_test_go_tables_source.rs` | `aggregation_arithmetic_cast_source.rs::test_wrap_with_cast_as_string`, `constant_test_go_tables_source.rs::constant_folding_sees_through_internal_charset_transcodes` |
| `truncate` | `builtin_info_json_math_source.rs`, `builtin_math_misc_op_source.rs` | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::truncate` ... |
| `unaryminus` | `builtin_math_misc_op_source.rs`, `compare_control_source.rs` | `builtin_math_misc_op_source.rs::unary_minus_ret_type_flen_follows_go_sign_reservation`, `builtin_math_misc_op_source.rs::unary_minus_source_table`, `compare_control_source.rs::test_compare_function_with_refine` |
| `uncompress` | `crypto_encryption_source.rs` | `crypto_encryption_source.rs::test_compress_and_uncompress_length_framing`, `crypto_encryption_source.rs::test_uncompress`, `crypto_encryption_source.rs::uncompress_rejects_handcrafted_payload_larger_than_declared_length` ... |
| `uncompressed_length` | `crypto_encryption_source.rs` | `crypto_encryption_source.rs::test_uncompress_length` |
| `unhex` | `builtin_string_time_source.rs` | `builtin_string_time_source.rs::test_translate_tables`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func` |
| `unix_timestamp` | `builtin_time_calendars_source.rs`, `expression_null_const_source.rs` | `builtin_time_calendars_source.rs::unix_timestamp_compact_numeric_and_zero_date_rows_match_master`, `builtin_time_calendars_source.rs::unix_timestamp_value_table_under_utc`, `expression_null_const_source.rs::test_const_level_case_table` |
| `vec_as_text` | `builtin_vectorized_time_infra_source.rs` | `builtin_vectorized_time_infra_source.rs::vectorized_builtin_vec_families_match_master_shapes` |
| `vec_dims` | `builtin_vectorized_time_infra_source.rs` | `builtin_vectorized_time_infra_source.rs::vectorized_builtin_vec_families_match_master_shapes` |
| `vec_l2_norm` | `builtin_vectorized_time_infra_source.rs` | `builtin_vectorized_time_infra_source.rs::vectorized_builtin_vec_families_match_master_shapes` |
| `week` | `evaluator_go_tables_source.rs` | `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `xor` | `builtin_math_misc_op_source.rs` | `builtin_math_misc_op_source.rs::logic_xor_source_table` |
| `year` | `compare_time_builtin_rows_source.rs`, `evaluator_go_tables_source.rs` | `compare_time_builtin_rows_source.rs::test_time_builtin_date_year_makedate_literal_rows`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `yearweek` | `builtin_time_calendars_source.rs` | `builtin_time_calendars_source.rs::yearweek_source_rows_pin_zero_month_null_and_boundary_years` |

### 2.2 Admitted names covered only by `tests/tikv_coverage.rs` (89)

These have no source-port test; the differential fixture already exercises them
with explicit `Expression` trees, so re-pointing cannot add a first observer.

- `atan2` `bit_length` `case` `char_length`
- `character_length` `date_add_day` `date_add_day_hour` `date_add_day_microsecond`
- `date_add_day_minute` `date_add_day_second` `date_add_hour` `date_add_hour_microsecond`
- `date_add_hour_minute` `date_add_hour_second` `date_add_microsecond` `date_add_minute`
- `date_add_minute_microsecond` `date_add_minute_second` `date_add_month` `date_add_quarter`
- `date_add_second` `date_add_second_microsecond` `date_add_week` `date_add_year`
- `date_add_year_month` `date_sub_day` `date_sub_day_hour` `date_sub_day_microsecond`
- `date_sub_day_minute` `date_sub_day_second` `date_sub_hour` `date_sub_hour_microsecond`
- `date_sub_hour_minute` `date_sub_hour_second` `date_sub_microsecond` `date_sub_minute`
- `date_sub_minute_microsecond` `date_sub_minute_second` `date_sub_month` `date_sub_quarter`
- `date_sub_second` `date_sub_second_microsecond` `date_sub_week` `date_sub_year`
- `date_sub_year_month` `datediff` `dayname` `dayofweek`
- `dayofyear` `inet6_ntoa` `is_ipv4_compat` `isfalse_with_null`
- `istrue_with_null` `json_memberof` `last_day` `lcase`
- `left` `like` `ln` `lower`
- `ltrim` `mid` `monthname` `octet_length`
- `position` `power` `regexp` `replace`
- `reverse` `rlike` `rtrim` `sha1`
- `str_to_date` `strcmp` `substr` `substring`
- `time_to_sec` `to_days` `to_seconds` `trim`
- `ucase` `upper` `vec_cosine_distance` `vec_l1_distance`
- `vec_l2_distance` `vec_negative_inner_product` `weekday` `weekofyear` `casewhen`

### 2.3 Admitted names covered only elsewhere in the crate (0)

None. `casewhen` is the rewriter spelling of the `case` subject, which the fixture
covers; it is counted in 2.2 through the `casewhen`~`case` alias.

### 2.4 Admitted names with no coverage anywhere in `tidb-expr` (0)

None. Every admitted name is at least name-reachable from some crate test or
the differential fixture, though 2.2/2.3 names have no Go-source-port oracle.

---

## 3. Excluded names and the "engine refuses" list

103 excluded names appear as the subject of at least one
source-port test; these tests become "engine refuses, native answers" cases
during coexistence and "engine refuses" after deletion. Names are ordered by
number of source-port tests.

| excluded name | exclusion reason (abridged) | source-port tests |
| --- | --- | --- |
| `rand` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::rand` ... |
| `time` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_string_time_source.rs::test_clock_parts_and_invalid_time_warning` ... |
| `timestamp` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::timestamp_compact_string_rows_match_master`, `builtin_time_calendars_source.rs::timestamp_delimited_argument_rows_match_master`, `builtin_time_calendars_source.rs::timestamp_float_rows_match_master` ... |
| `cast_char` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_string`, `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_string_truncates_at_flen`, `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_null_and_hybrid` ... |
| `format` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_string_time_source.rs::test_format_precision_side_truncate_warning_counts`, `builtin_string_time_source.rs::test_format_values_and_number_side_truncate_warnings`, `builtin_string_time_source.rs::test_format_with_locale` ... |
| `found_rows` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_current_resource_group`, `ilike_info_cast_source.rs::test_current_user`, `ilike_info_cast_source.rs::test_database` ... |
| `setvar` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_vectorized_time_infra_source.rs::vectorized_check_predicates_over_constants_columns_and_correlated`, `setvar_getvar_values_getparam_source.rs::setvar_from_column_snapshots_the_row_value`, `setvar_getvar_values_getparam_source.rs::setvar_stores_session_value_and_returns_it` ... |
| `to_base64` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `builtin_string_time_source.rs::test_to_base64`, `builtin_string_time_source.rs::test_to_base64_gbk_session_rows`, `builtin_string_time_source.rs::test_to_base64_sig_packet_boundaries` ... |
| `cast_signed` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_int`, `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_null_and_hybrid`, `aggregation_arithmetic_cast_source.rs::test_cast_functions_string_to_unsigned_and_signed` ... |
| `from_base64` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `builtin_string_time_source.rs::test_from_base64`, `builtin_string_time_source.rs::test_from_base64_sig`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2` ... |
| `insert_func` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `builtin_string_time_source.rs::test_insert_binary_sig`, `builtin_string_time_source.rs::test_insert_func_table`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec` ... |
| `json_pretty` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_pretty`, `builtin_info_json_math_source.rs::json_storage_free` ... |
| `json_schema_valid` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_schema_valid`, `builtin_info_json_math_source.rs::json_schema_valid_cache` ... |
| `get_lock` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `advisory_get_lock_integration_source.rs::test_get_lock_call_semantics_table`, `advisory_get_lock_integration_source.rs::test_get_lock_rejects_bad_names_with_3057`, `builtin_string_time_source.rs::test_lock` |
| `getparam` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `expression_null_const_source.rs::test_const_level_case_table`, `function_traits_source.rs::unfoldable_functions_contains_sysdate`, `setvar_getvar_values_getparam_source.rs::getparam_function_evaluation_matches_plan_cache_values` |
| `json_storage_size` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free`, `builtin_info_json_math_source.rs::json_storage_size` |
| `lpad` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `builtin_string_time_source.rs::test_lpad`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func` |
| `make_set` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `builtin_string_time_source.rs::test_make_set`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec_2`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func_2` |
| `now` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_string_time_source.rs::test_now_utc_timestamp_fixed_clock`, `helper_current_timestamp_source.rs::test_current_timestamp_time_zone_case_table`, `helper_current_timestamp_source.rs::test_timestamp_sysvar_renders_fixed_now_literal_rows` |
| `release_lock` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `advisory_get_lock_integration_source.rs::test_get_lock_call_semantics_table`, `advisory_get_lock_integration_source.rs::test_get_lock_rejects_bad_names_with_3057`, `builtin_string_time_source.rs::test_lock` |
| `translate` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_string_time_source.rs::test_translate_tables`, `builtin_string_time_source.rs::test_vectorized_builtin_string_eval_one_vec`, `builtin_string_time_source.rs::test_vectorized_builtin_string_func` |
| `uuid` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_math_misc_op_source.rs::uuid_generation_v1_v4_v7_shapes`, `expression_null_const_source.rs::test_const_level_case_table`, `function_traits_source.rs::unfoldable_functions_contains_sysdate` |
| `weight_string` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_string_time_source.rs::test_ci_weight_string_table`, `builtin_string_time_source.rs::test_weight_string_binary_cut_warning`, `builtin_string_time_source.rs::test_weight_string_forms` |
| `aes_decrypt` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `crypto_encryption_source.rs::test_aes_decrypt`, `crypto_encryption_source.rs::test_aes_encrypt` |
| `benchmark` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_info_json_math_source.rs::bench_mark`, `builtin_info_json_math_source.rs::vectorized_builtin_info_func` |
| `cast_json` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_binary_string_as_json_sig`, `compare_control_source.rs::test_compare` |
| `cast_unsigned` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_functions_neg_int_as_unsigned_warns_8031`, `aggregation_arithmetic_cast_source.rs::test_cast_functions_string_to_unsigned_and_signed` |
| `concat` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `constant_test_go_tables_source.rs::constant_folding_charset_binary_result_matches_source`, `constant_test_go_tables_source.rs::constant_folding_sees_through_internal_charset_transcodes` |
| `connection_id` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::connection_id`, `builtin_info_json_math_source.rs::vectorized_builtin_info_func` |
| `convert` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `convert_using_signature_source.rs::convert_using_result_type_carries_target_charset_metadata`, `convert_using_signature_source.rs::convert_using_unknown_charset_fails_before_evaluation` |
| `curdate` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_string_time_source.rs::test_current_date_current_time_utc_time_clocks`, `builtin_time_calendars_source.rs::with_time_zone_clock_builtins_render_the_session_zone` |
| `current_resource_group` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_current_resource_group`, `ilike_info_cast_source.rs::test_current_resource_group_ast_path` |
| `current_role` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::current_role`, `builtin_info_json_math_source.rs::vectorized_builtin_info_func` |
| `current_time` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_string_time_source.rs::test_current_date_current_time_utc_time_clocks`, `builtin_time_calendars_source.rs::with_time_zone_clock_builtins_render_the_session_zone` |
| `current_timestamp` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `helper_current_timestamp_source.rs::test_get_time_value_build_context_helper`, `helper_current_timestamp_source.rs::test_is_current_timestamp_expr_predicate` |
| `date_add` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::date_arith_day_month_year_overflow_tables_match_master`, `builtin_vectorized_time_infra_source.rs::vectorized_generated_time_unit_families_match_the_row_answers` |
| `date_sub` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::date_arith_day_month_year_overflow_tables_match_master`, `builtin_vectorized_time_infra_source.rs::vectorized_generated_time_unit_families_match_the_row_answers` |
| `extract` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `evaluator_go_tables_source.rs::extract_composite_units_over_fractional_strings_match_source`, `evaluator_go_tables_source.rs::extract_master_unit_table_matches_source` |
| `getvar` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `function_traits_source.rs::unfoldable_functions_contains_sysdate`, `vectorizable_and_chunk_eval_source.rs::test_vectorizable_over_go_expression_table` |
| `grouping` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `scalar_function_semantics_source.rs::grouping_construction_requires_metadata`, `scalar_function_semantics_source.rs::test_column_substitute_grouping_cleans_hash_code` |
| `json_array_append` | appending an array value through a nested path flattens it instead of appending the arr... | `builtin_info_json_math_source.rs::json_array_append`, `builtin_info_json_math_source.rs::json_contains_path` |
| `json_array_insert` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_array_insert`, `builtin_info_json_math_source.rs::json_contains_path` |
| `json_search` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_search` |
| `json_storage_free` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path`, `builtin_info_json_math_source.rs::json_storage_free` |
| `last_insert_id` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_info_json_math_source.rs::last_insert_id`, `builtin_info_json_math_source.rs::vectorized_builtin_info_func` |
| `password` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `crypto_encryption_source.rs::encoding_error_rows_follow_session_charset_conversion`, `crypto_encryption_source.rs::test_password` |
| `row` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `advisory_get_lock_integration_source.rs::test_get_lock_call_semantics_table`, `compare_time_builtin_rows_source.rs::test_compare_builtin_row_constructor_rows` |
| `row_count` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::row_count`, `builtin_info_json_math_source.rs::vectorized_builtin_info_func` |
| `rpad` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `builtin_string_time_source.rs::test_rpad`, `builtin_string_time_source.rs::test_rpad_sig` |
| `sleep` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_math_misc_op_source.rs::sleep_vectorized_incorrect_argument_levels`, `evaluator_go_tables_source.rs::sleep_errctx_levels_and_null_arguments_follow_the_caller` |
| `sysdate` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_time_calendars_source.rs::with_time_zone_clock_builtins_render_the_session_zone`, `function_traits_source.rs::unfoldable_functions_contains_sysdate` |
| `tidb_version` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::tidb_version`, `builtin_info_json_math_source.rs::vectorized_builtin_info_func` |
| `time_format` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::time_format_hour_family_rows_match_master`, `builtin_vectorized_time_infra_source.rs::vectorized_time_format_empty_format_returns_null` |
| `timestampadd` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::timestamp_add_delimited_rows_match_master`, `builtin_time_calendars_source.rs::timestamp_add_numeric_date_arguments_match_master` |
| `uuid_to_bin` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec`, `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_func` |
| `version` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::vectorized_builtin_info_func`, `builtin_info_json_math_source.rs::version` |
| `aes_encrypt` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `crypto_encryption_source.rs::test_aes_encrypt` |
| `bin_to_uuid` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_func` |
| `cast_binary` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_functions_char_and_binary` |
| `cast_date` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_time` |
| `cast_datetime` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_time` |
| `cast_time` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `aggregation_arithmetic_cast_source.rs::test_cast_func_sig_as_duration` |
| `charset` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::charset` |
| `coercibility` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::coercibility` |
| `collation` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::collation` |
| `concat_ws` | native enforces max_allowed_packet before allocating; the pinned engine facade has no e... | `constant_test_go_tables_source.rs::null_reject_conditions_survive_both_fold_modes` |
| `current_user` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_current_user` |
| `curtime` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_time_calendars_source.rs::with_time_zone_clock_builtins_render_the_session_zone` |
| `database` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_database` |
| `decode` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `crypto_encryption_source.rs::test_sql_decode` |
| `encode` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `crypto_encryption_source.rs::test_sql_encode` |
| `format_bytes` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::format_bytes` |
| `format_nano_time` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::format_nano_time` |
| `getvar_string` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `function_traits_source.rs::unfoldable_functions_contains_sysdate` |
| `ilike` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_ilike` |
| `is_uuid` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `json_contains_path` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_contains_path` |
| `json_merge` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_merge` |
| `json_overlaps` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_info_json_math_source.rs::json_overlaps` |
| `lastval` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `vectorizable_and_chunk_eval_source.rs::test_vectorizable_over_go_expression_table` |
| `load_file` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_string_time_source.rs::test_load_file` |
| `name_const` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `nextval` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `vectorizable_and_chunk_eval_source.rs::test_vectorizable_over_go_expression_table` |
| `nullif` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `compare_time_builtin_rows_source.rs::test_compare_builtin_nullif_rows` |
| `random_bytes` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `crypto_encryption_source.rs::test_random_bytes` |
| `release_all_locks` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `advisory_get_lock_integration_source.rs::test_get_lock_call_semantics_table` |
| `schema` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_database` |
| `sec_to_time` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_vectorized_time_infra_source.rs::vectorized_time_harness_representative_cases_match_scalar_answers` |
| `setval` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `vectorizable_and_chunk_eval_source.rs::test_vectorizable_over_go_expression_table` |
| `tidb_bounded_staleness` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::tidb_bounded_staleness_safets_windows_and_monotonicity` |
| `tidb_current_tso` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::current_tso_reports_session_transaction_tso` |
| `tidb_parse_tso` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::tidb_parse_tso_master_vectors_under_utc` |
| `tidb_parse_tso_logical` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::tidb_parse_tso_logical_consecutive_tso_counters` |
| `user` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `ilike_info_cast_source.rs::test_user` |
| `utc_date` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_time_calendars_source.rs::utc_date_answers_the_utc_statement_date` |
| `utc_time` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_string_time_source.rs::test_current_date_current_time_utc_time_clocks` |
| `utc_timestamp` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `builtin_string_time_source.rs::test_now_utc_timestamp_fixed_clock` |
| `uuid_timestamp` | TiKV UUID_VERSION/UUID_TIMESTAMP parse malformed input leniently where Go raises error ... | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `uuid_v4` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_math_misc_op_source.rs::uuid_generation_v1_v4_v7_shapes` |
| `uuid_v7` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `builtin_math_misc_op_source.rs::uuid_generation_v1_v4_v7_shapes` |
| `uuid_version` | TiKV UUID_VERSION/UUID_TIMESTAMP parse malformed input leniently where Go raises error ... | `builtin_math_misc_op_source.rs::vectorized_builtin_miscellaneous_eval_one_vec` |
| `validate_password_strength` | no local engine lowering: not selected by local_call, the temporal/JSON/vector families... | `crypto_encryption_source.rs::test_validate_password_strength` |
| `values` | session state, statement clock, RNG, user variables, sequences, or effects are absent f... | `setvar_getvar_values_getparam_source.rs::values_function_reads_the_current_insert_row` |

Note: a test may have both admitted and excluded subjects (e.g. `cast` plus
`cast_json`), so it appears in both section 1 admitted and excluded columns and
in this table. The `nested_integration_source` files (advisory locks, current
timestamp, setvar/getvar, vectorized filter) are the effectful/session-dependent
families that the checklist section 6 item 7 expects to stay refusals.

---

## 4. Ranking by how mechanical re-pointing is

Rank (a): the test already produces a `ScalarFunction` or SQL text, so a shared
differential helper converts it; per-case work is changing the helper call.
Rank (b): the test calls a native kernel on `Datum`s, so an `Expression` tree
must be written for each case before the engine can see it.
Rank (c): the file has no evaluator path (PB lowering, hash codec, rewriter DNF,
context override, traits) or is entirely ignored gap stubs; it is not a
re-pointing target. Category (c) contains two sub-kinds and they must not be
conflated: **(c-structure)** exercises code that survives deletion and should be
kept unchanged; **(c-delete)** asserts native evaluator internals and should be
deleted with the kernel. The only genuine (c-delete) file is
`vectorized_filter_consider_null_gap_source.rs` (VecEvalBool/rowBasedFilter
selection machinery); the other eight are (c-structure).

| rank | file | tests | a | b | c | gap | why |
| --- | --- | --- | --- | --- | --- | --- | --- |
| (a) | `builtin_string_time_source.rs` | 57 | 43 | 5 | 1 | 8 | SQL text / Expression tree already |
| (a) | `aggregation_arithmetic_cast_source.rs` | 51 | 41 | 0 | 2 | 8 | SQL text / Expression tree already |
| (a) | `builtin_math_misc_op_source.rs` | 34 | 21 | 1 | 6 | 6 | SQL text / Expression tree already |
| (a) | `builtin_time_calendars_source.rs` | 22 | 11 | 0 | 11 | 0 | SQL text / Expression tree already |
| (a) | `constant_test_go_tables_source.rs` | 14 | 10 | 0 | 0 | 4 | SQL text / Expression tree already |
| (a) | `compare_control_source.rs` | 17 | 8 | 0 | 5 | 4 | SQL text / Expression tree already |
| (a) | `ilike_info_cast_source.rs` | 14 | 8 | 0 | 0 | 6 | SQL text / Expression tree already |
| (a) | `setvar_getvar_values_getparam_source.rs` | 8 | 8 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `compare_time_builtin_rows_source.rs` | 7 | 7 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `scalar_function_semantics_source.rs` | 9 | 7 | 0 | 0 | 2 | SQL text / Expression tree already |
| (a) | `expression_null_const_source.rs` | 6 | 6 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `regexp_vec_cache_source.rs` | 7 | 5 | 0 | 0 | 2 | SQL text / Expression tree already |
| (a) | `evaluator_go_tables_source.rs` | 9 | 4 | 0 | 3 | 2 | SQL text / Expression tree already |
| (a) | `expression_with_null_source.rs` | 6 | 4 | 0 | 1 | 1 | SQL text / Expression tree already |
| (a) | `helper_current_timestamp_source.rs` | 4 | 4 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `in_func_decimal_collation_source.rs` | 4 | 4 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `builtin_vectorized_time_infra_source.rs` | 14 | 4 | 2 | 2 | 6 | SQL text / Expression tree already |
| (a) | `find_in_set_lookup_source.rs` | 4 | 3 | 0 | 0 | 1 | SQL text / Expression tree already |
| (a) | `advisory_get_lock_integration_source.rs` | 2 | 2 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `convert_using_signature_source.rs` | 3 | 2 | 0 | 0 | 1 | SQL text / Expression tree already |
| (a) | `json_merge_patch_integration_source.rs` | 2 | 2 | 0 | 0 | 0 | SQL text / Expression tree already |
| (a) | `vectorizable_and_chunk_eval_source.rs` | 4 | 2 | 0 | 1 | 1 | SQL text / Expression tree already |
| (c) | `filter_extract_dnf_source.rs` | 1 | 1 | 0 | 0 | 0 | rewriter DNF structure; survives deletion, keep unchanged |
| (c) | `util_filter_condition_source.rs` | 1 | 1 | 0 | 0 | 0 | rewriter predicate structure; survives deletion, keep unchanged |
| (b) | `builtin_info_json_math_source.rs` | 60 | 18 | 28 | 11 | 3 | native helper on Datum per case |
| (b) | `crypto_encryption_source.rs` | 19 | 1 | 15 | 0 | 3 | native helper on Datum per case |
| (c) | `context_override_values_source.rs` | 1 | 0 | 0 | 1 | 0 | structure or gap stub; no evaluator path |
| (c) | `distsql_pb_roundtrip_gap_source.rs` | 4 | 0 | 0 | 0 | 4 | structure or gap stub; no evaluator path |
| (c) | `expr_to_pb_lowering_gap_source.rs` | 20 | 0 | 0 | 0 | 20 | structure or gap stub; no evaluator path |
| (c) | `expr_to_pb_switcher_source.rs` | 3 | 0 | 0 | 0 | 3 | structure or gap stub; no evaluator path |
| (c) | `function_traits_source.rs` | 2 | 0 | 0 | 1 | 1 | structure or gap stub; no evaluator path |
| (c) | `hash_group_key_codec_matrix_source.rs` | 1 | 0 | 0 | 1 | 0 | structure or gap stub; no evaluator path |
| (c) | `vectorized_filter_consider_null_gap_source.rs` | 3 | 0 | 0 | 0 | 3 | structure or gap stub; no evaluator path |

---

## 5. Recommended conversion order

Order is driven by the family order in `tikv-expression-removal-checklist.md`
section 6 (arithmetic/comparison/bit/IN first) filtered by mechanical-ness, and
by using the first files to build the shared differential helper. Batch 1 is the
recommended start; the first three are named with a representative conversion.

**Batch 1 (build the helper):**
1. `in_func_decimal_collation_source.rs` (4 tests, rank a) - IN family, first in
   the family order, pure `Expression`/`chunk_e`; establishes the
   parse->rewrite->`Expression` -> engine path.
2. `find_in_set_lookup_source.rs` (4 tests, rank a) - string lookup, table already
   iterates evaluator closures, so adding the engine tier is a loop edit.
3. `builtin_math_misc_op_source.rs` (34 tests, rank a) - math/bit/control, 21
   tests are plain `e()` SQL text; the largest early coverage win.

**Batch 2 (small pure files):** `expression_null_const_source.rs` (6),
`compare_time_builtin_rows_source.rs` (7), `regexp_vec_cache_source.rs` (7),
`scalar_function_semantics_source.rs` (9), `expression_with_null_source.rs` (6),
`convert_using_signature_source.rs` (3), `json_merge_patch_integration_source.rs`
(2), and the two refusal carriers `advisory_get_lock_integration_source.rs` (2)
and `setvar_getvar_values_getparam_source.rs` (8).

**Batch 3 (large mixed):** `constant_test_go_tables_source.rs` (14),
`compare_control_source.rs` (17), `builtin_string_time_source.rs` (57),
`aggregation_arithmetic_cast_source.rs` (51), `builtin_time_calendars_source.rs`
(22), `evaluator_go_tables_source.rs` (9), `builtin_vectorized_time_infra_source.rs`
(14), `ilike_info_cast_source.rs` (14), `vectorizable_and_chunk_eval_source.rs` (4),
`helper_current_timestamp_source.rs` (4).

**Batch 4 (rank b, needs trees written):** `crypto_encryption_source.rs` (19),
`builtin_info_json_math_source.rs` (60, JSON family last per the family order).

**Batch 5 (rank c):** keep `distsql_pb_roundtrip_gap_source.rs`,
`expr_to_pb_lowering_gap_source.rs`, `expr_to_pb_switcher_source.rs`,
`hash_group_key_codec_matrix_source.rs`, `function_traits_source.rs`,
`context_override_values_source.rs`, `filter_extract_dnf_source.rs`,
`util_filter_condition_source.rs` as structure regressions; delete
`vectorized_filter_consider_null_gap_source.rs` with the native vectorized
filter code; keep its three `#[ignore]` stubs only as removal receipts.

### 5.1 Representative conversion: `in_func_decimal_collation_source.rs::in_func_collation_row_general_ci_folds_case_and_accents`

Today the case is two `chunk_e` calls; `chunk_e` already parses, calls
`crate::rewriter::rewrite_expr` and runs `Expression::eval`. The conversion adds
a sibling that also compiles the same `Expression` for the engine. The helper
below is a sketch of the target shape, not a compiling patch:

```rust
// tests/mod.rs (shared helper; sketch, consolidates the chunk_e parse/rewrite path)
fn chunk_both(expr: &str) -> String {
    let expression = rewrite_sql(expr);            // existing chunk_e_with path
    let native = expression.eval(&NoColumns, row).unwrap().label();
    let ctx = TestEngineContext::with_backend(Backend::Copying);
    let mut out = Chunk::new_with_capacity(&[expression.ret_type()], 1);
    EvaluatorSuite::new(vec![expression.clone()], true)
        .run(&ctx, &mut input, &mut out).unwrap();
    assert!(ctx.fallbacks.borrow().is_empty(), "engine refused: {:?}", ctx.fallbacks.borrow());
    assert_eq!(out.get_row(0).get_datum(0, ty).label(), native);
    native
}
// unchanged assertions, one helper swapped
assert_eq!(chunk_both("'a' collate utf8mb4_general_ci in ('Á' collate utf8mb4_general_ci)"), "INT:1");
assert_eq!(chunk_both("'a' collate utf8mb4_bin in ('Á' collate utf8mb4_bin)"), "INT:0");
```

`in` is admitted (`Family::Control`, `Shape::Any` since `6d70e0d`), so the
fallback assertion is meaningful: if the collation shape is not admitted the
test fails loudly instead of silently staying native.

### 5.2 Representative conversion: `find_in_set_lookup_source.rs::find_in_set_const_strlist_pad_space_lookup_value_rows`

The test already loops `for tier_evaluator in [e, chunk_e]`. Add a third closure
that runs the engine on the same SQL text:

```rust
fn engine_e(expr: &str) -> String {
    let expression = rewrite_sql(expr);       // same path as chunk_e
    // one-row input, engine-only context (backend Some), no native fallback
    // returns the engine label; asserts FallbackReason is empty
}
for tier_evaluator in [e, chunk_e, engine_e] {
    assert_eq!(tier_evaluator(&general_ci("find_in_set(' ', '  , , ,')")), "INT:2");
}
```

`find_in_set` is admitted (`Family::String`). The file's fourth test,
`find_in_set_strlist_cache_lifecycle_gap`, is an ignored empty stub about the Go
strlist cache and stays a gap receipt (the cache is a native implementation
detail, rank c).

### 5.3 Representative conversion: `builtin_math_misc_op_source.rs::bit_count_source_table`

Today it is a 12-row table of `assert_eq!(e(sql), want.label())`. Replace the
evaluator inside the loop with the differential helper:

```rust
for (sql, want) in [ ("bit_count(8)", Datum::Int(1)), /* ... */ ] {
    assert_eq!(e(sql), want.label(), "{sql} (native)");
    assert_eq!(engine_e(sql), want.label(), "{sql} (engine)");
}   // or: assert_eq!(both_e(sql), want.label(), "{sql}");
```

`bit_count` is admitted (`Family::Miscellaneous`), so this is a straight
dual-run. The same edit pattern applies to every `e()`-only test in the file
(`conv_source_table_type_and_valid_prefix_rows` is `A+E` and takes the helper
too). The file's six `X` tests (`pi_is_the_exact_f64_constant`,
`uuid_generation_v1_v4_v7_shapes`, `shift_parameter_count_boundaries`,
`bit_xor_parameter_count_boundaries`, `vectorized_builtin_miscellaneous_func`,
`math_string_coercion_raises_one_truncate_warning_each`) assert metadata or
warning counts that the differential helper must compare rather than the value
label; they are converted with the warnings-aware variant of the helper.

---

## 6. Acceptance/receipt notes for the parent

- Section 5 of the removal checklist requires every admitted signature to have
  differential evidence; this plan supplies the mapping from the 413-test corpus
  to the 141 admitted names it can feed. The remaining 88 admitted names that
  only `tikv_coverage.rs` covers need the fixture extended or a new source port,
  not a re-pointing of an existing port.
- 140 of 413 source-port tests carry at least one excluded subject and are the
  input to the Milestone E refusal surface; 103 distinct excluded names are
  involved.
- Counts are static and name-based. They were not validated by building or
  running the crate (parent owns the build slot).

---

## 7. Engine-only mode: the measured Milestone E corpus gap

Section 1-6 count *static* subjects. They cannot say how many corpus cases the
engine actually answers, because the dual-run harness in
`rust/crates/tidb-expr/src/tests/mod.rs` deliberately skips an expression the
adapter declines. To turn that silence into a number, `chunk_e` gained one
switch:

    TIKV_EXPR_ENGINE_ONLY=1 cargo test -p tidb-expr --features tikv-expr --offline -j1 \
      -- --test-threads=1

With the variable set, `engine_case` returning `None` (the adapter declined)
panics with `engine declined the expression: <expr>` instead of falling through
to the native answer. This is the exact behaviour Milestone E needs after the
native evaluator is deleted, so the run doubles as the E readiness measurement.

Measured at branch `feat/tikv-expression-coverage`, HEAD `47ce598` plus the
harness switch, feature on, no native deletion:

| Result | Count |
| --- | --- |
| lib tests passed | 1148 |
| lib tests failed (engine declined) | 65 |
| lib tests ignored | 99 |
| distinct declined expressions | 63 |

So **59 distinct constant expressions of the current corpus have no engine
path**; 1148 of 1208 runnable cases already agree through the engine. This is
the concrete E blocker set, and it is much smaller than the 140-test excluded
surface in section 6, because most excluded names never reach a constant
expression in these tests (they are exercised on columns, where the adapter
declines by name but the harness never materialises the engine).

Three of the first run's expressions moved to engine-covered in the same
change: `date('20111213')`, `month(20240315123045)` and
`last_day(20240315123045)` over the implicit temporal cast. The first two were
declined by a blanket "no temporal cast over a constant" rule in `coerce()`,
which turned out to cover exactly one real divergence; the third was that
divergence -- TiKV's `last_day` is typed `DateTime` internally and returns a
midnight `DateTime` where the declared type is `DATE`, so the bridge now
rebuilds the declared `DATE` the way Go's DATE decoder drops the time part
(`bridge::check_time`). `convert_tz(20240315123045, ...)` appears in the list
in their place: it was declined all along, but the test that contains it used
to stop earlier. Four more moved out of the admission group when `extract`,
`time` and `timestamp` gained local lowerings (section 7.3).

### 7.1 Where each refusal happens

`FallbackReason::NotAdmitted` covers five different stages, so a silent
fallback hides which one a given expression is stuck at. `TIKV_EXPR_DEBUG_COMPILE=1`
prints the stage and, for the engine stage, the engine's own error. Recompiling
the current set with it:

| Stage | Count | What it means |
| --- | --- | --- |
| admission table | 32 | the name is not admitted, or the expression's result type is outside `supported_type` |
| local lowering | 20 | `local_call`/`families`/catalog lowering returned `None` |
| engine compile | 7 | the wire program was built and the engine refused it |
| evaluation | 2 | compiles, then the engine declines per batch |

The admission stage is a third of the whole gap, so it reports its own
sub-reason too (`TIKV-EXPR-ADMIT-REJECT [<reason>]`). For the 32:

| Admission sub-reason | Count |
| --- | --- |
| row excluded: no local lowering | 22 |
| constant is a binary/bit/literal datum | 3 |
| lazy-shape rule (a skipped arm is not a leaf) | 2 |
| row excluded: `max_allowed_packet` | 2 |
| row excluded: session state / clock / RNG | 1 |
| row excluded: native `COT` bug | 1 |
| row excluded: `OCT` over a bit literal | 1 |

That reframes the group: only **five** of the 32 are policy exclusions the
adapter chose deliberately (`COT`, `OCT`, `max_allowed_packet`, session state),
two are the lazy-arm value rule, three are the binary-literal representation
gap, and the 22 "row excluded: no local lowering" are names whose exclusion
reason is a placeholder -- they are not missing *rows*, they are rows that say
"nothing lowers this yet".

The split matters because the three engine-side groups need engine or
embedder work, while the admission group needs either a new lowering (the
`extract`/`time`/`timestamp`/`convert_tz` family) or is a deliberate exclusion
(`cot`, `oct`, `json`, `load_file`, `format`, `make_set`, `to_base64`, the
collation-sensitive shapes).

Lazy shapes with a `NULL` arm were the first to fall out of the lowering group
(`coalesce(NULL, 1)`, `NULLIF`, `CASE WHEN NULL THEN ...`, `0 OR NULL`): the
engine's argument validator reads each child's declared `FieldType`, and a
`NULL` constant was labelled `Bytes` (its SQL eval family), so `CoalesceInt`
rejected it with `Expect Int, received Bytes`. `lazy_args` now retags a `NULL`
leaf to the target family instead of inserting a cast, which keeps the arm a
leaf and adds no node. Three expressions moved to engine-covered; three
others that the same tests used to stop before (`1.50 or 0e0`,
`coalesce(1, 1.1e0)`, `case when cast('0' as json) then 1 end`) took their
place in the list, so the count stayed at 63.

The 59, verbatim:

    0xff like 0xff
    (1, 2) = (1, 2, 3)
    1.50 or 0e0
    7 in (7, -9, 9)
    addtime('01:00:00.999999','02:00:00.999998')
    addtime('2020-01-01 10:00:00','01:00:00')
    b'1111111111111111111111111111111111111111111111111111111111111111' + 0
    benchmark(-3, 1)
    case when cast('0' as json) then 1 end
    case when false then 1.5 else 0 end
    cast('"123"' as json) < cast('"123"' as json)
    cast('1' as json)
    cast('2019-11-02 22:00:05' as datetime) in (cast('2019-11-02 22:00:04' as datetime), cast('2019-11-02 22:00:05' as datetime))
    char(65, 16740, 67.5 using utf8)
    coalesce(1, 1.1e0)
    coalesce(1, 'x' regexp '[')
    coalesce(cast(1 as json), cast(2 as json))
    convert(0x1e240 using utf8)
    convert('haha' using cp866)
    convert_tz(20240315123045,'+00:00','+08:00')
    cot(1)
    elt(1.1, '2.1', '3.1', '11.1', '1.1')
    elt('2abc','x','y','z')
    extract(hour from '-25:03:04')
    field(1.10, 0, 11e-1)
    field(NULL, 2, 3, 11, 1)
    find_in_set('a', 'b,a,c,a')
    find_in_set(' ', '  , , ,') collate utf8mb4_general_ci
    find_in_set(' ' collate utf8mb4_general_ci, '  , , ,' collate utf8mb4_general_ci)
    format(12345.67, 2, 'en_us')
    format(1234567.89, 2, 'en_US')
    greatest('2020-01-01','99-1-1')
    greatest(-9223372036854775808, cast('9223372036854775809' as unsigned))
    greatest("a", "b", "c")
    greatest('a' collate utf8mb4_general_ci, 'B')
    hex(weight_string('a'))
    hex(weight_string('aAÁàãăâ' collate utf8mb4_general_ci))
    if(cast('2020-10-10 12:59:59' as datetime), 1, 2)
    ifnull(1, 'x' regexp '[')
    ifnull(null, cast('[1]' as json))
    interval("9007199254740991", "9007199254740992")
    interval(null, 1, 2)
    json_schema_valid('{"required":["a"]}', '{"a":1}')
    load_file('')
    make_set(1, 'a', 'b', 'c')
    NULLIF(1, "1.0")
    oct(1.0)
    regexp_like('abc', 'abc', 'p')
    round(1.2345,'2')
    round(3.14,'abc')
    round(5, -100)
    subtime('01:00:00.999999','02:00:00.999998')
    timestamp('2020-01-01','01:00:00')
    to_base64('')
    translate('ABC', 'A', 'B')
    translate('abcabc', 'ab', 'xy')
    truncate(1234.5678,'-2')
    upper(elt(1,'a',x'61'))
    weight_string(NULL)

They fall into five groups, in descending order of how much E work each needs:

1. **Lazy / conditional shapes** (`coalesce`, `ifnull`, `if`, `case when`,
   `nullif`, `elt`, `field`, `interval`, `in`, `or`, `benchmark`): these need
   the Tier-1 lazy execution and the `LazyTail`/`IfThree` shapes to be admitted
   for constant children, not just for column children. 25 of the 63.
2. **Declined constant types** (bit/binary literals in numeric context, `x'61'`
   under `upper`, `0e0` cast to datetime, `0x1e240` string conversion): the
   binary-literal leaf encoding listed in the TiKV semantic-gap doc. The
   temporal part of this group is done: `date`, `month` and `last_day` over an
   implicit cast now run in the engine.
3. **Collation-sensitive string kernels** (`greatest`, `find_in_set`,
   `weight_string`): engine compares bytes; a non-binary collation needs
   collator support or an explicit refusal.
4. **Native-only features** (`json`, `json_schema_valid`, `load_file`,
   `convert ... using cp866`, `regexp_like` with match type `p`, `convert_tz`,
   `extract`, `time`, `timestamp` -- the last four are admission-table
   exclusions with no local lowering yet): out of the parity target, since the
   native surface is the target and these exist only natively; after deletion
   they must produce the *classified* error rather than a wrong value.
5. **Genuine gaps with a known divergence** (`cot(1)` one ULP is a native bug
   the engine matches Go on; `oct(1.0)`; `format(...)` locale; `round`/`truncate`
   with a digit argument that is not a literal or a column).

Groups 3-5 are removal blockers only in the sense that they must become
*explicit, classified refusals*; groups 1-2 are the ones that need more engine
capability before the native code can go. Section 6's 140-test excluded surface
therefore over-states the work: the engine-constant subset is 63 expressions.

The `round`/`truncate` entries are worth naming because the refusal is not
where it looks. `round(1.2345,'2')`, `truncate(1234.5678,'-2')` and
`round(5,-100)` all pass the admission table and lower successfully; the
*engine* refuses the encoded program, for one reason shared with the
`ROUND(1.0, -400)` panic guard: the fractional digit must reach the kernel as
an `Int64`/`Uint64` literal or an input column, and here it is
`CastStringAsInt('2')` or `unaryminus(Int(100))` because the rewriter does not
constant-fold. `TIKV_EXPR_DEBUG_COMPILE=1` prints exactly which program the
engine refused and why, so a silent `NotAdmitted` no longer hides the gate.
The way in is an embedder-side constant fold of the digit or a runtime digit
check inside the kernel; both are engine work, not admission work.

The switch is inert unless `TIKV_EXPR_ENGINE_ONLY` is set, so dual-run remains
the default during the coexistence period.

### 7.2 The lazy arms' leaf rule is a VALUE rule, measured

The remaining lowering refusals in the control family (`coalesce(1, 1.1e0)`,
`1.50 or 0e0`, `case when false then 1.5 else 0 end`, `elt(1, 65)`) are family
mismatches: the adapter's `Shape` policy allows only leaves in a
possibly-skipped arm, and a mismatched leaf has to stay native rather than
receive a cast. The engine's lazy boundary can evaluate an arbitrary child on
demand (`ChildHandle::eval` calls `eval_subtree`), so the restriction looks
like it could be relaxed to "coerce every arm".

It cannot. Replacing the leaf rule with `coerce` and rerunning the dual-run
corpus produced three immediate disagreements (native vs engine):

| Expression | Native | Engine after the cast |
| --- | --- | --- |
| `case when 0.1 then 1 else 2 end` | `INT:1` | `INT:2` |
| `coalesce(1, 123.456)` | `DEC:1.000` | `DEC:1` |
| `if(cast('0.1' as decimal(2,1)), 1, 2)` | `INT:1` | `INT:2` |

The first and third are the same trap: Go's truthiness (`EvalInt` on a
non-zero DECIMAL is true) is not Go's integer cast, which rounds `0.1` to 0,
so an inserted `CastDecimalAsInt` flips the branch. The second is not a value
change but a *declared-shape* change: coercing the first arm rewrote the
result's decimal scale from 3 to 0. So the rule is not about warnings or eager
evaluation -- inserting an implicit cast changes the answer. The experiment
was reverted; a future change that wants `coalesce(1, 1.1e0)` in the engine
must reproduce Go's argument coercion for that signature, not reuse `coerce`.

### 7.3 `EXTRACT`, `TIME` and `TIMESTAMP` gain local lowerings

Four expressions left the admission group without any TiKV change:

- `TIME(x)` and `TIMESTAMP(x)` are casts. TiKV already dispatches the whole
  temporal cast matrix (`CastStringAsDuration`, `CastIntAsTime`, ...), so
  `families::temporal` lowers them through `coerce` and then *re-declares* the
  function's own result type, because the generic cast declares FSP 6 and a
  `TIME(0)` result would round differently. A `Duration` argument still
  declines: a duration-to-datetime cast needs "today", which the facade's
  context does not carry.
- `EXTRACT(unit FROM value)` keeps Go's two-argument shape with the unit as a
  string constant. TiKV has no `Extract` signature, but the simple units are
  exactly the per-unit signatures `temporal` already maps, so the call is
  lowered by re-entering that table under the unit's own function name
  (`YEAR` -> `Year`, `DAY` -> `DayOfMonth`, ...).

The duration units are deliberately **not** among them, and the dual-run found
out why on the first attempt. `extract(hour from '-25:03:04')` is `-25`
natively, but TiKV's `Hour`/`Minute`/`Second`/`MicroSecond` read the duration's
absolute components (`Duration::hours` is `to_secs().abs() / 3600`), so the
engine answered `25`. That kernel is right for Go's `HOUR()` builtin -- the
native port pins `hour('-10:30:45')` as `10` -- and wrong for Go's `EXTRACT`,
which keeps the sign. The two Go paths differ, so mapping EXTRACT onto those
kernels is an adapter error rather than an engine gap; `HOUR`/`MINUTE`/
`SECOND`/`MICROSECOND` stay native, and `EXTRACT` over a duration stays native
with them. `extract(year from 20240315)`, both `time(...)` cases and
`timestamp('2020-01-01')` are engine-covered and agree.

### 7.4 The minted `cast_*` spellings are NOT the internal `cast` arm

Most of the 22 "row excluded: no local lowering" names are the rewriter's
explicit-cast spellings: `CAST(x AS SIGNED)` becomes `cast_signed`,
`CAST(x AS DATETIME)` becomes `cast_datetime`, and so on. `cast_decimal` and
`cast_double` are already admitted through the reused pushdown catalog, so it
looks like the rest only need the same treatment -- and the local arithmetic arm
already derives `Cast{source}As{target}` from the source and the function's own
static type, which is exactly what these need.

Admitting them and routing them to that arm produced a *smaller* gap
(61 -> 59) and two immediate dual-run disagreements:

| Expression | Native | Engine |
| --- | --- | --- |
| `coalesce(cast('2020-10-10 12:59:59' as datetime), ...)` | `STR:2020-10-10 12:59:59.000` | `STR:2020-10-10 12:59:59` |
| an `INTERVAL` argument through a minted cast | `INT:0` | `INT:1` |

The first is the promoted-scale re-declaration a `COALESCE` of `DATETIME(0)`
and `DATETIME(3)` needs; the second is a rounding difference on an integer
target. Both are the same class as 7.2 -- "it is the same operation" is a
hypothesis, and the corpus refutes it -- so the experiment was reverted and the
minted spellings stay native. A future attempt has to reproduce Go's explicit
`CAST` coercion (result metadata included) rather than reuse the internal arm.

### 7.5 `NULLIF` is lowered, and the two sides build separately

`NULLIF(a, b)` is `IF(a <=> b, NULL, a)`, which is how Go rewrites it, so
`comparison()` now lowers it for any pair the comparison can promote. The first
attempt reused one promoted type for the whole node and the engine refused it:

    Expect `Int`, received `Decimal`

MySQL's `NULLIF` returns **expr1's** type, not the comparison's promoted type:
`NULLIF(1, 1.0)` compares as DECIMAL and returns BIGINT. So the condition is
built in the promoted type (`NullEqDecimal(CastIntAsDecimal(1), 1.0)`) while the
value arm stays expr1's type, and an `If` node never declares a type its value
child does not have. The same-type shape needs no cast at all, which is the
common `NULLIF(col, 0)`.

Two shapes stay native on purpose: a pair the comparison cannot promote
(`NULLIF(1, '1.0')`, where Go compares numerically) and a string pair with a
non-binary collation (`NULLIF('a', 'A')` is equal under `utf8mb4_general_ci`,
unequal bytewise). `NULLIF(1, 1.0)` is engine-covered now, which moved
`NULLIF(1, '1.0')` into the list in its place, and the fixture gained
engine-run checks for both the same-type and the promoted-condition shape over
a column.

### 7.6 Every excluded row now says which work it needs

Section 7.1 showed that the admission stage was reporting one placeholder
reason for almost every excluded name: *"no local engine lowering: not selected
by local_call, the temporal/JSON/vector families, or the reused pushdown
catalog"*. Read literally, that says "write a lowering", which is wrong for a
function the pinned engine cannot express at all -- and a wrong work list is
worse than none for the removal decision.

The 100 rows that used it are now split by a check anyone can repeat:

* **`NO_WIRE_SIGNATURE`** (4 names: `translate`, `weight_string`, `load_file`,
  `json_schema_valid`) -- `grep -E '^\s*<Name> = '` finds nothing in the pinned
  `tipb/proto/expression.proto`. No lowering can ever push these; after native
  deletion they must raise a classified error.
* **`NO_ENGINE_KERNEL`** (5: `format`, `char_func`, `convert_using`,
  `convert_tz`, `cast_vector`) -- the signature is in the proto, but
  `components/tidb_query_expr/src/lib.rs` has no `ScalarFuncSig::<Name>` arm.
  These need TiKV work, not adapter work.
* **`EXPLICIT_CAST_SPELLING`** (15: `cast_signed`, `cast_datetime`, ...) -- the
  kernel exists; section 7.4 is why the adapter cannot use it yet.
* **`NATIVE_SESSION_STATE`** (18: `current_user`, `connection_id`, `version`,
  `tidb_*`, ...) -- needs session state the facade's `Context` deliberately does
  not carry.
* **`NOT_TRIAGED`** (58) -- no lowering site selects the name and the
  engine-only corpus never reaches it. This is a to-do marker, not a finding.

The lesson generalises: an exclusion reason is load-bearing data for the
removal, and a boolean "excluded" was hiding four different kinds of work. A
name that the wire protocol cannot express is not a lower-priority lowering; it
is a permanent native exception, which changes what "delete the native
evaluator" can even mean for it.

### 7.7 The integer cast spellings are admitted, and they exposed an unsigned hazard

Section 7.4's rule covers the minted cast spellings as a group, but
`cast_signed` and `cast_unsigned` are the subset with no metadata of their own:
`CAST(x AS SIGNED|UNSIGNED)` is exactly `Cast{source}AsInt`, every source family
has that kernel, and the local cast arm already derives it from the function's
own static type. Admitting those two and matching them in the arm is
therefore safe where the temporal and string spellings are not.

Doing it exposed a second bug, in a different place. `test_interval_func` broke
with `INT:0` vs `INT:1` -- not because of the cast, but because the expression
`interval(9223372036854775807, cast('9223372036854775808' as unsigned))` had
been *declined* while `cast_unsigned` was excluded, and admitting it let the
unsigned pair reach the engine's `IntervalInt`, which compares the raw `i64` and
ignores the unsigned flag. `interval` is an ordering comparison, so an UINT64
above `i64::MAX` sorts wrong; `in`/`field` are equality, which survives unsigned
values only while every argument is unsigned (mixed signedness compares bit
patterns). `comparison()` now refuses those shapes instead of answering them
wrongly, which is why the corpus stays at zero divergences.

`cast(1 as signed) < cast(1 as signed)` is engine-covered, `cast_char` and the
rest still are not (they are the `EXPLICIT_CAST_SPELLING` rows of 7.6), and the
fixture gained engine-run checks for `cast_signed` over an integer and a decimal
column.

### 7.8 The string cast spellings are admitted too, and the guard keeps the fixed ones out

`cast_char` and `cast_binary` are the same shape of exception as 7.7: an
explicit `CAST(x AS CHAR|BINARY)` is `Cast{source}AsString`, every source family
has that kernel, and the target charset lives in the result `FieldType` rather
than in the signature. Both are admitted now, `cast('123' as char) < cast('123'
as char)` is engine-covered, and the fixture checks `cast_char`/`cast_binary`
over an integer column.

The local arm still refuses a *fixed-width* binary target
(`is_binary_string() && flen >= 0`), because padding and truncation there are
bounded by `max_allowed_packet`, which the facade's `Context` does not carry.
So `CAST(x AS BINARY(10))` stays native while `CAST(x AS BINARY)` runs.

What is left of the minted spellings is the temporal group (`cast_datetime`,
`cast_date`, `cast_time`) plus `cast_json`/`cast_year`, and 7.4 is why: they
carry result metadata (FSP, promoted scale, JSON document policy) that the
local arm does not reproduce. `cast('12:59:59' as time) < ...` took the place
of the `char` expression in the gap list.

### 7.9 The temporal cast spellings close the group, via the declared FSP

Section 7.4 kept `cast_datetime`/`cast_date`/`cast_time` native because routing
them through the local arm produced `STR:2020-10-10 12:59:59` where native said
`STR:2020-10-10 12:59:59.000` for
`coalesce(cast(... as datetime), cast(... as datetime(3)))`. The gap was never
in the arm: it is that TiKV's `CastTimeAsTime` passes the source value through,
while Go's promoted `COALESCE` result is declared `DATETIME(3)` and TiDB
*renders a declared precision as part of the value*. `bridge::check_time` now
carries the declared fractional precision onto the returned value, the same way
it already rebuilt a declared DATE, and the three spellings are admitted.

That also retires the other round-25 disagreement by a different route:
`test_interval_func`'s `INT:0` vs `INT:1` was never a cast problem -- it was the
`IntervalInt` unsigned hazard section 7.7 fixed once `cast_unsigned` let the
expression reach the engine.

The minted spellings are now down to `cast_json` and `cast_year`. Engine-only
corpus: 1148 passed / 61 failed / 59 declined, 0 divergences.

### 7.10 One more lazy-arm coercion is exact, and one is not

Section 7.2's rule is that a lazy arm may not acquire a coercion, because the
coercion can change the value. Two shapes were tested against that rule:

* **A numeric constant rendered as a string is safe.** Go wraps a lazy arm's
  value with `WrapWithCastAsString`, that rendering is exact, and the engine's
  `Cast{Int,Decimal}AsString` mirrors it. `lazy_args` now applies it, so
  `elt(0, 2, 3, 11, 1)` and `elt(1, 65)` run in the engine with zero
  divergences.
* **A numeric constant used as an `elt` index is not.** `elt(1.1, '2.1', ...)`
  answers the FIRST element natively while the engine's `CastDecimalAsInt(1.1)`
  index answered the second -- so the index does not follow the same coercion
  the value arms do. The one-run corpus caught it (`STR:2.1` vs `STR:3.1`) and
  the change was reverted.

That is the third time the same pattern has decided a question (7.2, 7.4, and
here), and the useful generalisation is narrower than "coercions in lazy arms
are unsafe": a coercion is safe exactly when it is the coercion Go itself
applies at that position. `WrapWithCastAsString` on a value is; `WrapWithCastAsInt`
on an `elt` index is not, because Go's `elt` index is not an ordinary int cast.

### 7.11 The engine-only outcome is now a CI gate

`TIKV_EXPR_ENGINE_ONLY=1` measures the gap, but it is an environment-gated run:
nothing in a normal `cargo test` stops a change from adding a native fallback.
`crates/tidb-expr/tests/tikv_ratchet.rs` turns the measurement into a gate. It
holds two pinned lists -- the 15 expressions that moved from native to engine
during this work, and the 59 that still decline -- and three tests:

* `covered_expressions_stay_in_the_engine` -- losing one is a regression;
* `declined_expressions_stay_declined` -- gaining one fails the test so the
  expression and the documented count move together;
* `the_gap_count_is_pinned` -- both list lengths are asserted.

The gate was mutation-tested: adding `date('20111213')` to the declined list
fails with *"date('20111213') now runs in the engine; move it to COVERED and
update the count in tikv-expression-corpus-plan.md"*, and the count test fails
with it. A rewrite or compile error counts as "not owned", which is what the
corpus harness does, so an expression the rewriter cannot build without a column
resolver is on the declined side.

This is objective item 2's counting gate: no change can add or remove an engine
fallback without saying so.

### 7.12 What deleting the native evaluator does to the 59, measured

The ratchet's fourth test,
`every_declined_expression_fails_cleanly_without_the_native_evaluator`, drives
each declined expression through `EvaluatorSuite` with a resolver whose
`tikv_expression_required()` is `true` -- the resolver milestone E ends up with
-- and asserts the outcome is never a value:

| Outcome | Count |
| --- | --- |
| structured `ExternalEngine` (code 1105) naming the refusal | 57 |
| planning-time refusal, never reaching evaluation | 2 |

The two planning-time refusals are pinned by name. `(1, 2) = (1, 2, 3)` is a
row-value comparison the rewriter cannot build without a column resolver, and
`convert('haha' using cp866)` names a charset the port does not support; both
fail before evaluation, which is a different and already-correct contract.

So the removal's error contract is asserted for the whole remaining set rather
than discovered during the cutover: no declined expression can silently produce
a value once native is gone, and each one carries its reason.

