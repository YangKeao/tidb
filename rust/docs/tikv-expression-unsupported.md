# TiKV engine-only unsupported inventory

> Generated from `docs/tikv-expression-coverage.json`. This is an admission/shape inventory, not a claim of full SQL semantic coverage.

## Contract

- Admitted expressions execute only in the shared TiKV expression engine.
- Excluded or non-lowerable shapes return a structured engine decline/unsupported error.
- There is no TiDB Rust native expression replay, error replay, or synthetic short-circuit fallback.
- A name can have both admitted and excluded rows because admission is signature-, type-, and shape-specific.

## Summary

- Admission rows: **384**
- Admitted rows: **211**
- Excluded rows: **173**
- Distinct names with at least one excluded shape: **173**

## Structural refusals

| Condition | Result |
|---|---|
| Session/executor has no TiKV expression context | Structured required-context error; feature-disabled builds return `Unsupported("the engine-only expression demo requires the tikv-expr feature")` |
| Row-major evaluator program | `ExternalEngine` code 1105: `TiKV expression engine does not admit row-major programs` |
| TiKV lowerer declines a compiled expression | `ExternalEngine` code 1105 with the lowerer decline reason |
| `SHOW ... WHERE` virtual-row resolver | Explicit demo contraction until it carries statement TiKV state |

## Excluded function shapes

| SQL name | Signature | Required eval types | Shape | Reason |
|---|---:|---|---|---|
| <code>&#x27;tidb`.(dateliteral</code> | <code>None</code> | — | <code>Any</code> | Go internal literal-function name, not a rewriter function spelling |
| <code>&#x27;tidb`.(timeliteral</code> | <code>None</code> | — | <code>Any</code> | Go internal literal-function name, not a rewriter function spelling |
| <code>&#x27;tidb`.(timestampliteral</code> | <code>None</code> | — | <code>Any</code> | Go internal literal-function name, not a rewriter function spelling |
| <code>acos</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>adddate</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>aes_decrypt</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>aes_encrypt</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>asin</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>atan</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>atan2</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>benchmark</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>bin_to_uuid</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>cast_decimal_in_union</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_int_to_decimal_in_union</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_json</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_real_in_union</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_real_to_decimal_in_union</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_string_to_decimal_in_union</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_unsigned_in_union</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>cast_vector</code> | <code>None</code> | — | <code>Any</code> | engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it |
| <code>cast_year</code> | <code>None</code> | — | <code>Any</code> | explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) |
| <code>ceil</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine decimal result-domain parity is not established |
| <code>ceiling</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine decimal result-domain parity is not established |
| <code>char_func</code> | <code>None</code> | — | <code>Any</code> | engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it |
| <code>charset</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>coercibility</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>collation</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>compress</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>concat</code> | <code>None</code> | — | <code>Any</code> | native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting |
| <code>concat_ws</code> | <code>None</code> | — | <code>Any</code> | native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting |
| <code>connection_id</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>conv</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine signed-prefix/base parity is not established |
| <code>convert</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>convert_tz</code> | <code>None</code> | — | <code>Any</code> | engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it |
| <code>convert_using</code> | <code>None</code> | — | <code>Any</code> | engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it |
| <code>cos</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>cot</code> | <code>None</code> | — | <code>Any</code> | the engine's libm tan differs from Go's result by one ULP; the native math kernel was deleted, so COT is explicitly unsupported until the engine can provide the required result |
| <code>curdate</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>current_date</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>current_resource_group</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>current_role</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>current_time</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>current_timestamp</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>current_user</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>curtime</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>database</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>date_add</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>date_sub</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>decode</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>default_func</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>degrees</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>encode</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>export_set</code> | <code>None</code> | — | <code>Any</code> | engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it |
| <code>floor</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine decimal result-domain parity is not established |
| <code>format</code> | <code>None</code> | — | <code>Any</code> | engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it |
| <code>format_bytes</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>format_nano_time</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>found_rows</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>from_base64</code> | <code>None</code> | — | <code>Any</code> | native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting |
| <code>from_days</code> | <code>None</code> | — | <code>Any</code> | TiKV returns the zero date for out-of-range input where Go returns NULL (EXPRESSION_SEMANTIC_GAPS.md) |
| <code>fts_match_word</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>get_format</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>get_lock</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getparam</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar_decimal</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar_int</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar_real</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar_string</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar_time</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>getvar_uint</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>grouping</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>ilike</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>insert_func</code> | <code>None</code> | — | <code>Any</code> | native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting |
| <code>instr</code> | <code>None</code> | — | <code>Any</code> | native string-tail kernels were removed; the pinned bridge does not safely preserve search collation or trim-direction metadata |
| <code>is_free_lock</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>is_used_lock</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>is_uuid</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>json_array_append</code> | <code>None</code> | — | <code>Any</code> | appending an array value through a nested path flattens it instead of appending the array (EXPRESSION_SEMANTIC_GAPS.md) |
| <code>json_array_insert</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>json_contains_path</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>json_merge</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>json_overlaps</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>json_pretty</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>json_schema_valid</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>json_search</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>json_storage_free</code> | <code>None</code> | — | <code>Any</code> | native JSON storage leaves were removed and no pinned-engine lowering for their binary-storage accounting semantics is admitted |
| <code>json_storage_size</code> | <code>None</code> | — | <code>Any</code> | native JSON storage leaves were removed and no pinned-engine lowering for their binary-storage accounting semantics is admitted |
| <code>last_insert_id</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>lastval</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>load_file</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>localtime</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>localtimestamp</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>locate</code> | <code>None</code> | — | <code>Any</code> | native string-tail kernels were removed; the pinned bridge does not safely preserve search collation or trim-direction metadata |
| <code>lpad</code> | <code>None</code> | — | <code>Any</code> | native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path |
| <code>make_set</code> | <code>None</code> | — | <code>Any</code> | native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting |
| <code>match_against</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>md5</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>name_const</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>nextval</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>now</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>password</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>position</code> | <code>None</code> | — | <code>Any</code> | native string-tail kernels were removed; the pinned bridge does not safely preserve search collation or trim-direction metadata |
| <code>pow</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine domain/error parity is not established |
| <code>power</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine domain/error parity is not established |
| <code>radians</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>rand</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>random_bytes</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>release_all_locks</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>release_lock</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>repeat</code> | <code>None</code> | — | <code>Any</code> | native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path |
| <code>round</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine digit/result parity is not established |
| <code>row</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>row_count</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>rpad</code> | <code>None</code> | — | <code>Any</code> | native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path |
| <code>schema</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>sec_to_time</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>session_user</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>setval</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>setvar</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>sha</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>sha1</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>sha2</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>sin</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>sleep</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>sm3</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>space</code> | <code>None</code> | — | <code>Any</code> | native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path |
| <code>subdate</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>sysdate</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>system_user</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>tan</code> | <code>None</code> | — | <code>Any</code> | native trig was removed and the engine's libm path is not verified bit-exact with Go |
| <code>tidb_bounded_staleness</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>tidb_current_tso</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>tidb_decode_binary_plan</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>tidb_decode_key</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>tidb_decode_plan</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>tidb_decode_sql_digests</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_encode_index_key</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_encode_record_key</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_encode_sql_digest</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>tidb_is_ddl_owner</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_mvcc_info</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_parse_tso</code> | <code>None</code> | — | <code>Any</code> | native residual temporal kernel was removed and no verified pinned-engine lowering is admitted |
| <code>tidb_parse_tso_logical</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_row_checksum</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>tidb_shard</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>tidb_version</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>time_format</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>timestampadd</code> | <code>None</code> | — | <code>Any</code> | native residual temporal kernel was removed and no verified pinned-engine lowering is admitted |
| <code>to_base64</code> | <code>None</code> | — | <code>Any</code> | native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path |
| <code>translate</code> | <code>None</code> | — | <code>Any</code> | unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function |
| <code>trim</code> | <code>None</code> | — | <code>Any</code> | native string-tail kernels were removed; the pinned bridge does not safely preserve search collation or trim-direction metadata |
| <code>truncate</code> | <code>None</code> | — | <code>Any</code> | native math was removed and engine digit/result parity is not established |
| <code>uncompress</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>uncompressed_length</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>unix_timestamp</code> | <code>None</code> | — | <code>Any</code> | native session-zone temporal kernel was removed; the pinned engine cannot reproduce named-zone DST gap/ambiguity semantics |
| <code>user</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>utc_date</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>utc_time</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>utc_timestamp</code> | <code>None</code> | — | <code>Any</code> | statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host |
| <code>uuid</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>uuid_short</code> | <code>None</code> | — | <code>Any</code> | not triaged: no lowering site selects this name and the engine-only corpus never reaches it |
| <code>uuid_timestamp</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>uuid_to_bin</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>uuid_v4</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>uuid_v7</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>uuid_version</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>validate_password_strength</code> | <code>None</code> | — | <code>Any</code> | native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted |
| <code>values</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>vec_from_text</code> | <code>None</code> | — | <code>Any</code> | native vector SQL kernels were removed and the pinned engine does not dispatch VecFromTextSig/CastStringAsVectorFloat32 |
| <code>version</code> | <code>None</code> | — | <code>Any</code> | session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context |
| <code>vitess_hash</code> | <code>None</code> | — | <code>Any</code> | native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV |
| <code>weight_string</code> | <code>None</code> | — | <code>Any</code> | native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path |

## Reason counts

| Reason | Rows |
|---|---:|
| session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context | 36 |
| not triaged: no lowering site selects this name and the engine-only corpus never reaches it | 31 |
| native miscellaneous kernels were removed; only ANY_VALUE remains admitted through TiKV | 17 |
| native crypto was removed and no engine-compatible implementation with verified diagnostics, collation, and session semantics is admitted | 14 |
| statement clock: tipb can name it, but the engine has no kernel (only SYSDATE does, and that one reads the host's own clock), and every row of a statement must read the statement's start time under the session time zone, so the clock has to come from the host | 10 |
| native trig was removed and the engine's libm path is not verified bit-exact with Go | 9 |
| explicit-CAST spelling: no lowering selected, and routing it through the local cast arm is not equivalent because Go's explicit CAST coercion and result metadata differ (corpus plan 7.4) | 8 |
| unrepresentable on the wire: the pinned tipb defines no ScalarFuncSig for this function | 7 |
| engine has no kernel: tipb defines the signature but the pinned engine does not dispatch it | 6 |
| native packet-limited string kernels were removed; the pinned engine has no equivalent max_allowed_packet statement setting or verified WEIGHT_STRING wire path | 6 |
| native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting | 5 |
| native string-tail kernels were removed; the pinned bridge does not safely preserve search collation or trim-direction metadata | 4 |
| Go internal literal-function name, not a rewriter function spelling | 3 |
| native math was removed and engine decimal result-domain parity is not established | 3 |
| native JSON storage leaves were removed and no pinned-engine lowering for their binary-storage accounting semantics is admitted | 2 |
| native math was removed and engine digit/result parity is not established | 2 |
| native math was removed and engine domain/error parity is not established | 2 |
| native residual temporal kernel was removed and no verified pinned-engine lowering is admitted | 2 |
| TiKV returns the zero date for out-of-range input where Go returns NULL (EXPRESSION_SEMANTIC_GAPS.md) | 1 |
| appending an array value through a nested path flattens it instead of appending the array (EXPRESSION_SEMANTIC_GAPS.md) | 1 |
| native math was removed and engine signed-prefix/base parity is not established | 1 |
| native session-zone temporal kernel was removed; the pinned engine cannot reproduce named-zone DST gap/ambiguity semantics | 1 |
| native vector SQL kernels were removed and the pinned engine does not dispatch VecFromTextSig/CastStringAsVectorFloat32 | 1 |
| the engine's libm tan differs from Go's result by one ULP; the native math kernel was deleted, so COT is explicitly unsupported until the engine can provide the required result | 1 |

## Regeneration

```bash
cd rust
python3 scripts/tikv_expression_coverage.py --self-check --check
python3 scripts/tikv_expression_unsupported.py --check
```
