# M0 coverage baseline — Validation E evidence

This is an evidence ledger, **not a second plan**. The sole plan remains `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`, edited only by the main agent. Validation E performed inventory/source analysis only: no product edits, Cargo/build/test commands, commits, resets, or changes to another agent's files.

## Frozen scope and counting rule

| Item | Baseline |
|---|---|
| TiDB | `364aef2bab5cc633ecb76a775ae8f36f86a6687d` |
| TiKV | `548812e1ef57aef077a2062a9cc356640a6347f5` |
| Worktrees | `/home/agent/tidb/expression-unification/{tidb,tikv}` |
| Implemented pure/contextual family denominator | **245** |
| Minimum fully migrated families for at least 90% | **221** |
| Host-only family rows, separately listed below | **35** |
| Compatibility-stub rows | **1** (`load_file`) |
| Total family rows | **281** |
| Separate registry-only / syntax / unavailable rows | **12** |
| Actual registry tuples | **308**, not the stale comment's 309 |
| Full-domain migrations established by this inventory | **0** |

The denominator is a union of actual function dispatch, binary/unary operators, synthetic casts, explicit AST computations, typed/PB execution, and public value-helper domains. It is **not** registry size, protobuf enum size, test count, call frequency, or number of aliases. The numerical equality between the **245 families** and **245 distinct PB/native admitted signature IDs** below is coincidental; they are different sets.

`coverage-baseline.json` is the machine ledger. It has one family row per canonical ID, shared domain profiles, source-site IDs, test-module IDs, complete PB/native signature rows, source/function hashes, source deletion map, and consumer/test tables. Repeated tables are shared rather than copied into every row. Tables are serialized one JSON record per line to keep this ledger near 10k lines rather than the initial 87k-line draft. It is ordinary JSON, not JSONL.

`collect-coverage-baseline.py` reads **fixed `SHA:path` Git objects** from the new worktrees. Concurrent product or test edits cannot change this baseline. Old `expression-reuse` implementation trees were not inspected or used. The main agent's toolchain wrapper references an installed compiler there; that is not implementation evidence.

### How families were normalized

Actual dispatch bodies were extracted with comment/string-aware balanced-brace scanning; explicit syntax/binding additions were reviewed separately. Registry arities are recorded as **construction contracts**, not proof of implemented overloads. Wire match left-hand sides are independently enumerated and set-checked. Aliases include CEIL/CEILING, POW/POWER, LOWER/LCASE, UPPER/UCASE, CHAR_LENGTH/CHARACTER_LENGTH, LENGTH/OCTET_LENGTH, SUBSTRING/SUBSTR/MID, DAY/DAYOFMONTH, DATE_ADD/ADDDATE, DATE_SUB/SUBDATE, SHA/SHA1, and documented clock/session aliases.

* CAST is **one family** with all target/source/implicit/in-union variants, not dozens of denominator entries. `CONVERT(x,type)` belongs to CAST; `CONVERT(x USING charset)` belongs to charset conversion. `to_binary`/`from_binary` are charset-conversion domains.
* ATAN's one/two-argument forms and ATAN2 are one family. POSITION grammar belongs to LOCATE; INSTR remains separately counted. REGEXP/RLIKE and REGEXP_LIKE share a family, with their wrapper/match-type domains preserved. JSON_MERGE/PRESERVE share a value operation, but the deprecated spelling's warning remains an obligation.
* NOT IN/BETWEEN/LIKE and IS NOT variants are retained as negative syntax domains under the base family plus NOT; they are not asserted to have identical results. `istrue_with_null` remains a distinct, actually implemented NULL-preserving family despite a stale builtin-list exclusion comment.
* BETWEEN has its own native AST computation, so is counted. DATE/TIMESTAMP literal validation computes during rewrite and differs from ordinary CAST, so each is counted. Ordinary constant leaves, columns, parameter bindings, parens, and ROW carriers do not inflate the denominator.
* `sig_{ScalarFuncSig}` names are diagnostics, **not 129 extra families**. Every admitted PB signature is instead a domain row under an existing family.

### Reviewed correction to the early draft

The early 236-pure/44-host draft was **not** the freeze. Nine mixed families were moved into the denominator before finalization: RAND; NOW, CURDATE, CURTIME, UTC_TIMESTAMP, UTC_DATE, UTC_TIME, SYSDATE; and TIDB_BOUNDED_STALENESS. Thus the final counts are **245/35/1**, not 236/44/1.

`math_fn/mod.rs:692–714` proves a deterministic per-row seeded RAND path (`MysqlRng::new_with_seed(seed).gen()`); constant-seed occurrence state and unseeded session RNG are separate binding/lifetime domains. The generator algorithm in `tidb-util/src/mathutil/rand.rs:30–61` is not host-exempt. Clock functions' FSP parsing, rounding, timezone/date/time formatting, and staleness clamping are likewise not excluded merely because the instant/SafeTS is host-owned. UNIX_TIMESTAMP, JSON_SCHEMA_VALID, and VALIDATE_PASSWORD_STRENGTH already remained counted despite host-dependent subdomains.

### Complete denominator by category

The JSON contains the complete sorted IDs/spellings, not just these totals.

| Category | Families |
|---|---:|
| Numeric/bit operators | 14 |
| Comparison/IN/BETWEEN/extrema/INTERVAL | 12 |
| Logic/control/NULL predicates | 13 |
| CAST | 1 |
| Charset conversion | 1 |
| Math including seeded RAND | 26 |
| String | 43 |
| LIKE/ILIKE/regexp | 6 |
| Temporal, including clock subkernels and typed literals | 57 |
| JSON | 28 |
| Vector | 8 |
| Crypto/compression/password policy | 13 |
| Network address functions | 8 |
| Miscellaneous pure kernels/metadata masks | 15 |
| **Total** | **245** |

## Complete host-only exclusion list (35)

These exclusions describe the **outer host service or binding**, not permission to retain duplicate general-purpose cast/string/math algorithms in its arguments. Ordinary child expressions must still use the unified evaluator, with correct demand/multiplicity. Pure UUID transforms, formatting, GROUPING metadata-mask computation, TiDB shard/hash, and TiDB plan/digest transformations are counted, not blanket-excluded as “TiDB-specific.”

| Exact canonical IDs | Count | Exclusion reason |
|---|---:|---|
| `charset`, `collation`, `coercibility` | 3 | Read frontend FieldType/expression metadata; not value computation. |
| `connection_id`, `current_resource_group`, `current_role`, `current_user`, `database`, `user`, `version`, `tidb_version` | 8 | Session identity/database/resource group, server build/system-variable values. Their aliases remain in the ledger. |
| `found_rows`, `row_count`, `last_insert_id` | 3 | Previous/current statement result/insert-id state; LAST_INSERT_ID(expr) mutates session state. |
| `getparam`, `getvar`, `setvar`, `values` | 4 | Prepared parameter, user-variable, and current INSERT-row bindings/mutation. Type casts of those values remain counted CAST obligations. |
| `nextval`, `lastval`, `setval` | 3 | Sequence/catalog/session allocation state. |
| `get_lock`, `release_lock`, `release_all_locks`, `is_free_lock`, `is_used_lock` | 5 | Advisory-lock service. |
| `tidb_current_tso`, `tidb_decode_key`, `tidb_is_ddl_owner` | 3 | Transaction/DDL-owner/catalog callbacks. In particular `builtin_ext/misc.rs:64–71` calls `Columns::tidb_decode_key`; it is not merely a stateless decoder. |
| `benchmark`, `sleep` | 2 | Repeated child execution or wall-clock/cancellation service, not pure value kernels. BENCHMARK requires a demand protocol, not eager pre-evaluated arguments. |
| `random_bytes`, `uuid`, `uuid_v4`, `uuid_v7` | 4 | Entropy/process/clock allocation primitives with no exposed deterministic seeded value overload. Their parameter coercion and reusable packing/formatting subkernels are still deletion/delegation obligations; deterministic UUID parsing/transforms are separately counted. |
| **Total** | **35** | |

Source evidence for each exclusion is retained in its family row. Statement/context sensitivity by itself is **not** an exclusion criterion.

## Not counted as implemented pure families

* `load_file`: native code deliberately returns SQL NULL; there is no filesystem-reading implementation. It is a compatibility stub, neither a successful migrated filesystem kernel nor a denominator-padding constant.
* `tidb_decode_sql_digests`, `tidb_encode_index_key`, `tidb_encode_record_key`, `tidb_mvcc_info`, `tidb_row_checksum`: registry-only in the audited Rust baseline; no executable dispatch/lowering found across current Rust crates. “Never implemented” here means at this fixed native-evaluator scope, not a claim about all project history or Go implementations.
* `uuid_short`: explicitly `FunctionNotExists` in `builtin_registry.rs:479–505`.
* `fts_match_word`, `match_against`: native full-text evaluators are explicitly unavailable (`builtin_registry.rs:491–500`). There is an implemented gated ILIKE-composition fallback (`fts.rs`) and a session regression; its ILIKE/IFNULL/logical domains remain counted. This is not a native full-text-search kernel.
* `row`: structural carrier; row comparisons/IN expand to counted comparison/control domains. Standalone ROW has no scalar result evaluator.
* The three internal ``'tidb`.(...literal`` registry spellings are not runtime-dispatched function names. DATE/TIMESTAMP use the two counted rewrite-time literal kernels. TIME literal currently falls through ordinary cast lowering, while direct AST `eval_in` rejects all three literal styles; a dedicated TIME-literal validator is not claimed.

These are the **12 separate registry rows** plus the one stub family, not hidden omissions.

## Supported-domain evidence and explicit limits

The exact baseline domain is defined **intensionally** by the fixed executable branches, guards, and referenced function/source hashes. The JSON summarizes these branches; it does not invent a full Cartesian matrix. A family's mere presence means an implementation domain exists, **not** that all SQL argument combinations work or that its existing tests cover every domain. Every counted row retains unresolved cross-product and TiKV-compatibility status.

Important frozen axes:

* **Numeric/operators:** signed/unsigned pairing, Real/Decimal/int lanes, Float32 widening, binary-literal provenance, YEAR/BIT/ENUM/SET hybrids, NULL evaluation order, division precision (including explicit zero), NO_UNSIGNED_SUBTRACTION, overflow/truncation policy, warning order, and expression text. Native fast/vector paths are separate obligations.
* **CAST:** Signed, Unsigned, UnsignedInUnion, Char, Binary, Decimal, Date, DateTime, Year, Double, Float, Vector, Time, Json; implicit argument casts and source-specific in-union casts are not extra families. `cast.rs:64–72` restricts vector sources to Char/Binary/Vector. ARRAY casts are rejected. Full flags/width/scale/FSP/source type, not just EvalType, select behavior.
* **String/pattern:** binary octets versus UTF8 rune units, invalid-octet handling, explicit/implicit charset transcode, derived versus datum/default collation, packet limit, match/escape/position/occurrence options, and statement-keyed successful metadata caches. LENGTH's binary-aware encoding is not CHAR_LENGTH's unit count. PAD SPACE trimming is not universal string normalization.
* **Collations:** `collation.rs:204–235` admits 16 names plus legacy DerivedBinary mode and a fallback to utf8mb4_bin. The pinyin name is a registered **panic stub** (`609–704`), not a supported computation domain. Catalog admission does not prove each expression supports all collations. Signed wire IDs, compare/key/hash/LIKE policies, no-pad paths, and malformed bytes require separate proof.
* **Temporal:** source FieldType, Date/Datetime/Timestamp/Duration, numeric/text input, interval units, FSP, timezone/DST/date SQL modes, statement date, default_week_format, diagnostics, and typed versus AST result representation. NOW/UTC_TIME/CURTIME have observably different no-arg/explicit-FSP/NULL rounding rules. Host clock binding is retained; pure formatting is counted.
* **JSON:** BinaryJSON versus text, SQL NULL versus JSON null, typed SQL-to-JSON scalars/opaque binary values, paths/wildcards, diagnostics and metadata caches. JSON_SUM_CRC32 has a homogeneous scalar-array helper domain (`tests/builtin_info_json_math_source.rs:579–598`), **not** typed ARRAY-cast support. JSON_SCHEMA_VALID's local validation and external HTTP/file `$ref` retrieval are distinct domains, not a blanket pure or blanket host assertion.
* **Crypto/password:** AES mode/key-size/IV and lazy ignored-IV warning; binary-aware input encoding; password policy/identity/global-var binding. **Vector:** eight value functions, source dimensions and numeric behavior, not automatic all-cast or all-PB coverage.
* **Control:** AST branch result shape versus typed merged result shape, nullability, demand and multiplicity. Source comments are sometimes stale: actual AST and typed COALESCE branches are lazy despite an old `func.rs` comment describing eagerness.

### Minimal demand counterexamples to characterize (not executed by E)

`lib.rs:902–920` evaluates both AST binary children; `scalar_function.rs:1618–1632` and PB `Kernel::Logic` short-circuit. Proposed direct-AST probes are `0 AND ('x' REGEXP '[')` and `1 OR ('x' REGEXP '[')`: the AST reaches the invalid regexp; a directly built no-fold typed logical node should not evaluate its dead child. This is a **source-proven discrepancy / runtime-unverified prediction**, not a passing oracle result. Disable folding in the typed construction to avoid confusing build-time failure with evaluator demand.

Existing relevant tests: `tests/control.rs::{if_source_vectors_use_wrapped_condition_and_lazy_branch,ifnull_source_vectors_preserve_first_non_null_value,case_when_source_vectors_preserve_lazy_truthiness,coalesce_source_vectors_preserve_first_non_null_value}` and `distsql_builtin::tests::protobuf_control_does_not_evaluate_the_unused_branch`. Strict unification may correct AST behavior, but must record that correction and add red/green/oracle evidence rather than claiming the original paths were identical.

## PB typed Kernel and unistore SimpleSig inventory

| Static set | Count |
|---|---:|
| `PbBuiltin::new` accepted IDs P | **129** = 86 non-cast + 43 casts |
| Explicit native converter IDs S | **218** |
| `SimpleSig` enum variants | **163** |
| P ∩ S (wire-shadowed native mappings) | **102** |
| P − S | **27** |
| S − P (residual native wire domains) | **116**, represented by **61** variants |
| P ∪ S | **245 single-node admitted IDs**, not tree closure |
| S domains explicitly returning None despite admission | **8** |

Every exact signature is listed under `pb_and_unistore.signatures` and linked to a family. The typed cast set is the 42 off-diagonal pairs over `{Int,Real,Decimal,String,Time,Duration,Json}` plus `CastTimeAsTime`; it is not a full identity/vector-cast matrix. Typed control supports CaseWhen/If for six types excluding String; IfNull has seven types including String. A latent VectorFloat32 cast target branch has no selector in `cast_types` and is not counted as supported PB cast.

Residual native wire domains are:

* 56 date +/- IDs: `{AddDate,SubDate} × {String,Int,Real,Decimal,Datetime,Duration} × {String,Int,Real,Decimal}` (48) plus `{AddDate,SubDate}Duration{String,Int,Real,Decimal}Datetime` (8).
* 28 comparisons: LtInt/LeInt/GeInt/NeInt and `{Lt,Le,Gt,Ge,Eq,Ne} × {Real,String,Decimal,Time}`. Typed PB has EqInt/GtInt, not general comparison coverage.
* 14 integer arithmetic IDs: four signedness-specific PlusInt; MinusInt plus four signedness-specific and three forced-unsigned MinusInt; MultiplyInt and MultiplyIntUnsigned.
* 6 integer divisions; 3 real arithmetic IDs; 6 non-time identity casts; InInt/InString/LikeSig.

The eight `Duration*Datetime` date variants are admitted but `eval_time:3578–3588` returns None; an existing test asserts the refusal. The other 48 date variants have native text/datetime/duration result paths. Admission is not successful value coverage.

### Routing and consumer restrictions

`cophandler.rs:2077–2082` routes any typed-supported scalar immediately to `convert_shared`, using `supports_signature == PbBuiltin::new(...).is_some()`, **not** the remote encoder catalog. `pb_to_expr` recursively requires typed support for every child. Therefore:

* `LtInt(PlusInt(...), ...)` can be a native Func with a Shared child.
* `PlusInt(LtInt(...), ...)` and `LogicalAnd(InInt(...), ...)` fail typed child decoding; no native fallback repairs them.
* All 102 intersecting native wire mappings are shadowed, but public `SimpleExpr::Func` and tests still directly instantiate their variants. They are not globally dead Rust code.
* Native top-level literal decoding supports seven literal kinds + ColumnRef; Shared adds Uint64/Bytes/Float32/MysqlDuration. Native Float64 reads raw bits whereas Shared uses `decode_float`; their equality is not established.
* Six native `eval_*` value helpers call `eval_shared(...).ok()?`, swallowing errors into None. Some native integer branches also swallow child Err. Do not treat these Option channels as proven correct SQL NULL propagation.
* `eval_datum:1248–1267` and TopN reject Func computations but accept Shared. Aggregate args use this boundary. Group-by is Column-only even for Shared.
* Request timezone/flags/precision/column types and one warning sink/drain belong to RequestEvalContext. Signature-selected argument signedness differs from result wire unsigned interpretation. Preserve full wire FieldType, including enum/set elements.

PB-specific existing tests cover signature-vs-display-name independence, preserved wire type, MOD signedness, JSON reuse across rows, lazy IF, and binary-vs-UTF8 CHAR_LENGTH. Catalog decoder tests do **not** prove all 218 native signatures or arbitrary-tree evaluation. Most old native tests construct Func directly, bypassing wire routing.

## Live entrypoints and native deletion map

All paths in this section are relative to TiDB repository root. Exact symbols/lines and preservation obligations are in JSON `entrypoints` and `native_deletion_map`.

| Surface | Source evidence / compute to replace | Keep |
|---|---|---|
| Direct AST and values helpers | `rust/crates/tidb-expr/src/{lib.rs,func.rs}`: eval_in, apply_binary/unary, concat_values, date_add_interval, avg_of*, fit_decimal_column, get_time_value | Binding/AST interfaces; fold-disabled prepare; host services. Helpers are not all mere wrappers. |
| Typed row | `scalar_function.rs:1182–1202`: PB-first, then integer fast path, then eval_by_signature | Node identity/hash/schema/FieldType/decorrelation; detached compiled state and invalidation. Replacing only eval_by_signature misses two paths. |
| PB | `scalar_function/pb_builtin.rs`, `distsql_builtin.rs` | Wire IDs/types and representation adaptation, not a second algorithm selected by display name. |
| Vector | `evaluator.rs:204–206,406–420`; `scalar_function.rs:3293–3407,4187–4215,4437–4470` | Logical/physical selection, EQ-from-IN NULL mask, row order, constant broadcasts, ColumnSwapHelper. |
| Projection/Expand | executor `projection.rs:140–141,295–303,381–383`; `expand.rs:54–57,100–105` | Shared immutable program, execution/worker-local suite and owner transfers. |
| Selection bypass | executor `selection.rs:62–73,110–146,158–253,277–323` FastSelectionFilter NullTest/StringIn/And | This shortcut is **nonbatched only**. Batched selection calls vectorized_filter_consider_null. |
| **Additional scan bypass** | executor `predicate_pushdown.rs:247–264,362–378,610–715` FastScanFilter StringIn/Like | Scan acceptance/binding and negation. Direct key-hash/LIKE computations must also migrate. |
| **Additional group bypass** | executor `vec_group_checker.rs:104–176,439–457` directly calls try_eval_numeric_batch | First/last-boundary-first evaluation order, grouping state. |
| Folding/planner | expr constant_fold, expr_util/fold/substitute; planner plan_builder/ranger/physical_plan_cache/logical rewrite | TryFold warning rollback/stash, deferred parameter scope and inference; not whole-file deletion. |
| Default/generated/CHECK/partition | executor column_default/generated_column/union_scan/kv_table/partition_routing/partition_pruning; exec table_info_build | Current statement/dependency bindings, storage conversion, NULL CHECK policy. |
| DML/correlated | executor driver/dml and driver/dml/correlated | Prepare once and bind per row; original-row assignment semantics, host subquery execution. |
| Join | executor joiner/hash_join_v2/base_join_probe/apply/native/join/index_hash | Per-worker context, CNF order and IN unknown masks. |
| Aggregate/window/sort/group | executor hash_agg/input/group_key/window_numeric, stream_agg, window, shuffle | States/executors stay TiDB-owned; arguments and shared SUM/AVG arithmetic migrate. Sort admits columns/constants without evaluating constant keys; noncolumn local TopN keys are projected first. |
| Session | show.rs directly calls AST; SET uses SELECT execution; prepared_ast/warnings/dispatch refresh/drain | Session lifecycle, fresh binding and one diagnostic ownership boundary. |
| Unistore | native `SimpleSig`, Func converter/dispatch and all value/date helpers | Scan/storage, RequestEvalContext, row decoding, TopN/aggregate infrastructure, one warning drain. |

Pure implementations to delete or delegate as their complete domains close include `ops*`, `coerce`, `cast`, `arg_eval_type`, `row`, `math_fn/*`, `string_fn`, `string_signature`, `string_packet`, `like`, `regexp`, `time_fn/*`, `time_literal`, `builtin_ext/*`, `convert_charset`, `go_flate`, GROUPING computation, and RNG subkernel. Mixed modules cannot be removed wholesale. Metadata constructors/inference are not duplicate evaluators simply because they share a file.

Primitive deletion also reaches datatype collation, Decimal/MyDecimal, datum compare/convert, JSON/time/duration operations and their aggregate/codec/key consumers. Storage codec and Chunk layout are not automatically replaced. A wrapper conversion alone is not Decimal algorithm unification.

Unistore exact production sites: definitions `1526–2034`; converter `2073–2682`; eval_decimal `2695–2836`; eval_json `2859–3042`; eval_real `3046–3211`; eval_duration `3414–3530`; eval_time `3534–3680`; eval_bytes `3784–4049`; Func dispatch `4154–5053`; date helpers `3219–3412`. After pruning only the 102 shadowed domains, date helpers and numeric_prefix remain necessary. `collation_of` still serves TopN. Never delete native tests simply because their enum is being retired: port them to the shared entrypoint and add routing/origin assertions.

## Existing test targets and exact commands

**No tests below were run by Validation E.** Static declarations/filters are not executable discovery, passed results, or full-domain coverage. Main agent serializes all heavy builds.

TiDB manifests set `autotests=false` and use `rust/scripts/aggregate-tests.rs`. The script aggregates top-level `tests/*.rs` except all.rs, sibling `#[path]` helper sources and files marked `aggregate-test: standalone`. An explicit `[[test]]` alone does not remove a file from aggregation. Thus filename != Cargo target. Static included module counts: expr 5, executor 79, planner 141, session 309, exec 148.

`tidb-expr --test all` has 18 literal tests: benchmark_source 2, field_name_resolution_source 3, info_metadata_source 3, pb_int_predicate_source 9, pb_string_predicate_source 1. The PB predicate files primarily test admission/serialization; run distsql_builtin unit tests too.

Planner `core_expression_eval_source` has 11 declarations including one ignored placeholder. `prepare_plan_cache_session_suite_source` has **25 all ignored**, and `instanceplancache_builtin_func_source` has **8 all ignored**. A zero-runnable-test green result is not cache validation. `tests_prepared_plan_cache` in session has 46 nonignored declarations. TiKV baseline has **no `local::tests` module**; that filter is future acceptance, not baseline coverage.

Run the following from `/home/agent/tidb/expression-unification/tidb/rust`, using the main agent's isolated-cache/pinned-compiler wrapper. The repository's ordinary equivalent is `cargo test --offline --locked -j12 ...` when dependencies/toolchain are ready; resource settings for this experiment remain parent-owned.

```bash
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib evaluator::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib constant::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib constant_fold::deferred_function_tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib distsql_builtin::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --test all -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-unistore --lib cophandler:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib projection::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib selection::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib predicate_pushdown::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib vec_group_checker::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib expand::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib column_default::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib generated_column::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib driver::tests::dml:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib driver::tests::column_defaults:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib joiner::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib hash_agg::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib stream_agg::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib partition_pruning::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --test all window_executor_source:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --test all default_on_update_source:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-planner --test all core_expression_eval_source:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-session --lib tests_eval_bool:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-session --lib tests_in_list_full_evaluation:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-session --lib tests_prepared_plan_cache:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-session --lib tests_generated_columns:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-session --lib tests_window:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-session --test all expression_default_fold_source:: -- --test-threads=1
```

From `/home/agent/tidb/expression-unification/tikv` (parent supplies the pinned TiKV toolchain/native-build environment):

```bash
cargo test --locked -p tidb_query_datatype --lib codec::collation:: -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_like::tests:: -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib short_circuit -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_control::tests:: -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_compare_in::tests:: -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_regexp::tests:: -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_cast::tests:: -- --test-threads=1
```

These TiKV filters have respectively 8, 4, 16, 4, 5, 4, 79 literal test declarations. JSON records exact cwd/commands and the larger test-source table; macro/cfg/ignored cases still require actual discovery.

### Separately attributed parent-run baseline receipts

E read the completed logs, but did not launch these commands:

| Parent command (TiDB rust cwd) | Observed result | Log under `/home/agent/tidb/expression-unification/logs/` |
|---|---|---|
| `tools/cargo-tidb test --locked -p tidb-datatype --lib collation -- --test-threads=1` | 25 passed, 0 failed | `tidb-datatype-collation-baseline.log:92` |
| `tools/cargo-tidb test --locked -p tidb-datatype --test all collation_sort_keys_match_go_byte_for_byte -- --test-threads=1` | 1 passed, 0 failed | `tidb-go-collation-baseline.log:23` |
| `tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1` | **1226 passed, 4 failed, 93 ignored; 1323 discovered** | `tidb-expr-lib-baseline.log:2078–2112` |

The wrapper basename in this receipt is the parent's command label; the executable's absolute path is shown in the runnable commands above. The four original-baseline failures are:

1. `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`: expected `build_call("ifnull", [literal,column]).is_none()`.
2. `tests::builtin_info_json_math_source::exp`: expected FloatOverflow for EXP(100000).
3. `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`: negative duration FSP panic at datatype `duration.rs:211`.
4. `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`: STR_TO_DATE("01","%d") without NO_ZERO_DATE returns NULL rather than expected zero-year/month text.

Main agent reports all four independently reproduced, each exit 101, in `tidb-expr-baseline-isolated-failures-v2.log`. These precede algorithm migration and are **not migration regressions**. Passing baseline tests do not establish every family's complete domain. Later tests-only additions are not part of this fixed-SHA inventory.

Parent subsequently completed the source-clean original TiKV baseline. E read the result summaries: collation **8/8** (`tikv-collation-baseline-native-compat.log:33`), full expr **428/428** (`tikv-expr-baseline.log:483`), and `codec::mysql::decimal::tests` **26/26** (`tikv-decimal-baseline.log:34`). Native-build compatibility environment and exact invocation remain parent-owned. These are local baseline passes, not TiDB/TiKV cross-domain parity. Implementation agents were released afterward; this inventory continues reading immutable baseline Git objects.

## Integration recommendations and unresolved evidence

* Use the 245 fixed IDs for coverage. Count a family complete only when **all existing supported overload/type/collation/context/entrypoint domains** are shared and native algorithms retired. Partial and explicit deferred domains stay visible; do not later remove hard families from the denominator.
* Preserve full signature/FieldType/literal provenance at one lowering boundary; synthetic names, slot binding and remote pushdown authorization are different responsibilities.
* Replace PB Kernel and residual SimpleSig alongside row/AST entrypoints. Add positive/negative tree-shape and consumer tests, not only set-count or catalog-decode tests.
* Cover FastScanFilter and vec_group_checker, not just projection/selection. Execute origin assertions for one-shot helpers, fold/default/generated/DML/join/aggregate/window paths and error paths.
* Characterize AST demand differences, native swallowed-child errors, literal decoder differences, admitted-but-None date signatures, and mixed-result metadata **before** claiming preservation or intentionally correcting them.
* Prepare once per execution/worker; rebind parameters/context/rows without caching statement values or mutable metadata. Preserve first-error/warning order, warning-before-error retention, caps, TryFold rollback and one request drain.
* Move pure subkernels of mixed RAND/clock/schema/password families, leaving only narrow host services; a generic recursive host evaluator is not a compatibility boundary.
* Full overload × type × collation × context matrix, TiKV parity, dynamic execution origin, successful native deletion, deep lazy/caching/parallel/selection stress, and performance remain unresolved for every family here. Test-module links are lexical navigation only, never positive/full-coverage proof.

## Evidence regeneration / validation

From any cwd:

```bash
python3 -B /home/agent/tidb/expression-unification/evidence/collect-coverage-baseline.py
python3 -B /home/agent/tidb/expression-unification/evidence/collect-coverage-baseline.py --check
```

Only the authorized JSON is generated; the notes remain reviewed prose. `--check` regenerates in memory and compares deterministically without writing. `-B` prevents incidental Python bytecode artifacts. The initial source parser hit an inline-test/raw-string balancing error, was corrected to lex Rust strings/comments and remove only balanced test modules, and regeneration subsequently succeeded. This was an inventory-tool failure, not a product test.

Final validation includes JSON parse/count/ID/link checks, exact PB set assertions (129/218/102/27/116/245), all 308 registry names accounted for, 281 unique family IDs, 245 counted rows, 35 host exclusions, one stub, 12 separately classified registry/syntax rows, and deterministic regeneration. No product code, build/Cargo state, main-plan edits or other agent outputs belong to this delivery.
