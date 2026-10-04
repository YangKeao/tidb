# INTERVAL eager and lazy runtime

**interval-runtime-101 / R104**, after [extrema-runtime-100](extrema-runtime-checkpoint.md).
Functional **235/245 (95.92%)**, strict0, remaining10. Only `interval` is appended;234 prior family objects and all partial/type records are unchanged.

## Shared execution

`native_interval.rs` owns classification and search through three Values/OwnBytes/one-call profiles:

| Profile | Actual input | Successful reports |
|---|---|---|
| IntervalEagerHeadNative | Complete nullable identity list | Index, sentinel error, or request real target0 |
| IntervalLazyHeadNative | Per-argument optional evaluation type and full flags | Request integer/real target0 |
| IntervalStepNative | Whole SDK continuation plus actual cast identity or NULL | Index or next requested index/cast |

Successful replies are always present: a NULL target yields integer -1, not a NULL reply. Existing minimum arity2 is retained. No carrier, compile limit, PB/legacy admission or specialized vector kernel is added.

`compare2.rs` deletes its classification, conversion loops and both searches. `tikv/interval.rs` only actuates SDK requests, using original `cast_arg_as_int` with complete source FieldType or `to_f64_with_mysql_string`, then materializes the returned index. Integer comparisons reuse existing SDK signed/unsigned comparers. No host search, nullable/domain answer or reconstructed continuation remains.

### Distinct policies retained

- Eager AST/ready-values: evaluate children first; scan all sentinels before target NULL. Actual Int/UInt/NULL selects exact integer mode. Real mode converts the target and every non-NULL boundary before searching, so even an apparently unused suffix can warn or fail.
- Lazy typed/SQL: actual optional type/flags metadata selects integer/real and nullable-linear/NOT_NULL-binary. SDK requests only visited children. Missing type is not assumed integer. Casts and warnings retain the original caller context.
- Eager nonnullable search uses original `partition_point(boundary <= target)`; lazy binary uses `target < boundary`, otherwise moves right. These are deliberately different for NaN. Declared NOT_NULL does not reject an actual NULL boundary or restart a linear search.
- Lazy **Head now admits before `eval(0)`**. Resource refusal can therefore precede a child error. After admission, visited child/cast errors and warning order remain unchanged. This is not a claim that all request-root/default-NoColumns propagation is closed.

Compact eager state stores NULL markers and only converted real payload bits. Skipping long NULL runs adds no placeholder list. Retained reply bound is checked input lengths plus256; actual capacities are also checked. Final temporary materialization used by the original partition search is not a physical/peak-memory guarantee.

## Validation

Six locked serial Cargo launches all pass on first execution: SDK core1/local353+1ignored; native interval9/gateway196+1ignored/compare2 module13; session interval-filter11. The session filter includes unrelated interval-named tests, not eleven function-specific INTERVAL tests.

Five new tests cover SDK semantics, all three direct roots, native scope/demand and SQL.139CPP/390native original test bodies are byte-identical. The old constant-fold warning regression, unreachable-warning tests and original INTERVAL cross-tier rows pass. SevenCPP/six native Rust files, two new modules; pinned formatting and diff checks pass.

The new SQL test executes **32 SELECTs**: eight stored-operand templates × two vector settings × pool1/0. Sixteen positive results cover exact mixed integers above2^63, nullable boundaries, real search and NULL target -1. Identical real values with different nullability metadata either skip bad text quietly or read it and emit exactly one1292 warning. Sixteen refusals isolate Head only, without nested casts; later-root and callback-count claims come from direct/native tests, not these SQL refusals. Result metadata is signed/binary LongLong20/0.

[Exact commands, counts and hashes](../logs/interval-runtime-summary.txt), [manifest](../checkpoint.json), [ledger](../migration-progress.json).

## Remaining work

[Two core, two ordinary and six complex candidates](remaining-acceptance.md) remain—not approved blanket exceptions. Request-root/default-NoColumns/liveDAG and final acceptance remain open. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical heap/OOM/allocator/dual-tzdata/whole Go-package/PR readiness were not verified. Historical R100 four expression and one unistore failures remain visible.
