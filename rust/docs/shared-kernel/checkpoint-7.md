# Shared kernel checkpoint 7 — review corrections

Review of checkpoint6 found two correctness gaps. This revision fixes both;
minimum M1 remains submitted for review, not full compatibility or integration.
Previous fixed pair: TiDB975f35d7a341d1fc5abd915c46d030832465bf8a and
TiKV8b59ab429a3e48662f8781b70d9727da9afdfd0a. Original bases remain364aef2/548812e.

## Changes and original-entry evidence

SETVAR preserved the original literal Datum, but shared compilation erased Bytes
or non-default String collation before writing a default String. Compilation now
rejects those direct literal carriers at SETVAR before byte encoding. Nested
SETVAR reaches the same check. Default String and NULL remain accepted; tests
compare actual session Datums. String IF remains outside admission, with a
composition rejection assertion; it was not an additional executable bypass.
The check is local to SETVAR, preserving byte inputs to transforming functions.

The extracted Decimal multiply omitted the original checked-i128 branch. That
branch preserves hidden coefficient digits; the word-based branch rounds them.
For `(8 / 7) * 1`, both displayed1.1429, but candidate coefficient1142900000
was wrong versus original1142857142, with storage_scale9/result_scale4.
Following `/ 100` exposed0.01142900 versus original0.01142857. Both Scalar and
Vector reproduced the discrepancy. The shared kernel now retains the original
checked-i128 branch, scale limits, sign and storage behavior. Regression inputs
also exercise factors−1,0 and i64::MAX and compare sign, full coefficient,
storage/result scales, declared shape and displayed value after both operations.
Existing192 Decimal boundary operation pairs continue passing.

## Validation and provenance

From tidb/rust, use ../../tools/cargo-tidb with --locked --offline:

- test -p tidb-expr --features shared-kernel --lib shared_kernel_ -- --nocapture
- test --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr --lib -- --nocapture
- clippy --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr -p tidb_query_executors --lib

Final native31 passed/0 failed (1323 filtered); SDK expression library429
passed/0 failed. Scoped Clippy completed with warnings. Root make lint, formatting
and diff checks pass. Official make clippy remains incomplete from checkpoint6;
it was not rerun or counted as passing. No SQL/performance or full-native test run.

Raw logs remain in Engineer evidence/shared-kernel-refactor. The first SETVAR
attempt had a test helper-name compilation error after a wrong formatter path;
checkpoint7-setvar-before-2.log captures the actual four-case admission failure.
checkpoint7-native-after.log passed30 tests before adding the Decimal regression;
checkpoint7-decimal-before.log captures the actual original/candidate parts and
visible-result failures. Final logs are checkpoint7-native-final.log,
checkpoint7-sdk-final.log and checkpoint7-clippy.log. Earlier failures are retained.

Final reruns were bracketed by the same5525-source-input snapshot, digest
2116f19b78a8ced4eb84b2ccad052c0191320560054a83f89c7026323a2498f1.
Test inputs are committed in shared_kernel.rs. The receipt records source/log
hashes; verify from the tidb root with:

    python3 rust/docs/shared-kernel/verify-checkpoint-6.py --tidb . --tikv ../tikv --receipt rust/docs/shared-kernel/checkpoint-7-validation.json

Add --evidence /path/to/evidence/shared-kernel-refactor to verify preserved logs.
This checks inputs, not independent compilation or a new original-dependency run.
Checkpoint6 receipts and report remain historical evidence for their fixed pair.
The default route and all other previously disclosed admission limits remain.

## Complete additions/deletions

The following includes both repositories, reused algorithms, tests, build files,
reports and audit scripts. Old default algorithms remain; no deletion credit.

| Reference | Production + / - | Tests + / - | Build + / - | Docs + / - |
| --- | --- | --- | --- | --- |
| Original bases |3539 /58|2269 /4|1866 /91|8521 /0|
| Checkpoint6 |30 /0|142 /0|0 /0|140 /1|

Original source+tests: +5808/−62, net5746, additions+deletions5870. Original all files: +16195/−153, net16042, additions+deletions16348. This revision: +312/−1, net311, additions+deletions313.
