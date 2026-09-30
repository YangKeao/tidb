# Four further unary families — shared-text-four-08

Functional delegation/native-deletion progress:10/245 (target221). Added CRC32, REVERSE, CHAR_LENGTH/CHARACTER_LENGTH and QUOTE, including required binary/UTF-8 variants. TiKV adds six closed operations to the existing worker/driver and owned Int/Bytes results. One root/pool remains; no native fallback or extra per-family pool.

Frontend compatibility retained:

- REVERSE text and main/PB CHAR_LENGTH use existing Go per-invalid-byte normalization before strict UTF-8 kernels. Binary variants retain raw bytes.
- QUOTE keeps its original Rust lossy normalization, including binary input. TiKV owns escaping and NULL→Some("NULL"); no native NULL-result algorithm remains.
- CRC32 keeps byte coercion/Auto charset and its raw native UInt result, after checking the official nonnegative signed checksum fits u32. Existing SQL inference is signed LongLong and stays unchanged.
- CHAR_LENGTH removes counters in public BuiltStringLength, typed PB and unistore's legacy SimpleSig. PB retains child-demand order and other functions' NULL behavior. Legacy CharLengthUtf8 intentionally keeps Rust lossy grouping before calling the shared helper; its truncated E2 82 input still counts1, while main Go normalization counts2. This difference was pre-existing, not silently unified.

## Actual commands/results

From `tikv/`, via `../tools/cargo-tikv`:

- `test --locked -p tidb_query_expr --lib local:: -- --test-threads=1`:180 passed/1 ignored/466 filtered.
- `test --locked -p tidb_query_expr --lib test_evaluated_bytes_rejects_same_carrier_operation_and_kernel_drift -- --test-threads=1`:1 passed/646 filtered.

From `tidb/rust/`, via `../../tools/cargo-tidb`:

- `test --locked -p tidb-expr --lib next_bytes_dispatch_ -- --test-threads=1`:2 passed/1472 filtered. Includes native UInt, normalization/packing and real PB C4/NULL/zero-slot failure.
- `test --locked -p tidb-session --lib tests_core::lifecycle::evaluated_ascii_ -- --test-threads=1`:19 passed/2078 filtered after a newly authored fixture metadata correction.
- `test --locked -p tidb-unistore --lib legacy_char_length_keeps_rust_lossy_grouping_before_shared_kernel -- --test-threads=1`:1 passed/182 filtered.
- `test --locked -p tidb-expr --lib -- --test-threads=1`:1376 passed/4 unchanged baseline failures/94 ignored,1474 discovered, exit101. Complete failure blocks equal07 after only thread-ID normalization.

Initial SQL run:18 passed/1 failed. All payloads passed; the new fixture incorrectly assumed an unsigned SQL column from the raw CRC32 UInt result. Unchanged rewriter/result_type.rs:1234,1543 returns signed int() for CRC32, and chunk materialization follows that declaration. Only the new expected metadata/Datum tag was corrected; no old expectation, payload or production inference changed. Raw evaluator UInt remains explicitly tested. The second run also passes QUOTE(NULL)'s String+Binary metadata assertion.

Real serial one-slot SQL mixes binary/text columns, multibyte reversal/counts, high-bit checksum3421780262 and QUOTE NULL/empty/escapes/invalid FF. Zero-slot tests reject14 corresponding dynamic NULL/non-NULL calls with typed PoolResource/Pool and1105/HY000, instead of native replay.

Frozen baseline CHAR_LENGTH includes CharLength/CharLengthUtf8 PB+unistore signatures; those are connected, with legacy public helper additionally covered. Other three families have no implemented PB/unistore signatures to invent for credit. JSON_SUM_CRC32 is a distinct, still-unmigrated family.

Not claimed: all unistore/session/workspace suites, network end-to-end, make lint, allocation remeasurement, factory peak/physical OOM bounds or release performance. Strict final-audited count remains separate. Architecture-index review changes mappings only, references existing paths and scopes Rust commands; no policy/build workflow changes. Next: HEX/BIN/LEFT/RIGHT/REPLACE through fixed typed ready arguments, not an arbitrary program API.
