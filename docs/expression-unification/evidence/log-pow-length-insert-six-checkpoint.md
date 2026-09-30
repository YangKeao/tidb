# Logarithms, POW, uncompressed length and INSERT — log-pow-length-insert-six-20

**Original stage plus validated casing repair: nineteen separate receipts.**

The corrected functional delegation/native-deletion ledger is restored from the temporary **63/245** to **65/245**; final acceptance remains **0/245**. The original round-20 slice adds six families through seven operations: LN, LOG (both arities), LOG2, POW/POWER, UNCOMPRESSED_LENGTH and INSERT. The casing supplement restores two previously credited families rather than adding new family credit. Parent reviewed and precisely restaged the same eleven TiDB and ten TiKV source files after the repairs.

## Historical coverage correction and completed casing supplement

Read-only follow-up found independent LOWER/UPPER algorithms and NULL bypasses in unistore's legacy `eval_bytes` branch. Prior round-18/19 complete-coverage claims had missed that surface and were incorrect on this point. The proposed 65/245 milestone was withheld at63 while the gap was repaired; the missed surface is closed in round20. Earlier commits and original receipts are not rewritten to pretend that coverage was complete at the time.

Dedicated LowerAsciiNative/UpperAsciiNative operations now own legacy ASCII conversion, with one thin standard-byte wrapper per direction and no second mapping table or casing layer. ASCII high bytes remain unchanged. Existing wire binary LOWER/UPPER remain no-ops. The legacy native case loops and NULL bypasses are removed: RawCaseFunction routes all four signatures, including NULL, through the closed value driver. UTF8 reuses the existing private LowerUtf8Ready/UpperUtf8Ready operations after the original Rust-lossy preparation; incomplete E2 82 remains one U+FFFD, not Go's two per-invalid-byte replacements.

Two legacy-case tests cover four signatures, ASCII/high-byte preservation, Go-simple Unicode casing, grouped invalid UTF8, NULL admission and typed infrastructure failures through direct bytes and nested int/bytes casts. These repairs are evidenced by the eight separately named casing-repair receipts below, **not by the eleven original-stage logs**. Restoring the two withheld credits yields65, not67.

## Original round-20 implementation and policies

LN and one-argument LOG share Ln; two-argument LOG is included, not left native. Binary IEEE arguments require two Values, except POW may use one Undemanded operand only opposite a real NULL. The physical representative is non-NULL +0 bits, never fabricated SQL NULL. PB POW preserves NULL suppression of the opposite operand's coercion; the native tuple path still coerces both. Frontend logarithm diagnostics retain their original timing: warning3020 survives a subsequent pool refusal. For successful invocations, actual invalid-domain IEEE inputs reach C computation before the native result is masked to NULL. Wire math policy is preserved.

Parent separately found the previously overlooked unistore `SimpleSig::Pow` powf algorithm and early NULL bypass. Its purpose-specific RawPowReadyArgs bridge now reaches C4 even for left NULL without evaluating the right expression; a right NULL follows evaluation of the left. Legacy NaN/Inf remain raw rather than being passed through native finite_float policy. Two dedicated tests cover raw values, operand demand/NULL admission and typed pool failure propagation through real/int/bytes/comparison consumers.

UNCOMPRESSED_LENGTH has one LE32/empty/short core. The private quiet entry leaves native short-input warning1259 at the frontend while keeping the C4 boundary warning-free. NULL remains NULL; empty and lengths1–4 return0, with warnings only for the nonempty short inputs. All 32 header bits survive; neither zlib stream validation nor advertised-length packet rejection is introduced.

INSERT shares range/splice logic: binary uses the real wire byte operation; native UTF8 uses character indices in the normalized source but does not decode or lose raw replacement bytes. Existing wire error/strict policies remain. Packet policy runs only after an actual successful non-NULL result; NULL never reads the limit. Four-column admission is restricted to four PAD plus two INSERT operations, with max_nodes5 for those operations and4 otherwise, depth3 unchanged. The driver, pool and general graph are not generalized.

## SQL coverage verified from source

The main lifecycle SELECT explicitly checks **three rows by eleven columns**: six Real math results, two integer lengths and three INSERT results. It checks POW/POWER equality, numeric widths/flags, INSERT text/binary collation metadata and raw result bytes. A separate two-column latin1 fixture retains raw FF as a text-signature replacement, yielding bytes E4 B8 AD FF without changing its text tag.

Five logarithm domain cases return NULL with3020; POW(-2,0.5) returns1690/22003. Four short headers of lengths1–4 produce0 with1259. A 1026-byte/342-character replacement creates a real1028-byte INSERT result before packet1024 produces1301/NULL; the corresponding NULL-source row has no packet warning. Fourteen direct zero-slot refusals preserve typed PoolResource/Pool1105/HY000, including preexisting3020/1259 warnings. The would-overflow INSERT refuses before computation and emits no pre-1301; returned evaluation-origin1105 is not a warning-buffer Error row.

## Eleven original validation receipts and corrections

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/log-pow-length-insert-six-summary.txt) retain all nineteen receipts in separate original-stage and casing-repair sections. The following eleven receipts precede the casing repair.

- TiKV local: 219 passed/1 ignored/469 filtered; official math46/643 filtered, encryption8/681 filtered, string63/626 filtered.
- Initial dispatch: compile exit101, two E0308 errors, zero tests; the initial chain stopped. The new raw-POW helper attempted Option<f64> although the existing driver returns Datum. Parent changed the helper to Real/Null Datum and unistore to an explicit match/InvalidResult, without generalizing the driver.
- First dispatch retry: 3 passed/1 failed/1504 filtered. The new INSERT overflow instrumentation expected one packet getter call; the existing result comparison reads it once and the warning formatter reads it again. Parent changed only that new assertion to2 and its comment. This assertion repair changed neither runtime nor preexisting expected values.
- Second dispatch retry: 4 passed/1504 filtered. Legacy POW: 2 passed/187 filtered. Session lifecycle/SQL: 43 passed/2078 filtered.
- Full expression: 1410 passed/4 failures/94 ignored, 1508 discovered, exit101. Full unistore: 175 passed/1 failure/13 ignored, 189 discovered, exit101. Parent ran both independently in the same shell; expression exit101 did not skip unistore.

Parent compared complete failure blocks: the four expression blocks match round19, and the unistore block matches the round15 retry, after only thread-ID normalization. Both full suites remain non-green. Original logs remain intact and must not be relabeled as post-casing-repair runs.

## Eight casing-repair receipts and the final source cut

The `log-pow-length-insert-six-casing-repair-*` logs are the supplemental evidence, separately preserved from the original eleven receipts:

- TiKV local: 220 passed/1 ignored/469 filtered, 0.18s; official string: 63 passed/627 filtered, 0.02s.
- Legacy-case tests: 2 passed/189 filtered, 0.00s. This completed before the subsequent dispatch compilation failure.
- Repair-stage initial dispatch: compile exit101, one E0004 for two omitted ASCII enum variants in an existing test helper, zero tests; SQL was not reached. Parent added only an explicit two-variant test-helper panic arm documenting the required raw legacy entry. No wildcard, false SQL LOWER/UPPER mapping, runtime change or preexisting expected-value change was introduced for this helper repair.
- Repair dispatch retry: 4 passed/1504 filtered, 0.00s; SQL lifecycle: 43 passed/2078 filtered, 0.58s.
- Post-repair full expression: 1410 passed/4 existing failures/94 ignored, 1508 discovered, 10.32s, exit101.
- Post-repair full unistore: 177 passed/1 existing failure/13 ignored, 191 discovered, 2.98s, exit101.

Parent again compared complete post-repair failure blocks against expression round19 and unistore round15 retry: identical after only thread-ID normalization. Both full suites ran and remain non-green. The validated casing repair restores the two withheld credits at65/245 without new family credit; parent precisely restaged the same eleven TiDB and ten TiKV source files. Nineteen receipts are retained, including all compilation/test failures and their subsequent runs.

## Scope and unresolved gates

No new PB/unistore admission was added by either phase. Existing legacy POW and casing coverage was repaired, not counted as newly admitted functions. Preexisting expected values were unchanged; the new getter assertion and test-helper exhaustiveness were corrected as documented. SUBSTRING's original algorithm remains unchanged, unmigrated and uncredited. GB foundation residuals and the subsequent complete search batch remain open. Complete operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, release performance, full-workspace validation, make lint and final acceptance remain unverified. Final acceptance is0/245; this checkpoint is not PR-ready.
