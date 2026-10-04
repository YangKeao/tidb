# GREATEST/LEAST staged runtime

**extrema-runtime-100 / R103**, after **extrema-policy-99**. Functional **234/245 (95.51%)**, strict0, remaining11. Only `greatest` and `least` are added;232 previous family objects and all prior partial/type records are unchanged.

## Ownership and execution

Eight SDK profiles in `native_extremum.rs` reuse [the shared policy services](extrema-policy-checkpoint.md) and cover all five native domains. `compare2.rs` now delegates through `tikv/extremum.rs`; its remaining reducers and string-as-time conversion are deleted. Planner signature/return metadata and existing distinct CPP wire policies are unchanged.

| Profile | Actual arguments / role |
|---|---|
| Head | Complete nullable identity-list packet, Ordering, effective collator tag / Values |
| Numeric | Whole SDK state and actual comparison identity or NULL / Values |
| Time | State and actual requested datetime-cast identity or NULL / Values |
| Vector | State and actual converted vector identity / Values |
| String | State and actual prepared bytes / Values |
| TimeText | State and actual coerced UTF8 text or NULL / Values |
| TimeContext | Whole state, independently read mode flag and actual full timezone / existing TemporalText |
| Finish | State and actual selected original/Real/Decimal identity / Values |

All are one-call OwnBytes recipes. No new carrier, compile/factory limit, PB/legacy admission or specialized vector kernel is introduced. Role gates and report subsets are closed per profile. Temporal context reuses the existing metadata binding, zone-capacity accounting and cleanup guard.

SDK reports own domain selection, winner indices, next conversion/comparison demand, temporal parsing/fallback and result promotion. Native code performs only requested original generic preparation and result materialization. Even a selected original value passes its real identity through Finish; it is not returned by an unmetered host clone.

Numeric comparison retains historical NoColumns semantics and fixed division precision4 while using the selected execution authority via `scope.with_columns(&NoColumns, ...)`. Statement casts/getters retain the original context. StringAsTime obtains text before separately reading modes then timezone; parse failure retains original text quietly. RetagString uses the original default-string constructor, not the comparison collation. Raw Time/FSP, vector NaN bits, Decimal scale, first ties and original eager children/global NULL boundaries remain distinct.

The head resolves the actual effective collator, including global legacy-mode Binary/NoPad behavior. Global mode tests restore the flag/defaults and run serially; they do not claim concurrent isolation.

## Resource and representation contracts

The checked retained-report bound is input byte lengths plus512; a pending Decimal precision finish additionally accounts for positive signed-scale expansion. All actual input capacities and fresh output capacity are checked separately. Zone heap capacity is included. This is not a physical-memory or temporary-allocation guarantee.

Head identity framing is representation-only, without premature UTF8/SQL-type/range conversion. Vector identity payload is raw LE f32 elements **without a storage dimension prefix**. State is always the whole SDK-computed report, never reconstructed by the native bridge.

## Validation history

Initial CPP core/local gates passed, but native entry testing exposed a real vector port bug: two new tests and an existing vector test failed with InvalidBatch/Invoke. The new SDK decoder mistakenly used the length-prefixed storage codec on an identity payload; its new direct fixtures repeated the same mistake. The production consumer was corrected to raw-element reconstruction, and only new SDK/direct-worker fixtures were corrected from the representation contract. Old native tests and expected values were unchanged. The failed native run remains fail-before evidence.

Ten locked serial launches: nine green and one retained regression run. Final core2/local352+1ignored, native entry6/gateway196+1ignored, original cross-tier1, SQL2 and original SQL6 pass. [Exact commands and hashes](../logs/extrema-runtime-summary.txt), [manifest](../checkpoint.json).

Five new tests cover all eight actual roots, closed stage/role checks, pre-invocation capacity/instruction refusals with reuse, zone cleanup, selected-scope/NoColumns separation and original getter demand.151CPP/388native original test bodies are byte-identical. NineCPP/six native Rust files, two new modules; formatting and diff checks pass.

The new SQL test has **28 SELECTs** across seven stored-operand templates, two vector settings and pool1/0. Fourteen positive results cover every domain, quiet invalid-time fallback and NULL; fourteen zero-slot probes isolate the new Head root without nested CAST. They do not claim SQL isolation of later worker roots. Values, declared versus winner scale, effective collation and metadata are pinned. The SQL gate also reruns R102's four SELECTs, and six original greatest SQL tests pass. No SQL NaN or unsupported vector-cast admission is invented.

## Remaining acceptance

This step does not close request-root/default-NoColumns/liveDAG ownership or final acceptance. Full workspace/lint/dev/release/exhaustive/TiFlash/FIPS/performance/physical heap/OOM/allocator/dual-tzdata/whole Go-package/PR readiness remain unverified. Prior full-expression/unistore failures remain visible in [remaining acceptance](remaining-acceptance.md).
