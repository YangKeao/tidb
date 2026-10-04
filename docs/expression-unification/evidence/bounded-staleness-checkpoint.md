# TIDB_BOUNDED_STALENESS: computed demand and raw SafeTS clamping

Checkpoint **bounded-staleness-90**, after **decimal-policy-89**. Functional227/245, strict0, remaining18. All226 prior family objects are unchanged; only this frozen family is added.

## Native deletion and entry boundaries

`time_fn/mod.rs::tidb_bounded_staleness` is now a thin call to `tikv/bounded_staleness.rs`. Its invalid-zero/range/getter-demand/clamp/metadata selector body is deleted. TiKV `native_bounded_staleness.rs` owns those decisions.

AST and typed callers still evaluate every argument, then perform both original datetime casts left-to-right before reaching this family. NULL on the left does not skip the right cast. Arity remains exactly2; a non-Time pair with any actual NULL retains its original masking precedence, while a non-NULL wrong type reports the original error.

Actual NULL uses the existing `DateDiffNullNative` Int(None) witness. A private preparation-path marker separates that reply from Head: absent/malformed Head output cannot silently become NULL. No fake Time, comparison result or precomputed native answer is supplied.

Existing PB/legacy admission is empty and remains closed. No storage provider, clock query or timezone conversion is introduced.

## Two closed SDK stages

- **Head:** Values/Bytes2/1call/OwnBytes. Both inputs are actual raw Time identities. A strict one-byte report selects InvalidLeft, InvalidRight, RangeNull or NeedSafe. The SDK checks left month/day-zero first, then right, then raw core order. Year0, other invalid calendar fields and raw FSP values do not gain new validation.
- **Finish:** Values/Bytes3/1call/OwnBytes. Input is the original endpoints plus the actual optional SafeTS. Endpoints must satisfy the shared pure HeadNeedSafe classifier; SafeTS receives only structural Time validation, not a new invalid-zero check. Strict less/greater clamps to an endpoint, equality keeps SafeTS itself, and absent SafeTS selects the actual left endpoint.

Finish invokes shared metadata setters for DateTime/FSP3. It does **not** round/truncate microseconds, discard low bits or reconstruct through chrono/Unix time. Core comparison ignores the reserved low4bits; the selected value preserves them.

The bridge only interprets the report. InvalidLeft/Right formats that original endpoint and invokes the original `handle_truncate` authority, then projects computed NULL if the handler permits it. RangeNull projects NULL without a warning. Only NeedSafe reads the **original** `bounded_staleness_safe_time` once inside the active guard, then dispatches Finish through the selected scope. SafeTS is already timezone-adjusted by its provider contract.

Zero Head capacity fails before this family's new warning or SafeTS read. Earlier datetime-cast warnings/errors are a separate boundary; the entire expression is not claimed warning-free. Getter/warning panic poisons the scope. Default NoColumns and no-owner operation remain supported.

## Budget and dispatch evidence

Head's report is1byte; Finish returns the existing11byte Time identity with kind1/FSP3. Ready/direct-ready paths use the same producers for output bounds and still invoke the real kernels. Actual endpoint and SafeTS capacities remain charged, including a large SafeTS input that would be clamped away. The existing row metadata floor/postflight is unchanged.

No carrier, identity codec, role, general driver or compiler rewrite is added. Planning may repeat pure producer work; performance, zero-copy and physical-memory bounds are not claimed.

## Core verification

[Exact commands/counts/times/hashes](../logs/bounded-staleness-summary.txt); [manifest](../checkpoint.json).

Six new tests pass on first matching execution: one core, two local admission/budget, one native entry-preparation, one bridge/lifecycle and one SQL test. The native-root gate also executes the original SafeTS source test. Fixed cases cover first-invalid priority, wrong-type/NULL masking, original casting/getter/warning order, raw FSP/calendar domains, before/inside/equal/after/absent SafeTS, hidden microseconds/lowbits, malformed reports, capacity refusal, poisoning and default NoColumns.

SQL26SELECT=6cases×2vector×2pool=24direct plus2positive filters. The four non-NULL endpoint cases produce8new Head-root zero-slot refusals; the two NULL cases produce4existing NULL-witness refusals, recorded separately. Stored Time/NULL arguments take the original no-op casts, so there is no earlier caster/Compare worker substituting for Head proof. Metadata remains Datetime23/3/binary;123456 microseconds display as `.123` but remain123456 in the raw value.

**No current ordinary SQL context provides a production SafeTS override.** Thus successful SQL values prove None→lower-bound fallback. Nondefault SafeTS clamping/getter counts are direct SDK/bridge/source-test evidence, not SQL storage-integration evidence.

Seven locked nonzero single-threaded launches:5green,2only-old full RED. CPP core1/local343+1ignored; native bounded3/gateway196+1ignored; SQL1 pass. Full expression1600/4old/94ignored and unistore219/1old/13ignored remain RED. No new failure, compile failure, oracle correction, zero-match, interruption or fixture recording.

Entire failure sections match R92 after only numeric panic-heading thread IDs are normalized: expression `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, unistore `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:8CPP/6native Rust files,2new modules;206CPP/373native original test bodies byte-identical,3CPP/3native new tests. Pinned formatting/diff checks pass; E independently reviewed production without a blocker. Agent-doc changes are descriptive, not new policy. No Cargo/lock, Go/Bazel/generated or `compile.rs` changes; unrelated untracked BUILD excluded.

## Remaining work and IN investigation

[Remaining acceptance](remaining-acceptance.md):5core,7ordinary pending,6complex candidates. Overall goal remains active; threshold attainment does not close M0–M6.

IN was only investigated. AST/typed generic exhaust comparisons, ready-values/legacy stop at matches, typed temporal/JSON casts everything first, and native string-cache/probe policy also needs migration. The old `1 NOT IN (1,2)` observation fixes3facades (Eq,Eq,NOT); a new real reducer needs explicitly authorized mechanical receipt adjustment, not hidden calls, removed comparisons or changed SQL expectations. No IN code or old test was changed.

Known CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps and full-suite failures remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are unverified. No goal-completion or PR-readiness claim.
