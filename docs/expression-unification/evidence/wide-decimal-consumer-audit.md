# Wide Decimal consumer allocation audit

## Status, provenance, and scope

This is an evidence record and narrow design proposal, **not a second project plan, implementation authorization, or publication approval**. General wide Decimal publication remains **CLOSED**.

- Audited checkout: `/home/agent/tidb/expression-unification/tikv`.
- Required context read first: `AGENTS.md`, `doc/maintenance-guides/README.md`, `repo-overview.md`, then `src/coprocessor.md`.
- Initial datatype **374** and expression **575** passing tests were reported by the parent/user. This auditor did not execute them or establish a new baseline. Concurrent producer work can supersede these counts.
- Initial audit findings were **source-derived**. Parent subsequently reproduced unnecessary **bounded** caller allocations (§9); the huge-wide witnesses and remaining conversion/true-error hazards are still source-derived. Reading existing test assertions is not executing those tests.
- No product files, Cargo files, allocator configuration, dependencies, or old-tree files were changed. No tests, builds, links, or probe executions were performed by this auditor.
- Read-only artifact inspection used file metadata and `nm`; that establishes symbol availability, not successful linking or runtime counter behavior. Installed pinned compiler source under the old tree was read only.
- Parent authorized exactly two new private experiment files: this document and `../tools/decimal-diagnostic-alloc-probe.rs`. The auditor handed them back unexecuted; parent later linked and ran the probe as detailed in §9. The identical-source, matched-test-profile post-fix GREEN is recorded in §10.
- Source line references are the inspected snapshot. Concurrent work, especially appending Decimal tests, can move subsequent line numbers; named functions identify the corresponding sites.

The representation is owning `SmallVec`, with private Grow workers and independent full-u32 storage/result scales. A valid private wide state is not automatically an admitted public evaluator input. These findings explain why publication remains closed, not that existing wire inputs can already construct every wide state.

Paths below are relative to the TiKV checkout unless stated otherwise. Abbreviations:

- **decimal.rs**: `components/tidb_query_datatype/src/codec/mysql/decimal.rs`
- **arithmetic.rs**: `components/tidb_query_expr/src/impl_arithmetic.rs`
- **convert.rs**: `components/tidb_query_datatype/src/codec/convert.rs`
- **datum.rs**: `components/tidb_query_datatype/src/codec/datum.rs`
- **cast.rs**: `components/tidb_query_expr/src/impl_cast.rs`
- **time.rs**: `components/tidb_query_datatype/src/codec/mysql/time/mod.rs`
- **interval.rs**: `components/tidb_query_datatype/src/codec/mysql/time/interval.rs`
- **error.rs**: `components/tidb_query_datatype/src/codec/error.rs`
- **ctx.rs**: `components/tidb_query_datatype/src/expr/ctx.rs`

## 1. Valid-wide state and the allocation mechanism

A private checked import can represent:

```text
int_digits = 1
storage_frac = 0
result_frac = u32::MAX
negative = false
words = [1]
```

`Decimal::try_from_words`, decimal.rs:1662–1719, checks storage extent, word range, integer/fractional partial-word shape, and independently imports result scale. The above is a tiny valid owner; it is not a malformed target declaration or a request to allocate billions of backing words. The constructor remains private.

`Display`, decimal.rs:3438–3452, chooses legacy rendering only for active extent at most nine words and result scale at most 255. Otherwise it uses `write_result` (2195–2213). When result scale exceeds storage scale, the latter emits padding through `DecimalTextWriter::zeroes` (389–398).

The emitter's 128-byte staging buffer limits temporary staging and permits a rejecting sink to stop early. **It does not bound the output size or work of an accepting `String` sink.** A tiny stored `1` with full-u32 visible scale can request billions of displayed zeroes. No such formatting was executed here.

When result scale is smaller than storage scale, rendering instead rounds through `try_round_exact` (2055–2071) and `try_clone_for_worker` (1929–1934). That allocates scratch for active storage before sink writes. It does not copy arbitrary inactive heap cells. A bounded output sink therefore does not, by itself, establish a total scratch/resource bound.

## 2. Concrete remaining callers

### 2.1 Eager MOD and DIV diagnostics: immediate narrow target

**arithmetic.rs:299–305 and 498–505** construct:

```text
Error::overflow("DECIMAL", format!("({} % {})", lhs, rhs))
Error::overflow("DECIMAL", format!("({} / {})", lhs, rhs))
```

before calling `Res::into_result_with_overflow_err`.

The disposition helper, decimal.rs:66–96, only consumes the supplied error for `Res::Overflow`. `Res::Ok` returns its payload; `Truncated` uses the existing truncation handler. Nevertheless, both successful and truncated arithmetic currently format the original operands first.

`Error::overflow`, error.rs:48–50, then formats another owned message. Merely replacing the inner `format!` with `format_args!` does not defer the enclosing error construction.

For tiny stored `1`, wide result-only metadata, and ordinary rhs `2`:

- MOD retains maximum operand result scale, decimal.rs:3910–3917.
- DIV caps its **result** scale to 30, decimal.rs:3231–3237.
- Both error expressions render **original operands**, so DIV's result-scale cap does not protect this caller.
- A zero divisor instead takes the `None`/division-by-zero branch without these operand diagnostics. Preserve that ordering and behavior.

This is source evidence of unnecessary work even for `Ok`, not a runtime reproduction of an OOM.

### 2.2 True arithmetic errors remain unbounded after laziness

**arithmetic.rs:456–464 and 472–488**, particularly 463 and 485, format original operands after signed/unsigned integer DIV conversion overflows. These paths are already conditional, but their text remains unbounded.

A small-storage source witness is signed lhs `9223372036854775808`, divided by stored rhs `1` carrying result-only scale 4096. The Decimal quotient fits Fixed9; integer conversion rejects it and renders the operands. Unsigned lhs `18446744073709551616` gives the analogous unsigned case. These are proposed witnesses, not executed cases.

Actual `Res::Overflow` in Decimal MOD/DIV will also still render full original operands after the narrow lazy fix. Warning-detail limits do not bound construction: ctx.rs:204–208 receives an already-built `Error` before retaining or dropping its details.

### 2.3 Temporal and interval conversion text

| Source | Behavior and valid-wide hazard |
|---|---|
| cast.rs:1133–1139 | Decimal→Duration calls `to_string()` before parsing. A normal fsp0 target is enough; tiny stored `1` with result-only scale 4096 is a safe moderate analogue. |
| time.rs:1489–1497 | `Time::parse_from_decimal` materializes Display text at 1496 and formats it again on rejection at 1497. Called from cast.rs:1266–1283. An ordinary stored date such as `20240101000000` with visible padding suffices; malformed field metadata is unnecessary. |
| time.rs:1508–1510 | Default Decimal time parsing first follows the numeric path at 800–828, but rejection formats the original Decimal. Stored `1` is an invalid datetime yet a valid Decimal; do not conflate these categories. |
| interval.rs:722–769 | `to_interval_string` eagerly formats at 731. Simple integer units use the `_` arm at 761–766, discard that text, then round/as_i64/render the integer. Stored `1` as interval DAY therefore pays for padding it never uses. |
| interval.rs:740–756 | Composite units consume result text and may allocate again for replacement, prefixes, and sign handling. `Second` also consumes that text. These are not covered by simply deferring unused simple-unit formatting. |

Expression interval entry points include `components/tidb_query_expr/src/impl_time.rs:1063–1069`, also calls at 1107, 1143, and 1177.

For temporal and composite-interval conversions, the string is **semantic parser input**, not just a diagnostic. A bounded diagnostic synopsis must never be substituted into those parsers. New-wide admission/conversion resource policy remains unapproved.

### 2.4 Additional Datum consumers reported by B and source-verified here

**datum.rs:403–417**, specifically 411, implements inherent `Datum::to_string` with `Datum::Dec(ref d) => format!("{}", d)`. This is **RESULT/Display text**, unlike Decimal's storage `ToStringValue` conversion. Returning `Result<String>` does not make the internal `format!` allocation fallible. `into_string` (422–427) reaches it for non-Bytes values.

`Datum` Display at **136–155**, specifically 145, writes `Dec({})` using Decimal Display. `Datum` Debug at **157–160** delegates directly to that Display. Therefore `{datum:?}` is not bounded shape-only diagnostic output.

Mixed-Datum rejection branches format both values:

- `checked_add`: **683**;
- `checked_minus`: **715**;
- `checked_mul`: **736**;
- `checked_rem`: **771**.

`invalid_type!`, `components/tidb_query_datatype/src/codec/mod.rs:5–13`, builds `Error::InvalidDataType(format!(...))`. Thus a rejected direct operation such as `Datum::Dec(valid_wide_one)` with `Datum::I64(2)` can allocate full visible Decimal text before delivering the rejection. The Decimal itself is valid; the mixed operation is unsupported without the relevant coercion. This is not evidence of a malformed Decimal or of all typed SQL paths reaching the mixed branch. For remainder use nonzero rhs; rhs zero has a separate early NULL rule at datum.rs:747–750.

Do not sweep every Datum formatting site into this claim. For example `into_f64` explicitly handles `Datum::Dec` at 441 before its fallback error, and matched Decimal/Decimal arithmetic may use status conversions without formatting operands. The concrete mixed rejection branches and Display/Debug implementation above establish the remaining boundary.

## 3. Distinctions that prevent false findings

### Decimal→integer diagnostics

convert.rs:443–459 eagerly constructs `truncated_wrong_val` too, but first calls `round_decimal_with_ctx` (575–577). For a tiny stored integer, round(0) resets result scale to zero through decimal.rs:2446–2452. Do not label this the same tiny-storage/max-visible-padding hazard. Its source-owner clone and wide storage work are separate resource concerns.

`decimal_as_u64`, convert.rs:581–585, receives Time/Duration-produced values at 480–501, rather than being the generic arbitrary Decimal conversion route.

### Storage String/Bytes conversions: separate fallibility gap

Generic `ConvertTo<String/Bytes>`, convert.rs:93–111, uses `ToStringValue`. Decimal's implementation at decimal.rs:3422–3434 writes **storage** text and uses infallible `String::with_capacity` plus formatting expectations.

Consequences:

- Tiny storage `1`, result scale MAX, yields storage text `"1"` here; it does not request visible MAX padding.
- A Decimal with genuinely wide initialized storage can still require large infallible String allocation.
- Expression `cast_any_as_string`, cast.rs:645–655, materializes before applying its target string handling; a small target does not automatically bound this temporary.
- Fallible/storage-budget work is distinct from the visible-scale Display issue. It remains outside the three-file lazy proposal.
- **Datum::to_string is not this storage conversion**; see section 2.4.

### Decimal→f64 and JSON

Decimal's f64 conversion, decimal.rs:3274–3281, calls `try_storage_text` (2183–2192): count via the storage emitter, reserve fallibly, then parse. Visible padding is ignored. Actual large storage still implies proportional scanning/materialization; this is not a global quota guarantee.

Preserve native storage-value parsing, including infinity and signed underflow zero; do not invent SQL warnings or convert resource failures into numerical overflow. Decimal→JSON, `codec/mysql/json/mod.rs:553–560`, uses this f64 conversion.

### Already bounded logs and malformed targets

Wire encoding logs at decimal.rs:3545–3554 and 3565–3574 already emit bounded scalar shape fields rather than full Decimal Display.

`produce_dec_with_specified_tp`, convert.rs:646–695, formats target `(flen, decimal)` metadata at 665/674, not Decimal operands. Its narrowings at 667/671 require a separate declared-target admission review. Malformed or unadmitted target scales are not witnesses of valid-wide diagnostic amplification.

### Fixed dispositions are not automatically defects

Keep the existing Fixed9 arithmetic and status policy:

- capped quotient count/sentinel: decimal.rs:995–1000;
- Fixed rounding partial selection and `Truncated`: 2400–2411;
- `Res::Ok`/`Truncated`/`Overflow` disposition and context handling;
- legacy multiplication capacity selection that can remove low integer cells.

The maintenance guide explicitly retains these contracts. An exact Grow result is not an oracle that authorizes replacing Fixed with Grow-and-clamp. This audit grants no such replacement.

## 4. Minimal three-file lazy proposal

This is a proposed loan, not permission to edit product code. B currently owns `decimal.rs`; no second concurrent writer is authorized by this document.

### File 1: decimal.rs — compatible lazy sibling, one disposition worker

Preserve these existing signatures:

- public `into_result_with_overflow_err(ctx, Error)`;
- public `into_result(ctx)`;
- private `into_result_impl(ctx, Option<Error>, Option<Error>)` used by existing inline tests.

Add a private worker taking `F: FnOnce() -> Error` and the existing optional truncation error. Move the **single existing match on Res** into it:

- `Ok`: return the original payload, no factory invocation;
- `Truncated`: unchanged `handle_truncate_err`/`handle_truncate(true)` logic, no overflow factory invocation;
- `Overflow`: invoke factory exactly once, then unchanged `ctx.handle_overflow_err`, returning the original payload only when that handler permits it.

The old private helper becomes a thin adapter supplying:

```rust
|| overflow_err.unwrap_or_else(|| Error::overflow("DECIMAL", ""))
```

Add public sibling `into_result_with_overflow_err_lazy<F: FnOnce() -> Error>`, forwarding to that same worker with no custom truncation error. Existing eager public APIs keep their signatures and evaluation behavior. Their already-constructed argument cannot be made lazy retroactively.

No fallible factory/resource policy is needed or authorized in this stage. No duplicated disposition match, new formatting algorithm, or broad caller migration.

### File 2: arithmetic.rs — exactly two closures

At MOD 299–305 and DIV 498–505, switch to the lazy sibling and wrap the **unchanged** `Error::overflow(...format!(...))` expression in a closure.

Keep arithmetic first, the `Some`/`None` branches, division-by-zero handling, default truncation handling, `.map(Some)`, original error text/type, and warning order unchanged. Do not touch true integer-DIV diagnostics at 463/485.

### File 3: interval.rs — format only in text-consuming branches

Remove the unconditional `self.to_string()` at 731:

- Composite arm creates a local string and retains all existing replacement/prefix/sign operations exactly.
- `Second` returns `self.to_string()` unchanged.
- Simple-unit `_` arm retains its exact clone/Fixed round(0, HalfEven)/context disposition/as_i64 pipeline and renders only the final integer.

No numeric or parser substitution, unit broadening, warning reorder, scale reinterpretation, or diagnostic clipping.

### Compatibility coverage and ownership handoff

1. **Old API baseline, not RED:** test-local `Cell`/CountingDisplay shows the eager error argument is evaluated once even for `Res::Ok`. Expecting zero would contradict the old API contract.
2. **New API contract coverage, not prepatch runtime RED:** factory count zero for `Ok` and `Truncated` (strict/warning/ignore), once for `Overflow` (strict/warning, including full warning-detail buffer). Compare exact payloads, error codes/messages and warning prefix/count/details against the eager API under identical flags.
3. **Bounded caller compatibility:** existing MOD tests around arithmetic.rs:846 and DIV tests around 1222, NULL/divzero, signs and precision increment; old bounded error text and warning order. Pure numeric equality already passes the eager code and is not allocation RED.
4. **Interval compatibility:** existing test matrix interval.rs:894–944 covers composite formatting and signs; retain simple units, ties, negatives, zero, and context behavior. Decimal-private moderate fixtures may call the public interval trait from Decimal tests without any export, but value equality alone is not allocation RED.
5. Cover legacy result headers such as 0/30/81/127/128/255 and admitted overcapacity status text where relevant. Existing raw header255 rendering of tiny `1` is `"0"`, not full padding; do not replace it with a different formatter.

Recommended handoff: B explicitly yields a named source/test snapshot before another implementation owner borrows all three files, or B implements the agreed Decimal helper/tests and then yields. Parent remains the serial compile/test coordinator. No loan or build authorization follows merely from this document.

## 5. New-wide policies still requiring parent approval

Laziness eliminates diagnostics that should never have been constructed. It does not resolve genuinely emitted oversized text.

A later bounded diagnostic design should reuse the existing formatting selection/emitter, check envelope accounting, and use a capped/fallibly reserving writer. Never format the entire value and then truncate. Never make Display silently clip. A rejecting Display adapter passed into ordinary `format!` can turn formatting failure into a panic; fallibility must remain explicit.

All old bounded error text, codes, dispositions, and warning order must stay exact, including legacy signed-byte/Fixed30 interpretation and admitted negative-zero status text. Values whose exact diagnostic fits the chosen budget can retain that exact text. Budget approval and proof that it covers old bounded envelopes are still required.

For new-wide diagnostics exceeding the budget, the parent must choose an explicit policy, e.g. an unmistakably marked bounded synopsis preserving SQL disposition, or an outer resource refusal with specified effect timing. **Neither is approved here.** Do not invent silent fallback, zero/NULL substitution, or resource-failure→`Res::Overflow`/SQL1690 conversion.

An output cap does not cover pre-emission rounding scratch. Avoid a counting writer that first walks billions of padding bytes; use checked preflight or early rejection. Full warning buffers do not justify suppressing counts/order or assuming no diagnostic construction.

Temporal, composite-interval, and Datum result-text conversion semantics remain separately blocked. Diagnostic summaries are not parser input. General wide publication remains closed even if all three lazy changes and the bounded probe pass.

## 6. Safe moderate witnesses, not executed

| Witness | What it would establish / limitation |
|---|---|
| Private stored1/result300 or4096 with rhs2 through MOD/DIV | Moderate allocation amplification analogue. Numeric result equality alone is not RED; no new public fixture export. |
| Signed `9223372036854775808` / stored1 with visible4096; unsigned analogue `18446744073709551616` | True diagnostic remains oversized after laziness. Expected resource/synopsis behavior cannot be invented before policy approval. |
| Private stored1/result4096 as interval DAY | Text should be `"1"`; the current throwaway allocation needs instrumentation to become allocation RED. |
| Stored1/result4096→Duration fsp0; stored date/result4096→Time fsp0; stored1/result4096 rejected by default Time parser | Valid Decimal/ordinary targets; conversion or rejection text amplifies result metadata. New bounded semantics require separate approval. |
| `Datum::Dec(private valid-wide one).to_string()` | Result-text materialization, distinct from storage String conversion. |
| Direct mixed `Datum::Dec(private valid-wide one)` plus/minus/mul/rem `Datum::I64(2)` | Existing unsupported mixed-operation rejection formats Decimal; the value itself is valid. |

**Never run MAX-u32 through accepting String/Error/to_string formatting.** No huge formatting is needed to investigate the narrow lazy stage.

## 7. Instrumentation evolution and current private probe

### 7.1 No pre-existing portable caller counter found

The expression scalar test helper, `src/types/test_util.rs`, has no allocation/Decimal-format invocation counter. `RpnFnMeta.fn_ptr` is public (`types/function.rs:90–98`) but returns a `VectorValue`, adding avoidable vector/output noise.

Direct public native entry is available instead: expression `lib.rs:31` exports `impl_arithmetic`; public `ArithmeticOpWithCtx` is at arithmetic.rs:33–37, and `DecimalMod`/`DecimalDivide` implement it. No root-expression evaluation or new export is needed.

Existing `tikv_alloc` jemalloc cumulative thread stats are at `src/jemalloc.rs:118–143,216–230`, with safe registration/cleanup through `tikv_util/src/sys/thread.rs:424–443`. However:

- non-jemalloc stats are no-ops (`tikv_alloc/src/default.rs:48–54`);
- both inspected target fingerprints `tikv_alloc-ff8a4ccbf52629f4` and `tikv_alloc-a1511282d4dbf818` report `features:[]`;
- stats aggregation itself allocates a HashMap and merges trimmed thread names;
- no allocator feature build is authorized.

Therefore this was not an available portable allocation RED seam for the pinned default artifacts.

### 7.2 Rejected standalone second GlobalAlloc approach

A private executable cannot simply install its own Rust `#[global_allocator]`: datatype `src/lib.rs:23` explicitly links `tikv_alloc`, whose `src/lib.rs:130–131` declares the allocator for every backend, including System. A second declaration conflicts. This was source-derived incompatibility, not an observed compiler failure; no compilation was attempted.

### 7.3 Final approved private approach: GNU libc-symbol wrapping

Parent then approved a standalone GNU `--wrap` probe rather than replacing the Rust allocator.

Evidence:

- `tikv_alloc/src/system.rs:5–7` aliases `std::alloc::System`.
- Pinned compiler source `std/src/sys/alloc/unix.rs:14,36,54,83` calls malloc/calloc/realloc/posix_memalign on this Linux target.
- Read-only `nm --undefined-only --extern-only` on both existing allocator rlibs showed those unresolved symbols plus free in allocator objects.
- `/usr/bin/gcc-14` and `/usr/bin/ld.bfd` exist.

`--wrap` redirects unresolved references from statically linked objects. It is not a tracer for all internal calls made by shared libraries and does not establish global peak memory. The confirmed System routes are sufficient to propose this focused probe, contingent on runtime positive controls.

The authorized file `../tools/decimal-diagnostic-alloc-probe.rs` now contains:

- Four ABI-correct `__wrap_*` functions forwarding to external `__real_*`; no `#[global_allocator]`, no dependency changes.
- Private executable-local atomic request counts in order `[malloc, calloc, realloc, posix_memalign]`. Wrappers perform only atomics and delegation; no formatting, locks, TLS initialization, allocation, or policy conversion.
- A gate with unwind cleanup. All assertions/validation, context/operand setup, output interpretation/printing, and destruction of measured return values happen outside the gate.
- Ordinary bounded Decimal operands **1 and 2 only**. A bounded `0.5` value is an outside-gate expected result, not a wide operand.
- Empty-zero control; individual inherited-allocator positive controls for all four routes; bounded diagnostic-formatting positive control. Missing controls mean **INCONCLUSIVE**, not a false zero-allocation pass.
- Untracked warm-up of actual and baseline MOD/DIV/DAY, with `Res::Ok`, exact value/shape and empty warning checks.
- Eight alternating-order pairs for each operation; fixed stack arrays store counts. Require stable actual and baseline vectors across all pairs.

Measured pairs:

| Actual caller | Baseline using the same native implementation |
|---|---|
| `DecimalMod::calc(ctx, &one, &two)` | `(&one % &two)` followed by its existing `Res::into_result(ctx)` |
| `DecimalDivide::calc(ctx, &one, &two)` | `one.div(&two, same_increment)` followed by `Res::into_result(ctx)` |
| `one.to_interval_string(ctx, Day, false, 0)` | Same clone/round(0, HalfEven)/into_result/as_i64/into_result/integer-to-string simple-unit pipeline |

**Do not assert MOD or DIV allocates zero.** Current shared division source at decimal.rs:1215–1216 allocates at least a three-word scratch Vec even for nonzero `1 % 2`; there is no presumed inline-only early return. Both baselines include legitimate arithmetic scratch. DAY includes its legitimate final String allocation on both sides.

Probe classification:

- **Exit 0, PARITY:** exact per-route parity for all three stable pairs; not general resource safety or publication evidence.
- **Exit 1, RED:** controls/semantics/stability passed and at least one actual caller made more allocation requests than its same-native baseline. Only a future execution can establish this observation.
- **Exit 2, INCONCLUSIVE:** controls missing, unexpected status/value/warnings, instability, an unexplained route difference without positive total excess, or unexpected panic. Do not mislabel these as reproduced allocation defects.

The probe was **authored only**. No counter output, runtime RED, link success, or after-patch GREEN is claimed in this record.

## 8. Parent-only candidate link/run handoff

Freeze and record the current artifact identities/hashes first. The following candidates were discovered, not proven linked together by this auditor:

- `target-tikv/debug/deps/libtidb_query_datatype-c654d7c6d7b1484d.rlib` (inspected mtime Sep 29 00:27);
- `target-tikv/debug/deps/libtidb_query_expr-61c532d900245edb.rlib` (Sep 29 00:28).

Producer H/D5 or other serialized builds may replace artifacts. Use a matched snapshot, not stale assumed hashes. Preserve the probe source and command across before/after measurements. Parent owns any required compilation authorization and execution.

Candidate standalone link command (**not executed**):

```bash
ROOT=/home/agent/tidb/expression-unification
RUSTC=/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustc
"$RUSTC" --edition=2021 -C opt-level=0 -C lto=off \
  -C linker=/usr/bin/gcc-14 -C link-arg=-fuse-ld=bfd \
  -C link-arg=-Wl,--wrap=malloc \
  -C link-arg=-Wl,--wrap=calloc \
  -C link-arg=-Wl,--wrap=realloc \
  -C link-arg=-Wl,--wrap=posix_memalign \
  -L dependency="$ROOT/target-tikv/debug/deps" \
  --extern tidb_query_datatype="$ROOT/target-tikv/debug/deps/libtidb_query_datatype-c654d7c6d7b1484d.rlib" \
  --extern tidb_query_expr="$ROOT/target-tikv/debug/deps/libtidb_query_expr-61c532d900245edb.rlib" \
  "$ROOT/tools/decimal-diagnostic-alloc-probe.rs" \
  -o "$ROOT/tools/decimal-diagnostic-alloc-probe"
```

If existing native dependencies require search paths, reuse only recorded matching build-output `-L native=...` paths. Do not enable jemalloc, rebuild dependencies, change Cargo, or substitute allocators merely to make the probe link. A linker failure is not RED. The old compiler tree remains read-only; outputs go only to the new experiment.

A future parent run should record source/artifact hashes, command, complete stdout/stderr, and exit status. Exit 1 is an intentional allocation-contract failure, not an unexplained harness/build error. Parent should independently check controls and all sample vectors before accepting RED. Repeat the identical bounded probe against the authorized lazy-stage artifacts to establish parity, separately from bounded semantic/error-policy tests.

## 9. Parent-executed bounded allocation RED

After H passed datatype378/expression575/aggr40 and D5 passed18/full caller1292+four unchanged failures+93ignored, parent linked the exact §8 command successfully (exit0), with no extra native search paths or dependency rebuild. The standalone probe exited1 with validated, stable allocation-contract failures. Source and linked artifact SHA256 values were saved in `../logs/decimal-diagnostic-probe-before-artifacts.sha256`:

- Probe source: `4f03e8f1f16e6a1445b5b2531d33e932eef20f82a8f078cec9bd0534efc058cc`.
- Datatype rlib: `e46f4fb16b058fd6d62bf58aa0d571be5c7d340cdbe19c610a264364dda032c5`.
- Expression rlib: `d3d1df66d38b74129fa2a4e6fc46aee5b8eaed633a1c9d5d8acae325a8cdaea4`.

`../logs/decimal-diagnostic-alloc-red.log` records all observations. Empty control was `[0,0,0,0]`; each allocator-route control triggered its corresponding counter once, and bounded diagnostic construction triggered two malloc requests. Exact value/shape and zero-warning checks passed. All eight alternating-order pairs were identical for each operation:

| Operation | Actual request vector | Same-native baseline | Observed excess |
|---|---|---|---|
| MOD | `[3,0,0,0]` | `[1,0,0,0]` | 2 malloc requests |
| DIV | `[3,0,0,0]` | `[1,0,0,0]` | 2 malloc requests |
| DAY | `[2,0,0,0]` | `[1,0,0,0]` | 1 malloc request |

This is real bounded **caller allocation RED**, not a wide crash, heap-peak or output-byte measurement. Arithmetic scratch and the final interval String were included on both sides. No MAX-u32 or moderate private-wide operand was formatted.

Only after this gate did parent release B2.2-I's three product files with the exact §4 scope. `convert.rs` remains frozen at H. Reuse this unchanged probe source after the coherent library rebuild; GREEN is still pending. Earlier statements that the auditor did not build/run remain accurate historical provenance, not the final parent execution status.

## 10. Parent-executed matched-profile GREEN

B2.2-I's datatype-only suite passed381; the coherent C3c/I production library build passed and aggregate consumers passed40. The unchanged probe source (`4f03…58cc`) was then relinked against the refreshed **test-profile** libraries and exited0. `../logs/decimal-diagnostic-alloc-green-test-profile.log` records the same successful empty/four-route/diagnostic controls and eight stable alternating pairs for each operation: **MOD, DIV and DAY each have actual `[1,0,0,0]` equal to their own same-native baseline `[1,0,0,0]`**. Numeric/shape and zero-warning checks also passed. This removes the observed excess2/2/1 requests; it is not a zero-allocation or wide-resource claim.

Exact refreshed artifact hashes are in `../logs/decimal-diagnostic-probe-after-test-artifacts.sha256`:

- Datatype `186ef1671a1b311e3c658e95db4dba036fbb654538aa0c32176592bf04b276ec`.
- Expression `93dcca9455bab38aa1108bf57edc6dc80265f0a079d00239d7dc82719dfd0697`.

**Stale-artifact attempt retained, not hidden:** the earlier file named `decimal-diagnostic-alloc-green.log` actually contains RED. Parent had successfully built the DEV-profile library but reused the old explicit TEST-profile rlib paths; `decimal-diagnostic-probe-after-artifacts.sha256` is byte-identical to the before manifest. DEV and TEST differ in codegen units and overflow checks (`tikv/Cargo.toml:428–461`). That run never exercised the patch, and its set-e chain stopped before the subsequent trait probe. Parent then ran the aggregate test consumer to refresh the matching test graph and obtained the real GREEN above. Do not select an artifact by an old path merely because it still exists.

The new post-fix executable is `../tools/decimal-diagnostic-alloc-probe-after`; the prepatch executable remains separate. Full expression/caller compatibility and C3c frame/behavior tests are separate gates, not inferred from direct arithmetic probe parity.

## Final boundary

This record preserves the census, proposed ownership cut, and instrumentation reasoning. It does not grant the three-file product loan or approve oversized diagnostic/temporal/Datum conversion policy. B's producer ownership and the parent's serialized validation authority remain intact. **General wide publication is still closed.**
