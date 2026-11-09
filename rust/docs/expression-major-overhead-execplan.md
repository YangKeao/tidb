# Locate and remove one remaining shared expression execution cost

This living ExecPlan follows root PLANS.md. Maintain Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective. Task 19 continues YangKeao's message 6129cd47 and the existing performance work, with TiDBEngineer follow-up b1a9337c.

## Purpose / Big Picture


The retained implementation shares the SDK execution graph, but latest same-window whole-SQL CPU still differs substantially from the original native implementation. For 4096 rows, JSON is about 4.13 times, text 4.73 times and a 16-level expression 5.41 times the original; a scan is about 1.02 times. Locate a current, material expression cost and simplify one common path without bypassing native kernels, validation, short-circuiting, warnings or callbacks. An implementation hypothesis must follow actual current evidence rather than repeating rejected local refactors.

## Progress


- [x] Claim task 19 from retained Main c4fc700e6858 / SDK f7672bf9. Read all 23 existing measurements and the old profiling limitations.
- [x] Check local sampling support. LBR event recording is unavailable on this PMU despite a CPU feature flag; ordinary user-mode cycle sampling works. Preserve both probes.
- [x] Capture only three diagnostic workloads (JSON, text, depth16), retained service14 and original99a, six 15-second samples with six exact strict comparisons and actual session readbacks. No full timing-matrix rerun.
- [x] Identify a current hotspot and source-level mechanism; declare one falsifiable hypothesis, its affected contracts and smallest verification scope before production changes.
- [x] Implement the one-attribute candidate and build its actual service. The declared artifact gate failed; withdraw before unit/runtime and paired timing. Preserve the exact generated-code evidence and restore all9521 source/config inputs.
- [x] Deliver result and code counts in message e68749af before local submission. Raw evidence is being sealed with this result; the subsequent raw-delivery and local documentation-only DCO receipts are recorded separately after this immutable snapshot. No push, PR or deployment.

## Surprises & Discoveries


Old samples predate retained owned-value moves and frame reuse, so their percentages are not current cost attribution. Old DWARF records also lacked usable complete call chains. A fresh LBR support probe fails with unsupported branch-stack event, while a leaf-IP user-cycle probe records 93 samples successfully. The sampler-support probe also includes unresolved kernel-marked records despite the requested user filter; preserve those actual markers/unknowns and do not renormalize known-user symbols to100%. Leaf-IP means the recorded instruction address; it identifies sampled symbols, not their complete callers or total allocation/authentication cost.

## Decision Log


Decision: diagnose JSON, text and depth16 only, using saved actual service14 and original99a with unchanged backend355b. Rationale: latest measurements show large absolute and relative expression gaps across these families, while the scan is near parity. Six short samples are sufficient to choose a first source mechanism; they are not a new speedup benchmark. Date/author: 2026-11-10 / ExpressionReuseEngineer.

Decision: do not rebuild with frame pointers or assume successful call-chain collection. Preserve unavailable LBR evidence, use valid leaf-IP samples, and combine them with direct source inspection and a falsifying local test. Do not attribute memcpy/malloc self samples to an unobserved caller. Date/author: 2026-11-10 / ExpressionReuseEngineer.

## Outcomes & Retrospective


Six diagnostic captures ended with actual exit0/127.72seconds, 6662 complete-fetch queries under the profiler, unchanged source and owned cleanup. Initial strict preflight exited1/38.81seconds before sampling; all six observations were preserved, with only estimated row counts2730.67/3333.33 different. An exact owned-storage restart and unchanged six-comparison retry passed. No speedup is inferred. The one-attribute candidate was built and rejected at the declared artifact gate. The actual service build exited0 in558.633760seconds; saved service15 SHA256 dadaa14d19f4ca0c0b654e07414d3cd3d45581b100030c3ff1cd7fa9d7e68e88 remains a rejected artifact. All9521 source/config inputs and pretrial git status were restored exactly. No new gain is claimed. Task 18 remains fully delivered/in_review with source simplification only. Cumulative retained source reduction remains 5159 production lines and 192 test lines. The original broader goal is not complete.

## Context and Orientation


Main is /home/agent/tidb/expression-unification/tidb at c4fc700e68583dfd9ddf06095565c07d9beb2a06. SDK is sibling tikv at f7672bf980143f2938fd0f900650a4c29091e70f. The shared evaluator is components/tidb_query_expr/src/types/expr_eval.rs in SDK; native frontend adaptation is rust/crates/tidb-expr/src/evaluator/batch_hosts.rs. Determine the actual changed function only after diagnosis. Preserve error phases, source/generation authentication, physical row order, callbacks and same-program recovery.

Retained actual service14 SHA256 is 2d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea, original reference99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2, backend355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029. Saved paths and every launched PID/birth/command are recorded in agent evidence/path-unification/major-overhead-probe. Do not replace these with an unverified mutable target.

## Plan of Work


First preserve current source/configuration inputs and actual ELF hashes after creating this plan path. Reuse the existing deterministic 8192-row SQL fixture in a new owned database. Request legal initial chunk32, maximum1024 and vectorization1, capture actual readbacks, and compare complete results, fields, warnings and EXPLAIN for all six observations before sampling. Run current and original endpoints sequentially on the same fixed processor affinities and storage. Each sample is 15 seconds of complete fetches at 199 user-cycle samples/second; preserve actual perf exit, record logs, query count, sampled event/symbol output and before/after results. Report leaf self percentages as diagnostic samples only.

After all terminals, inspect the current source around actual sampled symbols. Prefer deletion or reuse of existing mechanisms, not another evaluator, speculative fast path or new abstraction. Revise this plan with one exact hypothesis and verification/rollback steps before implementation. A rejected mechanism must not be silently repeated under another name.

## Concrete Steps


From the agent workspace, run the durable job runner on this trial's profile job after a fresh current directive and actual 16-second load observation. The controller creates only its own loopback PD/TiKV, derived real SQL table and current/original endpoints. Use perf record -e cycles:u -F 199 attached to the actual saved frontend PID; no call-chain claim is made. Keep profiler active only during diagnostic loads, never during paired performance timing. Later required build/test commands will be specified for the actual source mechanism; source changes require root make lint, pinned Rust formatting and appropriate affected behavior tests.

## Validation and Acceptance


Source inputs and actual ELF hashes must be fixed throughout capture. All six initial observations must agree in rows, exact encoded result digest, field metadata, warnings, complete EXPLAIN and effective configuration. Each capture checks the same values and settings again, saves real terminal status and requires nonempty cycle samples. A transient plan mismatch may be preserved and diagnosed, followed only by an exact owned-storage restart/retry; never normalize plans or weaken equality.

Choose a candidate only when actual current samples plus source establish a distinct plausible operation that can be independently removed/shared. Existing full-task18 tests are inherited evidence during pure diagnosis, not newly executed tests. Later implementation must retain behavior and show paired affected-case performance evidence, with scans and error/recovery controls appropriate to the mechanism. No arbitrary post-hoc tolerance, causal claim from percentages or unobserved caller chain is acceptable.

## Idempotence and Recovery


Do not edit production source during active profiles/builds/tests/timing. Save each actual failed attempt separately; never overwrite task15/16/17/18 archives or captured perf files. Finally stop only recorded PID/birth-matched services and delete only their command-verified data directories. Protected unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel SHA256 cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52 must never be edited, staged or deleted. Local DCO authority YangKeao/yangkeao@chunibyo.icu persists; reports and raw evidence precede submission, no push.

## Artifacts and Notes


New evidence directory major-overhead-probe keeps sampler capability receipts, source/ELF pins, load observations, strict comparisons, six actual captures, compact samples, source hypotheses, later affected tests/performance and cleanup. Original task18 macro code/text evidence and archive remain immutable; profiles must be captured from the retained macro service, not an old pre-ownership version.

## Interfaces and Dependencies


Pure diagnosis changes no runtime signature or dependency. The chosen production function is NativeValueStore::insert_owned in rust/crates/tidb-expr/src/evaluator/batch_values.rs. Add only #[inline(always)] above it. Removing that attribute restores the entire original file including every test byte. This is one extra production line and no test change; no new branch, signature, dependency or registration implementation. All row/window arithmetic, numeric, string/JSON and diagnostic publishers continue using the existing common function with unchanged generation/role/origin/lease/error logic.

## Implementation milestone: expose the common owned registration body to callers


Fresh deep16 samples put collect_window_source_charged at19.43% and insert_owned at12.74% leaf self cycles; these are not recoverable gain percentages or complete caller costs. Actual annotated insert_owned code takes an80-byte Datum and moves it through stack payloads into112-byte Entry storage. Candidate hypothesis: inlining the existing registration entry exposes concrete caller values to the optimizer and removes the out-of-line ABI/intermediate materialization. It does not substitute a new value store, bypass authentication, change allocation behavior intentionally or implement a new kernel.

Only one inline(always) attribute is added. Preserve baseline/candidate entire-file hashes, unchanged test tail and initial/candidate9521 source/config inputs. Actual new unit15 must keep the original2440 names and run the original973 scoped consumers (expected972pass/1 exact inherited common failure after PID/time-only normalization). SDK is unchanged and its older1159/1/1 results and171 clippy messages remain inherited only; no new full2440 or allocation-failure run is claimed.

From Main with empty RUSTFLAGS and CARGO_BUILD_JOBS=1, run ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json, then the pinned consumer list; build tidb-server --no-default-features through the same wrapper, run root make lint, Jan30/Aug22 rustfmt checks and diff/source guards. Save actual unit15 and service15 with hashes; never overwrite retained14 or old archives. Native-store-inline functional_gates.py records real child exits and continues through all terminals.

Inspect actual hot caller finish_arithmetic_window and the owned registration symbol/entry. Symbol disappearance alone is not speedup: verify elimination of the out-of-line transfer and inspect intermediate move sequences/code growth. If the intended materialization/boundary does not improve, restore this one attribute before paired timing and report that falsification rather than running a full matrix.

Only if code generation supports the hypothesis, compare eight fixed scenes: scan, decimal, floating arithmetic, JSON, text, depth16, integer-plus-literal and ABS(nullable+j). The first five include differing domains and unaffected controls; the latter cases exercise deep chains, literal authentication and NULL behavior. A and B are separate launches of the same retained service14, C is candidate15, F is original99a, all on backend355b. Four balanced orders A-C-F-B, C-B-A-F, B-F-C-A, F-A-B-C yield32 strict observations and128 timing cells, each at least4seconds and30 full fetches without a cap. Exact actual settings/results/fields/warnings/full plans precede timing; source/ELFs stay fixed, fresh actual quiet is required, no profiler/build/test runs during timing. Report all CPU/p50/p95/QPS absolute values, paired medians/ranges and A/B variation; no control subtraction, causal claim from fractions or confidence-interval claim. This is a new focused candidate comparison, not a rerun of the completed23-scene matrix.

Retain only a supported measured improvement with preserved contracts and honestly assessed adverse observations; otherwise restore the guarded original file and record no new retained gain. If kept, one added production line changes cumulative net reduction from5159 to5158; tests remain192 fewer. Report and raw evidence precede already-authorized local DCO. Source simplification remains a broader goal; this candidate adds no duplicated execution path. No push.

Decision (2026-11-10 / ExpressionReuseEngineer): target a measured common registration boundary, rather than repeating eight previously rejected source-read deletions. Source body/semantics stay identical, and actual code generation plus focused performance can falsify the hypothesis. The artifact study records the existing duplicate authentication calls separately; no such read change is included here.

Build-order refinement before any candidate build: build the actual service first, pin its successful output and inspect the real hot caller. Only a supported code-generation mechanism proceeds to unit compilation, original973 runtime tests, lint and format checks, then focused timing. If the intended boundary/materialization does not improve, restore the attribute before unnecessary behavior or timing runs. The functional controller reuses the actual successful service receipt and immutable hash, without a duplicate service build. Service-first build was launched at2026-11-09 21:56:26UTC; no candidate unit or paired timing had begun at that point.

## Final artifact result and decision

The entry symbol insert_owned disappears, but this is not the promised removal of the common registration call boundary. Both arithmetic caller sites now call the outlined insert_with::<insert_owned closure> at0x44eb8f0,1718bytes. The caller still stages an80-byte payload (five16-byte chunks, stack0x110..0x15f), passes its pointer inr8, and the callee reuse path stages it through stack0x90..0xdf and an Entry scratch before112-byte storage. No claim is made that every local copy remains: some checks/materializations moved or disappeared and the outlined callee stack shrank344 to248bytes. The necessary caller-to-registration boundary was nevertheless retained.

The arithmetic caller grew6286 to6702bytes (+416); its reserved stack grew1896 to1912bytes (+16). Entire.text grew78390112 to78393888bytes (+3776). Static code sizes are not dynamic CPU costs and do not establish regression. This result fails the predeclared mechanism condition; do not change it after seeing the artifact to justify running the planned8-scene comparison. Restore only the guarded attribute and preserve the actual candidate ELF, raw caller/callee code and source/input snapshots.

Initial inspection mistakenly counted a closure name containing insert_owned as a direct call to that function; keep that raw JSON and use complete symbol equality for corrected classification. Corrected output shows zero direct insert_owned calls but two actual insert_with calls, so symbol disappearance alone would have been a misleading acceptance.

No candidate unit build,973 runtime tests, root lint, candidate formatting gates,8-scene paired timing, full2440 execution, new SDK run or allocation-failure injection was performed after artifact rejection. Earlier retained behavior/quality evidence is inherited from the byte-for-byte restored c4fc/f767 version only. No quantitative candidate performance result exists; do not report0percent as a measured speedup. Newly retained source/test changes and established speedup are zero; cumulative retained reductions remain5159 production/192 test lines. Actual baseline service14 remains the only retained trial reference. The mutable Cargo target currently contains rejected service15 and must not be used as the retained baseline without a new matching build/pin.

Decision (2026-11-10 / ExpressionReuseEngineer): withdraw the single insert_owned inline attribute at the artifact gate. Preserve six current diagnostic profiles and this independently falsified boundary hypothesis; do not silently repeat it or either prior read-deletion candidate. The original shared-path/no-performance-loss goal remains unmet. This completed scoped investigation is ready for review; report and raw evidence precede authorized local documentation-only DCO, no push.

Independent read-only scope/sampling review cad0cf36 and actual artifact/restoration review281aab24 passed. Reviewer found an additional observation-script issue: objdump continuation-byte lines had been counted as instructions. Both prior JSON/stdout versions are preserved; the corrected mnemonic-column filter gives actual hot-caller static instruction lines1098 to1159. This is not a dynamic instruction count or performance result. Raw peer attachments are included unchanged in the evidence archive.
