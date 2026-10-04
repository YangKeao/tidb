# M2 float-format and Decimal precision policies

Checkpoint **decimal-policy-89**, after **cast-real-uint-88**. Two native type-algorithm bodies are removed, but no new evaluator profile or family is claimed. Functional226/245, strict0, remaining19 and all226 family objects are unchanged.

## Float formatting: share layout, preserve different policies

Native `mydecimal::format_float_g_shortest` becomes a thin call to TiKV `Decimal::native_format_float_g_shortest`. This preserves the original Rust LowerExp generator, NaN/±Inf spellings and negative zero. The helper is used by MyDecimal::from_float64 and Datum conversion as well as CAST/expression diagnostic text.

The earlier `native_format_go_shortest_float` retains Ryu digits, its finite-only contract and zero normalized to `0`; `native_from_f64` still uses that entry. Directly aliasing the two would change behavior. They now share a single sign/significant-digits/exponent→Go-g layout renderer. This is not an assumption that both generators choose identical digits at every shortest-representation tie.

The LowerExp mantissa can be reconstructed from its existing digits without changing its spelling. Both paths retain scientific notation below exponent−4 or at/above6, signed exponents with at least two digits, and their original fixed-point layout. Special values return at their original policy boundary. Existing tests are unchanged.

R91 correctly recorded this formatter as still native at that checkpoint. Its old evidence and partial-slice record remain historical; current ownership is now TiKV.

## Decimal precision policy

Native `Decimal::cast_to_precision` becomes only toShared→`try_native_cast_to_precision`→fromShared→expect. No native rounding, digit-count/clamp decision or all-nine construction remains in that method. Its original infallible API still panics on bridge refusal rather than silently falling back.

The SDK uses the existing rounder with **scale as i32**, then the actual canonical coefficient length minus result scale. The canonical projection, also used by visible formatting, keeps the original storage-scale floor and one digit for integral zero; physical integer-word count is not equivalent.

The comparison uses `flen.saturating_sub(original_requested_scale)`. If clamping is needed, construct exactly flen nines, left-pad to the original scale and use the existing native digit constructor. Consequently `(flen1,scale2)` clamps `1.23` to `.09`, but leaves subunit `.12` unchanged. This odd existing policy is preserved, not repaired.

Other fixed cases cover rounding before overflow, negative values, flen0 still rounding and clearing declared shape, hidden storage, negative-zero normalization,90-digit Grow values, huge flen without a clamp, and safe u32MAX→i32−1 rounding. Bounded SDK tests reject huge clamp allocation before constructing it. No SQL65/30 or fixed-nine-word cap is introduced; no physical peak-memory bound is claimed. Existing extreme shift/padding behavior is unchanged.

CAST warning classification and overflow-before-truncation order remain in `report_decimal_production`. GL's source preparation and winner-scale policy remain with its existing callers. This is a shared type prerequisite, not complete CAST DECIMAL target, extrema or whole-CAST migration. No new C4 profile, transport, admission or request-owner lifecycle is added.

## Validation

[Exact commands, times, counts and hashes](../logs/decimal-policy-summary.txt); [manifest](../checkpoint.json).

Five new tests all pass on first matching execution:2CPP datatype,2native datatype and1SQL. CPP Decimal97 and native Decimal103 include the existing float-constructor/formatting and raw Decimal tests. Native cast21, prior bridge1 and prior PBShared1 also pass.

New SQL20SELECT=10cases×2vector modes, normal pool only. Sixteen Decimal-result probes cover positive/negative saturation, rounding carry,1690-before1292, zero/padding/ordinary values, exact type metadata and a stored FLOAT→DECIMAL value conversion through the newly shared formatter. Four diagnostic probes cover negative real→UNSIGNED and large real→SIGNED. Expected values/messages come from original source rules, not provider recording. No new zero-slot or worker-root proof is claimed.

The prior R91 SQL test's36SELECT probes also pass again, separately from the20new probes.

Nine locked nonzero single-threaded launches:7green,2only-old full RED. Full expression1598/4old/94ignored and unistore219/1old/13ignored remain RED. No compile failure, new execution failure, oracle correction, zero-match, interruption or fixture recording. Entire failure sections are byte-identical to R91 after only numeric panic-heading thread IDs are normalized: expression `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, unistore `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:1CPP/3native Rust files, no new modules;95CPP/203native original test bodies byte-identical,2CPP/3native new tests. Pinned formatting/diff checks pass. D independently reviewed formatter policies/layout, precision count/scale/shape/clamp/budget and native delegates without a blocker. Agent-doc changes are descriptive, not new policy. No Cargo/lock, Go/Bazel or generated changes; unrelated untracked BUILD excluded.

## Remaining work

[Remaining acceptance](remaining-acceptance.md) still contains5core,8ordinary pending and6complex candidates. Other CAST/source parsing/warning policy, extrema/IN/INTERVAL and actual request-owner/final cross-entry closure remain pending.

Known full-suite failures and extreme Decimal/CAST/INTDIV/mode/JSON/vector/older Values gaps are not repaired. Workspace/lint/dev/bazel_prepare/release, exhaustive generator equivalence or differential testing, TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are unverified. No goal-completion or PR-readiness claim.
