# Shared remaining numeric helper algorithms

**numeric-text-136 / R141**, after [float target fitting](float-target-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK integer/float/Decimal owners now hold numeric_helper's best-effort integer parser, const precision/display-length arithmetic and truncated fixed float text. Native retains compatible facade names and aliases the error enum; algorithm bodies are removed.

Best-effort Unicode trim, ASCII digit scanning, unsigned-accumulator/signed-limit saturation and trailing-input precedence remain distinct from ordinary numeric prefix parsing. Const arithmetic retains its original non-inverse and negative-length behavior. Fixed text keeps Rust Display, signed zero/nonfinite spelling and original exponent handling. A single fixed-layout leaf is factored from the existing Decimal Go-g formatter and reused; its Ryu/LowerExp/Display digit generators and scientific cutovers are not replaced.

## Validation

Six matched Cargo gates GREEN: new SDK3/existing formatter1, full native datatype479, numeric consumers5 and existing SQL2. [Exact commands/counts/hashes and integration incident](../logs/numeric-text-summary.txt). Initial rustfmt found a missing closing brace in the new parser; parent fixed it before any Cargo launch. Four new tests;104 SDK/7 native old touched-file test bodies unchanged. No new Rust files or SQL fixture/probe credit. Expression/SQL files unchanged. Other datatype source conversion/event merging/controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
