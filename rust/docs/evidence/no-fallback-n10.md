# N10: Common value sources selected by original demand policy

This is a local checkpoint in the continuing overall no-fallback task. TiDB
parent is 4ad29112; TiKV remains unchanged at 7df75c9. No push, PR, release,
complete Go-package port, whole-server validation or performance claim is made.

## Original policy and integration

Before changing production selection, the real original scalar entrance was
executed for 328 names and 2,308 arity cases, including malformed calls and
internal/prefix/unknown names. Exactly 2,036 reached the common eager body;
BC3F1FF91A278C16 hashes name/arity/actual phase only. An unchanged rerun and
2,308 independent classifier-versus-executed-original comparisons passed. The
original body itself is unchanged. Executed policy/tests/scalar/common/batch
sources were archived before integration as n10-independent-*.rs.

A metadata-only policy now distinguishes calls before the stored-name probe,
calls after that probe, and ordinary common eager calls. Every one of the 2,036
common cases has an actual source recipe. Existing dedicated actual kernels and
proved typed closures retain their priority. The former 54-name list remains
only as the compatibility priority for original byte/coercion behavior; it no
longer limits common source admission. New common sources use the same variadic
native-value host, unchanged original pure eager body and SDK driver. Unsupported
names and malformed common arities produce their original failure only when
actually demanded; dead unknown calls under six controls execute no alternate
evaluator.

The stored first-constant probe runs before argument demand at its original
semantic point. Probes are copied only after the complete metadata byte gate,
aligned with host slots, and omitted for policies returning before that point.
Families already owning the probe keep one copy of that phase. The original
ISNULL malformed-coefficient panic exposed the former omission before this fix.
No expression tree or expression callback is retained by a value kernel.

## Actual demanded work and diagnostics

FORMAT's Decimal number-text conversion and the existing Decimal-to-real
formatting paths now charge their finite lexical work before calling their
original owner. Valid 4,097-digit/4,096-scale regressions failed by the expected
budget assertions before the fix; initial malformed test-fixture panics remain
in earlier logs. FORMAT NULL still skips unused precision display. Computed
huge Decimal refusal and same-source NULL recovery include FORMAT, SIN and ACOS.
Existing strict JSON rejection and other ignored-metadata behavior remain tested.

Common math diagnostics retain raw numeric literal, column-name and punctuation
facts at preparation. Only an actual overflow renders these facts, using the
original native render helpers and finite work scope. There are no executable
expressions in this metadata. A genuine dead-EXP/huge-Decimal regression was red
before deferred rendering and then passed. Seventy independent comparisons with
the original static formatter include nested arithmetic, all numeric carriers
and the original invalid-coefficient panic. Dedicated ABS's existing proved
source protocol remains intact.

## Validation and limits

Final frontend 22722 exits 101 only for the exact three prior failures:
1,814 pass/3 fail/95 ignore; all 308 batch tests pass. All 346 warning-title
counts exactly match N9. Projection 61585 passes 26, including the additional
11-root cohort with SIN/ACOS/two-argument ROUND/POW/dead unknown calls and CHAR
parents, both vector flags and workers 0/2. Earlier cohorts and clock configuration
zero remain unchanged. Lint 28042 exits zero. Final cached-binary focus passes
2 policy tests, 8 source/work tests, the 70-case diagnostic test and FORMAT work
test. Both pinned formatters check all 14 touched Rust files. The read-only
n10-final-check.py passes, checking original witnesses, exact failures/warnings,
unchanged original scalar/common production owners, immutable fixtures, SDK
cleanliness, finite metadata structure, diff quality and unrelated BUILD exclusion.

The actual runtime matrix covers 1,699 non-niladic common call shapes with genuine
NULL rows, duplicate selection and four flag/reuse runs. User-variable shapes
remain explicitly in the pending ordered-suite adapter, and niladic clock/random
sources retain their separate deterministic proofs. This matrix exposed a real
old mismatch: LOGICAND/LOGICOR protocol aliases were accepted as scalar logical
operations although the original scalar body eagerly demands their operands and
reports Unsupported. Both aliases now use that original common body; additional
non-NULL comparisons preserve their original errors. The 328-name corpus is
isolated from concurrent extension-registry tests. All 2,036 common source recipe
cases remain separately covered.

Two old assertions expecting fallback/preparation rejection were
updated only where common ROUND(2 args) and malformed PLUS(1 arg) are now genuinely
prepared; the latter also compares its actual demanded error to the original.
Typed integer kernel identities and the full nanosecond Duration provider retain
all original protocol assertions. No inherited failure or immutable fixture was
changed or filtered out.

TiKV has no N10 source changes. Its N9 complete suite (1,141 pass/exact one prior
failure/one ignore) and clippy warning baseline remain the recorded SDK evidence;
these are reused, not claimed as newly executed N10 runs. No Go/Bazel sources
changed, and the unrelated untracked client BUILD.bazel stays excluded.

Reproduce from the TiDB root with RUSTFLAGS='' CARGO_BUILD_JOBS=8 and
`../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib` (exact three inherited failures), then the same
command with `-p tidb-executor --lib projection::tests`, plus `make lint` and both
pinned rustfmt checks. Workspace evidence/no-fallback/n10-*.log and
n10-final-check.py preserve attempts, original witnesses, final checks and
snapshot comparisons.

Remaining ordered effects, early/session/metadata/prefix adapters, parameter/
deferred/correlated/PB/optional-type sources, incomplete composed source phases,
public AST/unistore/filter traversal and generic Admission dispatch stay in the
active overall task. Continue those after signing this checkpoint without
waiting for another prompt.
