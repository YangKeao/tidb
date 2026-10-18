# N7f: actual bounded-staleness values and canonical generic sources

Local signed SDK commit `e212b91` is paired with this TiDB implementation and
receipt; neither is pushed.

The native TIDB_BOUNDED_STALENESS caller no longer encodes operands/results or
executes head/finish Ready programs. It uses actual SDK temporal values; explicit
wire compatibility APIs delegate to those same functions. Generic source calls
run through the existing canonical SDK source frame, including their original
datetime argument casts and statement-owned SafeTS service. Global no-fallback
migration is still incomplete; this is a local checkpoint, not a package port.

## Observable contract and changes

`native_bounded_staleness_head_values` checks left zero components first, then
right zero components, then reversed-range NULL. Only NeedSafe permits the
caller to obtain the original statement SafeTS. `native_bounded_staleness_finish_values`
clamps raw calendar cores and preserves every chosen core bit, then changes only
kind/FSP to DATETIME/3. It does not round microseconds, validate SafeTS calendar
fields or read timezone/date modes. Its None result means invalid call sequencing,
not a SQL NULL value. Explicit wire argument validation and invalid-frame errors
remain compatible.

The frontend value layer keeps original arity/NULL/unsupported behavior,
handle_truncate warning disposition and SafeTS demand. Warning formatting and
fixed-size result work are checked before their corresponding callbacks under
finite allowances. NULL and reversed-range NULL require neither operation.
Original FSP255 invalid diagnostic panics are not silently normalized; the
existing formatter and its work before the panic remain unchanged.

The generic source host has two authenticated native input capabilities, a
fixed-size retained first token and the original declared argument types. It
demands both sources before NULL suppression, then bounds/clones original
payloads and drops value-store borrows before the existing datetime casts,
warnings and SafeTS service. This permits callbacks to borrow the store
exclusively. Additional argument-type metadata copies are charged explicitly.
Typed time and native-value publication retain the existing finite ownership,
cancellation, selection-occurrence and provenance checks; no resource maximum
was raised. No cache/session-default SafeTS, fabricated source or secondary
expression program is introduced.

## Independent old behavior before retirement

At the signed N7e heads TiDBb6790f48/TiKV940f123, the unmodified public caller
produced468099 observations and fixed digest58044483da932cf4 in
`n7f-original-and-red.log` (27882 CLOSED101). The domain includes76 actual inputs,
9 SafeTS choices, all3 truncate dispositions and3 cache profiles, raw calendar
and auxiliary bits, kind/FSP metadata including valid-core FSP255, unsupported
carriers, NULL and arity boundaries. The initial zero-hash assertion was solely
a capture mechanism, not a behavioral RED.

That same run reproduces two actual defects: an intentional panic on old Ready
cache lookup, and canonical source refusal/fallback(1,1). New SDK value functions
were initially added beside untouched wire bodies, and the new frontend bounded
helper beside the untouched public caller. `n7f-original-paired-final.log`
(40439 CLOSED0) independently compares467856 old/new ordinary calls;243 arity
boundaries are protected by the captured digest, for468099 total observations.
Only after that pass were the caller's old programs/report decoder removed and
wire functions delegated to actual values. Temporary old/new comparisons were
removed after retirement; a new/new comparison is not retained as an independent
oracle. The fixed original digest and count remain executable regression checks.
The earlier63233 build failed only because the new helper used two incorrect
Time accessor names; it is not behavioral evidence.

## Source and resource regressions

Preparation initially rejected mismatched Time versus capability descriptors.
`n7f-focused-detail.log` (59806 CLOSED101) records the actual catalog error. Adding
the family only to root promotion is insufficient: argument_view and
native_value_call must also agree with the exact host declaration. Both were
corrected without weakening descriptor checks.

`n7f-source-context.log` (73549 CLOSED0) passes7 focused tests. They cover all8
stateful date-mode seeds and3 truncate strategies, String/YEAR conversion,
DATE/DATETIME/TIMESTAMP with FSP0/3/6, original eager later errors after NULL,
CHAR parents, empty/full/2050-duplicate selections and stateful SafeTS with both
vectorization flags. Original scalar comparison proves source/callback/publication
order; arithmetic equivalence is separately grounded in the pre-retirement old
comparison above.

`n7f-providers.log` (11419 CLOSED0) passes8 focused tests. The extra provider test
checks both typed/native outputs, stale/wrong-role/wrong-node/wrong-occurrence
capabilities at both prefixes, including a genuine NULL first operand; no SafeTS
runs on refusal. An excessive actual payload fails before SafeTS, double cancel
is safe, and reuse succeeds. Every statement service can borrow the value store
mutably; actual SafeTS selection preserves raw core and FSP3 metadata. Zero-work
value tests separately prove resource refusal precedes warning/SafeTS callbacks,
while zero-budget NULL/reversed-range terminals stay valid.

## Reproduction and final validation

From the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib bounded_staleness
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

All final validation handles are closed. Full frontend93291 CLOSED101
(`n7f-front-full.log`):1644pass/exact inherited3fail/95ignored/0filtered, all222
`evaluator::batch` tests pass. Both the bounded-staleness original digest and
STR_TO_DATE immutable history/explicit calendar delta tests actually pass.
Exact frontend failures remain:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

Full SDK28506 CLOSED101 (`n7f-sdk-full.log`):1096pass/exact inherited1fail/
1ignored/0filtered. The sole failure remains
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
No failure was filtered, ignored or reclassified to obtain these lists.
Projection5179 CLOSED0 (`n7f-projection.log`):all26pass. Clippy5718 CLOSED0
(`n7f-clippy.log`):89 expression-test warnings, and the171 diagnostic titles
across all emitted crates match N7e as an exact multiset, excluding generated
warning-count and future-incompatibility summaries. Lint26407 CLOSED0.
Projection adds a mixed four-root configuration for conditional source text,
actual bounded time, CHAR and IFNULL parents with serial/two-worker execution
and both flags; no extra test function is required. Static12-Rust-file formatting
under both repository compilers, both diff checks and unchanged historical
STR_TO_DATE gzip/Go fixture SHA256+JSON checks pass (`n7f-final-static.log`).

## Remaining scope

Native date arithmetic, legacy date arithmetic, EXTRACT/MONTH compatibility,
FROM_UNIXTIME and monolithic date/time helpers still contain old callers. Other
PB/source families, frontend3 and datatype2 inherited failures, whole-server/SQL
validation and global alternate dispatch removal remain open. The SDK's existing
STR_TO_DATE worker diagnostic/scanner issue is deliberately unchanged. Original
Go ignores a consumed `%j` context entry while native stores a wide day; changing
that parser policy requires independent Go fixtures and explicit semantic deltas
against the immutable historical record. Neither history nor parser expectations
were altered here. No datatype production source changed, so its last564pass/
same2fail baseline was not rerun. No full-server build or performance benchmark
was run, and there is no push, PR or deployment authorization.

The only SDK edit after runtime checks restores the original explanatory zero-component comment beside the moved classifier; pinned formatting was rechecked. No runtime or test body changed after final validation.
