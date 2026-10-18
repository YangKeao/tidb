# N7o: actual CONVERT_TZ execution

Local checkpoint on expression-unification-demo. The global no-fallback task
remains active; no push, PR, deployment, full-server validation or complete
Go-package transcreation is included. SDK companion: DCO commit `9668157`.
Previous signed checkpoint is TiDB `16eae632` / TiKV `62db99a`.

## Change and preserved behavior

The native CONVERT_TZ direct entry stops constructing a Ready invocation. It
passes actual values through consuming text-conversion stages into SDK
`native_convert_tz_value`, which accepts nullable borrowed text and finite work.
The explicit wire worker already uses the same unchanged pure
`evaluate_native_convert_tz` parser, zone converter and formatter. There is no new
encoded native value, validity witness, expression callback, argument interpreter,
wire codec, dependency or parser-policy copy.

The existing DurationConstructionNative host now admits CONVERT_TZ with three
original argument nodes/FieldTypes and result provenance. All eager capabilities
are authenticated before body conversion/callbacks. Its demand counter closes
before conversion can error or panic. The first source alone gets the original
ETDatetime cast, including full YEAR source metadata; store borrows are released
before its context callbacks. Consuming stages retain actual datetime text, then
actual from-zone text, always demanding all three strict text conversions even
with an earlier NULL. Conversion errors stop subsequent work. Only afterward
does NULL suppress parsing. The original generic temporal result finish precedes
ordinary return-family conversion, each once.

The original datetime parser runs before zone parsers. Both zone parsers run
before testing either Option, so a valid date, invalid first zone and Unicode
minute digit in the second offset still panic exactly as before. Named-zone
repeated/gap wall-clock policies, the four-hour gap guard, wide year/fraction
text and SYSTEM Local's distinct behavior remain unchanged. A caught original
panic cannot replay into NULL; the same provider accepts a later invocation.
This recovery no longer depends on the retired Ready cache. This step captures
fresh original SQL observations, not a reused-cache history digest.

Finite work checks precede demanded materialization and pure parsing/rendering;
owned intermediate strings count toward retained work. SDK NULL inputs that
already are borrowed text can skip parsing, while the frontend still performs
all demanded strict text conversions. Original invalid UTF-8 validation precedes
string materialization work. A new test initially expected Resource for a huge
invalid byte string; it was corrected to legal ASCII to isolate budget refusal.
Production validation order was not changed. These are conservative logical
work/retention allowances, not allocator-failure or total-statement peak proofs.

## Independent evidence before retirement

Before adding the candidate, real session16506 captured original calls through
the old Ready route. A fresh original context/cache for every case prevents an
earlier offset/domain panic from hiding later SQL behavior with cache poisoning.
Three separate children pin process TZ before Local is used; the parallel parent
never changes TZ. Each covers232452 observations, total697356:

| Child TZ | Original FNV checksum |
| --- | --- |
| UTC | BD5985171C035C25 |
| Asia/Shanghai | 522632888DE0D4F5 |
| America/New_York | B1E585671A6BACCD |

The original Ready lookup guard failed, and canonical source preparation showed
one Admission refusal and one fallback. This capture ended101 because those two
intentional regression tests failed; all original witnesses passed.

With old direct caller and worker still present, session18304 independently
compared every candidate result/error/panic payload and exact context trace with
fresh original calls. All697356 pairs and all three original hashes passed.
Only then was the direct caller retired. The permanent witness retains those
fixed original checksums; it does not generate expectations from the candidate.
Historical paired code is archived, not newly ignored or claimed to rerun after
retirement. The unchanged pure SDK kernel remains the original authority.

Artifacts under the persistent agent workspace `evidence/no-fallback/`:
`n7o-before-{front,sdk}.rs`, `n7o-original-witness.rs`,
`n7o-independent-pairs.rs`, `n7o-original-red.log`,
`n7o-independent-pair.log` and `n7o-final-check.py`/`.log`.
The applied `/tmp/n7o-candidate.py` is historical, never a restart script.

## Validation

All real execution sessions are closed. Final results:

| Check | Result |
| --- | --- |
| Full frontend82995 |1710 pass, exact3 inherited failures,95 ignored,0 filtered; all256 batch pass |
| Full SDK30658 |1120 pass, exact1 inherited failure,1 ignored,0 filtered |
| Projection69650 |26 pass; new7-root zone/CHAR/arithmetic group, both flags/workers0 and2 |
| Scoped clippy7644 |89 expression and171 all-crate warning-title multiset exactly N7n |
| Frontend warning titles |346 titles exactly N7n |
| Lint24945 |pass |
| Both pinned formatters |all11 changed Rust files pass |
| Immutable STR_TO_DATE fixtures |original lengths and SHA256 unchanged |
| Both repository diffs |pass |

Initial focus16605 had seven passes, including the entire source matrix and all
fixed witnesses, and the one incorrect invalid-UTF8 budget expectation described
above. The corrected legal-ASCII test passes in final full frontend. The candidate value and source implementations did not change after the
independent proof; the old direct caller was then retired. Only a witness explanatory comment changed
after final runtime gates; final dual formatter checks include that comment.

Source matrix covers all three argument positions,11 declared input families,
eight result families, four seeds/three error levels, empty/full/reordered and
duplicate selections, four vectorization transitions, CHAR parents, eager ABS
errors after NULL and2050 invalid-date warning occurrences. Comparisons include
complete physical output columns and exact dynamic callback/warning traces.
Contradictory metadata representation parity does not certify malformed shapes.
The expanded existing provider test covers both output roles and all three
capabilities (corrupt, stale, wire NULL, wrong role/node/occurrence/input row),
exclusive context access, cancellation/expired tasks, original final-argument
panic followed by replay rejection and recovery, and a demanded1MiB third text
rejected under128KiB even after two NULLs. SDK actual-value tests additionally
pin the known named DST cases, wider year, NULL parsing and finite work.

Exact inherited frontend failures remain:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

SDK failure remains
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
No inherited failure was filtered, dropped, renamed or reclassified.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib convert_tz
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The historical independent-pair run used the first command with filter
`convert_tz_original_observations` before retirement. Its archived test/log is the
paired evidence; the current test is the permanent original-checksum witness.
Commands from TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Both pinned rustfmt binaries are under
`/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-{2026-01-30,2026-08-22}-x86_64-unknown-linux-gnu/bin/rustfmt`,
run with `--check --edition 2021 --config skip_children=true` on the11 files in
`n7o-final-check.py`. That script also checks all counts, exact failures/warning
Counters, original hashes, fixture lengths/SHA and both `git diff --check`.

No Go/Bazel, datatype or unistore production file changed; no bazel_prepare gate
was triggered and datatype/unistore suites were not rerun. Prior datatype failures
remain open. The unrelated untracked tikv-client-rs BUILD.bazel is untouched.
Remaining ADDTIME/SUBTIME/TIMESTAMP/TIMESTAMPADD and format/source families,
generic admission fallback, old unistore traversal, unsupported/malformed source
coverage, inherited failures, Go semantic audits and full-server SQL gates remain
open. This local checkpoint must not be reported as global completion.
