# N63: preserve genuine contextual native profiles in Projection

This is a locally validated migration checkpoint. The confirmed
CHAR_LENGTH parameter gap is closed through the same SDK compiler and frame.
Overall task #4 remains in progress: public Unistore typed trees, full local
services and final unified performance still require completion. No push, PR,
release or deployment is part of this checkpoint.

## Production contracts

Only `evaluator/batch.rs` changes production Rust. The additional profile
inspects bounded genuine ordinary ScalarFunction metadata, existing native
eligibility and actual result target. Native scalar-row specializations retain
RootValue; other actual native targets retain BulkRoot(target), with a genuine
RootValue companion when vectorization is disabled. Existing atomic native
publication, declaration/owner/schema checks and limits are unchanged. Initial
cache binding applies to the newly admitted profiles; established Projection
and PB profiles preserve their previous policy.

No replacement expression, Suite, SQL type, interpreter callback, traversal
driver or value algorithm is introduced. SDK source is clean and unchanged at
391243a9b45cbff43635f07b53ef75404f0a68aa. All old kernels and fixed original
hashes remain unchanged. The capture-only writer is retired from the observer.

## Independent originals and actual reproduced failure

The signed N62 pre-observer archive authenticates 627 source/input files.
The observer exercises 76 real function/type specifications and 123 genuinely
native-eligible source shapes, including string lengths and aliases,
substring variants, ANY_VALUE/IFNULL/COALESCE typed returns and truth predicates.
Literal, parameter and deferred-parameter sources retain actual declarations.
Coverage includes both vector flags, NULL, callback SQL errors and panic,
empty/full/duplicate selections, output prefixes and two calls to the same Suite.

Native Suite and actual old Projection have different cache/flag contracts.
Each API was captured four times before production changes; they are kept as
separate immutable originals:

| API | Records | Digest |
| --- | ---: | --- |
| Native Suite | 16416 | 8A4471067FCF4CED |
| Actual pre-retry-removal Projection | 16416 | 58F7ED7180273A11 |

The Projection capture temporarily restored exact signed N61 evaluator.rs and
batch.rs, authenticated both source hashes and byte-identical outputs, then
restored the current signed N62 source exactly. The restore-state receipt is
inactive. The immutable batch_context_native_original.json enforces each real
Projection record; the collector body is unchanged from both executed captures.

Before the first N63 production edit, the actual current API comparison failed
with 4032 differences and 4032 refused records, including genuine CHAR_LENGTH(?)
value, empty selection, error and reuse cases. It made zero fallback records.
The exact executed source/observer/fixture was archived before applying the fix.
The candidate now matches all 16416 records, with zero refusal or replay.

## Validation

| Gate | Actual result |
| --- | --- |
| Complete frontend | 2358 passed; exact inherited3 failed;95 ignored;1023.19s |
| Four context original/protocol controls | 4 passed; 2.10s |
| Existing routing/original/scope controls | 10 passed; 6.89s |
| Legal PB/schema original comparisons | 127440 records; zero differences/refusals/replays |
| Original Projection scope | 672 / EA60639F6C88EF85 unchanged |
| Projection | 52 passed; 258.93s |
| Expand | 6 passed; 0.38s; fresh Projection binary SHA authenticated |
| Unistore | 195 passed; exact inherited3 failed;13 ignored;2.04s |
| Actual Session SHOW | 31 passed; exact inherited2 failed;0 ignored;0.02s |
| Production expression/session check | Passed;149 expression warnings |
| make lint | Passed |
| Both nightly formatter checks | Passed for all3 affected Rust files |
| Source/original/restore/protected audit | Passed;627 archived files;one production file |

The new controls additionally prove no entry into old native/scalar trees,
source metadata/node/depth budget refusal, genuine other-owner rejection before
callbacks and SQL-error recovery through the same prepared Suite. Native Int
wrappers, scalar row presentation and disabled-vector whole-operand phases are
compared to the corresponding original API rather than inferred from results.
The three inherited Unistore failures remain two old worker-count expectations
and strict DECIMAL selection. The two SHOW failures retain their original
AUTO_INCREMENT=30001 versus101 and warning String versus Int mismatches.

The unchanged SDK reuses authenticated N60 full1153/exactold1/1ignore and scoped
clippy171total/89expression warnings. This is unchanged-source reuse, not a new
SDK run. Tests are not ignored and expectations are not rebased.

Reproduce from the TiDB repository:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib context_native_closure_tests:: -- --nocapture --test-threads=1
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib routing_ -- --nocapture --test-threads=1
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-session --test all show_ -- --test-threads=8
    make lint

Workspace evidence uses n63-final-*, n63-original-audit.py,
n63-final-check.py, both original pins and the explicit inactive restore receipt.
Temporary setup attempts are retained separately. The final checker authenticates
all source hashes, exact inherited failure sets and unchanged warning counters
before and after the local signed checkpoint. Frontend warnings remain346; the
normal expression library remains149. The inherited frontend failures are IFNULL
pushdown catalog, EXP overflow and vectorized operator duration FSP.

## Next work

Continue the genuine public Unistore SimpleExpr and its seven typed consumers.
Actual ingestion first selects shared PB catalog signatures; this does not
retire the public typed APIs. Capture those original contracts before source
changes, including full i128 intermediates, SQL versus infrastructure errors,
eager-versus-short-circuit argument phases and selected context lifecycles.
Then validate full services and final unified performance. Unit durations and
the separate cgo experiment do not establish final performance.
