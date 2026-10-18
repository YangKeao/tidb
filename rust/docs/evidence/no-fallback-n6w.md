# N6w: legacy integer/bytes IN argument programs retired

The two legacy native IN adapters now call genuine SDK value/channel services.
Their head and continuation Ready programs, serialized state/reports, i128 byte
surrogates and scoped-context rebinding have been deleted from tikv/in_list.rs.
This removes an additional execution path; global fallback retirement and other
source/function coverage remain incomplete. No push/PR/deployment is authorized.

## Preserved contracts and shared implementation

SDK `native_in_legacy_i128_values` and `native_in_legacy_bytes_values` read the
original caller's actual typed source channels, without manufacturing Exprs or
invoking another evaluator. SDK owns list progress, NULL state, actual equality
and early return. Mandatory index0 remains an actual read for count0/1; even a
non-NULL missing-child reply retains its original result. Full i128 remains
untruncated. NULL base still reads every candidate; a real equality match alone
stops. The separate ordinary wire compatibility profiles keep complete original
validation/protocol and share actual equality/no-match primitives after proof.
No source admission or alternative legacy evaluator lowering is added here.

Bytes retain actual returned owners. Only a real non-NULL pair resolves the
original signed collation ID under the current global mode, after its candidate
callback; NULL pairs never resolve it. SDK compares original bytes rather than
a host-computed Boolean/key. A Source(E) variant preserves the original generic
source error unchanged. Frontend converts only explicit SDK ResourceLimit to
its original ResourceLimit/phaseNone boundary; it does not inspect strings,
reclassify source errors, replay work or invent a failure site.

Bytes use a finite allowance (native default64MiB): returned vector CAPACITIES
are measured before another read, and known native GB binary encoding work is
bounded after actual late policy resolution, before its buffers. Streaming
comparators and empty owners incur no retired frame overhead; integers use
constant stack state. Unknown source allocations are measured after callback
return. This is a conservative logical retained/work bound, excluding shared
immutable table initialization and whole-process allocator recovery. Failures
retain no suspended state, cursor or cache guard; later calls can run normally.

## Independent OLD evidence

Before any production change, actual head/step workers produced:

- 27360 return/demand/context/warning cases, fixed hash03fe1f325a53c789;
- 336 original generic SourceE identity/failure/retry cases, hash7643f3333ea24bfd;
- 160 late mutable-collation cases, hash86adc5e714cb9209.

These hashes
were captured from TiDB762c0152/SDKe5da224 and must NEVER be regenerated from
new value code. The first cohort crosses full-range integers, byte layouts,
NULL placements, empty/single/long lists, twelve signed/unknown collation IDs,
no/fresh/reused caches and deliberately non-NULL absent-child replies. Source
errors include SQL/resource errors in either channel, NULL/non-NULL bases,
counts0/1/2/5/usizeMAX, actual Arc pointer identity and immediate retry.

The mutable-mode test runs itself as the sole test in a child process; it
changes mode during actual candidate reads in both directions and tests NULL
pairs, padding/case, signed IDs and unknown/min/max IDs. It is a normal active
test, with no ignored-test escape and no race with the parallel parent suite.

Only after this capture, all three cohorts independently compared the STILL
ACTIVE original head/step implementation to the new SDK value services. Every
return, warning and context/read trace matched; original source Arc identity
was retained. Only AFTER that passing comparison were the old wire primitives
rewired and the frontend programs deleted. Test-only reproduction patch
n6w-original-legacy-in-baseline.patch targets762c0152/e5da224 and passes cached
apply-check using a separate Git index. Original production snapshots and both
old/independent test snapshots remain in the agent workspace evidence directory.
Both actual Ready lookup traps fail old code and pass after deletion.

## Final gates and logs

All logs below are in the agent workspace evidence/no-fallback/:

- n6w-original-legacy-in-witness.log:OLD value/source cohorts pass.
- n6w-original-late-collation.log:OLD isolated mutable-mode cohort passes.
- n6w-{integer,bytes}-worker-red.log:two actual lookup traps fail old code.
- n6w-old-vs-legacy-values.log:independent comparison of all three cohorts passes
  BEFORE wire/caller replacement (only the two pending worker REDs excluded).
- n6w-legacy-in-front-full.log:1560 pass, exact inherited4 failures,95 ignored,
  zero filtered. Both lookup traps, all fixed OLD witnesses, all169 batch tests
  and new actual-capacity/no-later-read/resource-class/phase/retry tests pass.
- n6w-legacy-in-core-full.log:1073 pass, exact inherited6 failures,one ignored,
  zero filtered. New units cover full i128/count0/1/usizeMAX/source identity,
  NULL demands, early matches, finite policy, empty zero-budget owners, actual
  capacity refusal before another read/policy and GB work refusal before buffers.
- n6w-legacy-in-clippy.log:successful scoped clippy,89 warnings unchanged.
- n6w-legacy-in-unistore.log:both actual cophandler IN consumers pass, retaining
  integer/string collation and NULL behavior through their production adapters.
- n6w-legacy-in-projection.log:all25 production Projection tests pass, including
  mixed31 roots under both vectorization flags and serial/zero-two workers.
- n6w-legacy-in-lint.log:make lint passes. Pinned rustfmt for all5 changed Rust
  files and both repository diff checks pass. All jobs closed.
- n6w-inherited-failures.txt:machine-matched exact frontend/SDK inherited names
  against N6v1. No inherited failure was ignored, filtered or reclassified.

From TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib cophandler::tests::in_
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

From TiKV root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
```

Datatype source unchanged and its suite not rerun. No full Unistore package,
SQL server/real-TiKV or final global-closure claim. Remaining functions/source/
composition/caller/global alternate dispatch and final inherited-failure gates
keep goal/task4 active. Ordinary explicit SDK compatibility recipes are separate
from the retired native caller programs; their presence is not global completion.
