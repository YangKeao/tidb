# N5a1: checked native value-handle prototype

TiDB parentd6520b85 plus this commit; TiKV unchanged7032b3c.

This is explicitly a test-only prototype in evaluator/batch_values.rs. It does
not change production routing or claim new SQL coverage. Its store owns actual
Datum values and has no expression or evaluation callback. Handles contain a
private ABI header, unique namespace, window generation, entry index and role.
Lookup also checks source-node ordinal and both selected occurrence/physical
row. A new window invalidates old handles even when entry indices are reused.
Ordinary SQL byte values are stored as bytes, never recursively interpreted as
handles. Namespace and generation arithmetic fail rather than wrap.

Insertion inspects borrowed storage before cloning/publishing. Record capacity
is observed directly; native payload charges are a conservative logical policy
(2x byte/coefficient/vector payload) rather than an allocator tracker. External
source owners, shared immutable string backing and SDK transient allocations
are outside this observer. Kernel copying checks the additional payload charge;
no outstanding store borrow needs to cross a native context callback. Consumers
must finish frames/tasks/output before beginning a new window.

All3 prototype tests pass. They verify all19 native kinds without normalizing
NaN bits, Float32 identity, raw Decimal bytes/scales/declaration, JSON tags,
ENUM/SET names and ordinals, raw strings, temporal metadata or vector NaN bits.
Other tests reject truncated/extended/forged, cross-store, stale, wrong-role,
wrong-source and wrong-occurrence/physical-row handles. Resource and counter
exhaustion publish no new handle. The whole frontend has1418 passes and exactly
the same4 inherited failures,95 ignored. make lint, scoped rustfmt and
 git diff --check pass. Core is unchanged; N4b1 evidence remains applicable.

Reproduce from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::batch::native_values
    make lint

Raw workspace evidence/no-fallback: n5a-store-prototype-final.log,
n5a-prototype-front-full-final.log, n5a-prototype-lint.log. Full frontend used the
fresh release binary printed by the focused command.

Next integrate demanded native source bindings and CHAR inputs, retain complete
original schema/metadata checks, and test real executor resource/lifecycle/error
behavior before enabling this transport in production. Global fallback remains;
the overarching goal is active.
