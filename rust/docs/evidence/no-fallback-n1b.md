# N1b: gate every literal payload before source snapshot copies

TiDB parentdeaddbbe plus this commit; TiKV unchanged24b02fe.

The source preflight previously copied ordinary String, JSON, Vector and Decimal
literals without charging their payload against max_metadata_bytes. Its
representability check itself cloned values before completing the rest of the
source gate. N3b had guarded only the special raw-JSON view.

literal_copy_budget now observes borrowed storage before any literal conversion:
raw string bytes, JSON payload plus tag, vector element bytes, and Decimal
coefficient bytes. It charges the three existing snapshot/copy groups (source
record, lowered literal, compiled literal). Decimal coefficient bytes are a
conservative logical source policy; shared arithmetic word storage may be
smaller. Fixed cells remain in node scaffolding. This is not an exact allocator
capacity, statement memory or transient peak guarantee.

All source/schema/metadata/payload gates now finish before even a temporary
source_literal representability conversion. Small unrepresentable literals
retain Admission; oversized payloads retain typed ResourceLimit and cannot
escape to another evaluator. The existing raw JSON special case uses the same
borrowed gate. SQL kernel algorithms and accepted carriers are unchanged.

Validation:

- Regression failed before the fix for all four payload families under a65536
  byte source policy: String/JSON/Decimal100000 bytes and Vector8192 elements.
- After the fix, all59 focused tests pass. The additional order test confirms
  a later oversized literal is gated before an earlier temporary conversion;
  a small unrepresentable literal still reports Admission.
- Full frontend1411 pass, exactly the same4 inherited failures by name,95 ignored.
- make lint, scoped rustfmt and git diff --check pass. TiKV is unchanged, so its
  N3d full1041/same6 and scoped project clippy evidence remains applicable.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib all_owned_literal_payloads_are_gated_before_source_snapshot_clones
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::batch
    make lint

Raw workspace evidence/no-fallback: n1b-before.log (red), n1b-after-fixed.log,
n1b-front-full.log, n1b-lint.log. The first after candidate failed compilation
only because a test's unwrap_err required Debug on a borrowed internal node;
that log is retained as n1b-after.log. The full run used the fresh release test
binary printed by the focused cargo command.

No-fallback goal remains active. Broader coercions/native datum transport,
context/effect ordering and scalar/AST/filter callers are still pending.
