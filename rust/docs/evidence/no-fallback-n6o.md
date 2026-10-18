# N6o — quotient value kernels

Status: verified preparatory local checkpoint;
MOD, / and DIV still need frontend source integration and adapter retirement.
The global migration is active. No push or PR is authorized.

The SDK now exposes the actual integer DIV/MOD value leaves with both source
signedness flags, a full-f64 quotient leaf with a dedicated Divide/Modulo
operation, bounded Decimal MOD and Decimal '/' value/disposition entries.
Existing native RPN workers delegate to these entries. Native real overflow
still carries the genuine operation-specific NativeBinaryArithmeticError;
zero yields computed None before any nonfinite result check. Integer signed
MIN remainder, mixed signedness, and unsigned result bits keep their existing
policies. Existing legacy arithmetic remains a separate semantic caller of the
same relevant primitive, preserving its original panic and nonfinite rules.

Decimal DIV's arithmetic body now accepts borrowed NativeIdentityRef values
and returns NativeIntDivValue: an owned computed warning and the actual integer
outcome. The existing RPN adapter decodes its input frames, retains its original
wrong-kind rejection, and encodes this computed report. Conditional probe/fallback getters remain caller-owned and
must match the shared classifier; one read must never substitute for another.
RHS raw zero still precedes lhs validation and precision access. The direct
zero outcome needs no frame allocation and can succeed with a zero buffer
allowance; the wire adapter continues to charge its actual report allocation.

This step adds no Expression callback, ready-child object, runtime compilation,
new SDK program or second arithmetic algorithm. Frontend source lowering is
unchanged. The public value APIs are preparation for replacing existing
frontend quotient ready-argument adapters, not a claim that those are retired.

Four new independent expected-result tests cover integer boundaries/signedness,
full real bits and typed nonfinite errors, actual Decimal dispositions/finite
budgets, and borrowed Decimal DIV precision demand/warnings/zero priority.
The existing native and legacy expected-result recipe tests are retained.

Validation logs are under agent workspace evidence/no-fallback/:

- n6o-quotient-core-full.log — the first run retained the six inherited
  failures and exposed one invalid new fixture (one coefficient digit with
  storage_scale4). The fixture now supplies10000/30000, representing exactly
  1.0000/3.0000; its expected warning/demand assertions are unchanged.
  n6o-quotient-core-full-fixed.log —1066 pass, exactly the six inherited
  failures, one ignored. Exact failure names match signed N6n. All four new
  independent expected-result tests pass, alongside the original recipes.
- n6o-quotient-front-full-fixed.log —1510 pass, exactly the four inherited failures,
  95 ignored. Failure names mechanically match signed N6n.
- n6o-quotient-core-clippy.log — scoped clippy passes with89 inherited warnings.
  n6o-quotient-core-clippy-fixed.log — passes again with89 inherited warnings
  after the fixture correction and original wire wrong-kind guard preservation.
- Pinned SDK nightly format checks and both git diff checks pass. All jobs close
  before the local checkpoint is signed.
- n6o-quotient-lint.log — make lint passed.

Commands (from the corresponding TiDB/TiKV repository root):

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash <agent-workspace>/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
make lint
```

Datatype and Production Projection code did not change in this step. Their
previous N6k/N6n evidence remains recorded; neither substitutes for the later
quotient source-integration gates. Inherited failures remain part of the final
migration audit.
