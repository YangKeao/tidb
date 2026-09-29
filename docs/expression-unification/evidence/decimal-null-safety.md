# Decimal NULL safety and mechanical ownership/width receipts

Validation E evidence under `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`, not a second plan or full Decimal-unification claim. Parent accepted the architecture direction, observed the safe RED, and explicitly released this **bounded safety change**. Wide representation/arithmetic is not part of this slice; the Decimal tested at B2.0 GREEN was still fixed-nine and its separate Default implementation is B-owned.

## B2.0 ownership and changed files

Paths relative to `/home/agent/tidb/expression-unification/tikv/`:

| File | E's changes |
|---|---|
| `components/tidb_query_datatype/src/codec/data_type/mod.rs` | Add Default to **Evaluable only**, with a NULL-backing comment. EvaluableRet unchanged. |
| `components/tidb_query_datatype/src/codec/data_type/chunked_vec_sized.rs` | Add Default to exactly two impl bounds; initialize a valid Default payload for NULL; document hidden ownership; retain the safe RED and add two owned-lifecycle regressions. |
| `components/tidb_query_datatype/src/codec/mysql/duration.rs` | Minimal `Default` delegating to `Duration::zero()`. |
| `components/tidb_query_datatype/src/codec/chunk/column.rs` | **Tests only**: mixed/reordered/repeated selected rows, replacement, exact NULL physical cells, complete column bytes, empty selection. |
| `components/tidb_query_datatype/src/codec/data_type/vector.rs` | **Tests only**: cloned vector remains usable after original drop; selected NULLs encode as NIL, not hidden Default Decimal. |

For the B2.0 slice, this evidence file was the only other E output changed. No Decimal, Expr, Codegen, Cargo/export/lock, guide, main-plan or other-agent files were edited in that slice. No builds, Cargo commands or product tests were run by E. All previous tests/benchmarks are retained. The three frozen coverage files keep their accepted hashes; denominator **245**, minimum **221** complete, is unchanged.

Read current TiKV rules and maintenance README/repository overview/coprocessor data-contract guidance. No deeper `components/**/AGENTS.md` was found. Parent should integrate the narrow valid-Default/bitmap ownership note into its owned maintenance-guide update; this evidence is not a substitute for a required product-guide change.

## Observed safe RED (parent-run)

Exact test:

```text
codec::data_type::chunked_vec_sized::tests::test_push_null_uses_default_payload
```

The test's local `#[derive(Clone, Debug)] DefaultMarker(u64)` has only a plain u64 field; every bit pattern, including zero, is valid. Its Default returns marker **7**. It pushes None and checks length 1, false validity bit, getter None, then hidden marker 7. The original zeroed initializer creates a valid marker 0: this is a safe assertion regression, not an invalid Vec/Box/NonZero/owner construction. It is neither ignored nor should-panic.

**RED observed:** parent job43, exit **101**, log `/home/agent/tidb/expression-unification/logs/tikv-null-default-safety-red.log` (read by E):

* Lines 7–8: exactly **one** selected test, FAILED.
* Lines 14–17: hidden-payload assertion, actual **0**, expected **7**. Earlier bitmap/getter assertions were reached and passed.
* Line 24: **0 passed, 1 failed, 318 filtered out**.

The pre-fix source hash was `bc36215bd77cf06d7bfd69ffa39e61a1dbcaba62260eb283be5fe1a06204e039`. Original pre-test source hash was `54f9992a9f50a0707e027222954588599e08755ac7c6d83e79e842de6c8ba88a`. E verified that the RED-stage edit consisted of exactly one added 25-line test; the then-production prefix remained `e8b7ea6c71d15d03d84b9428d2664a9a6e13445923fc585aa6db69ab4cd158a8`.

## Production behavior now prepared

`push_null` first constructs `T::default()`, then appends false validity and the owned value. This removes the invalid assumption that all-zero memory is a valid T. Default construction happens before bitmap mutation. Exactly these bounds change:

* `Evaluable: Clone + Default + ...`; **not** EvaluableRet.
* `impl<T: Clone + Default> ChunkedVec<T> for ChunkedVecSized<T>`.
* `impl<T: Clone + Default> From<Vec<Option<T>>> for ChunkedVecSized<T>`.

The container struct/getter need no additional global bounds. Int/Real/DateTime defaults already exist; E adds Duration default zero. B separately landed `Decimal::default() -> Decimal::zero()` and a Default-parts test proving int_digits 1/sign positive. E read that implementation but did not edit it. This does not add a new evaluator, column enum, numeric backend or Decimal width.

### `set(None)` was not rewritten

The existing setter replaces `data[idx]` with a fresh Default, then marks validity false. It **drops the old payload**, retains the newly initialized Default (which may own allocations), and drops that hidden Default on replacement/truncation/container drop. Repeated `set(None)` constructs a new Default and drops the previous hidden Default. Its body is byte-identical to the fixed source, verified by read-only extraction/comparison.

The new tests make this ownership observable using Vec and Box fields plus per-instance Arc/AtomicUsize Drop counters. There is no global shared test state. A NULL bitmap does not make the hidden value cease to exist: **future heap accounting must include allocations retained by hidden Default payloads**, their clones and their replacement/destruction. No heap accounting is implemented or claimed in this slice.

## Added tests after the initializer became safe

All names below are within `tidb_query_datatype --lib`:

| Fully qualified test | Assertions |
|---|---|
| `codec::data_type::chunked_vec_sized::tests::test_push_null_uses_default_payload` | Retained safe RED; parent-run GREEN recorded below. |
| `codec::data_type::chunked_vec_sized::tests::test_owned_payload_clone_append_truncate_and_drop` | Owned Vec/Box payload, live hidden NULL allocation, deep clone independence, clone destruction, append transfers without drops, truncation drops, final destruction exactly once per owned instance. |
| `codec::data_type::chunked_vec_sized::tests::test_set_none_drops_old_payload_and_retains_default` | Some→None drops old; hidden Default remains live; None→None drops previous hidden value; None→Some drops replacement Default; final Some is dropped. Setter unchanged. |
| `codec::chunk::column::tests::test_decimal_selected_null_cells_are_zero_bytes` | Selected rows `[3,1,0,2,3]` after Some→None/None→Some mutation give `[NULL,7,NULL,0,NULL]`; null bitmap `0x0a`; three NULL cells are exactly forty zero bytes; non-NULL Default zero has int_digits 1; exact column header/payload; empty selection encodes an eight-zero-byte header. |
| `codec::data_type::vector::tests::test_decimal_selected_null_datum_bytes_after_clone` | Same selection survives clone and original drop; exact default datum bytes `[0,6,1,0,0x87,0,6,1,0,0x80,0]`, keeping NULL flags distinct from numeric zero. |

Owned Vec/Box/Drop tests were added **only after** the safe production initializer was installed. They must not be transplanted onto the original unsafe zeroed implementation.

### Exact NULL byte contract

`Column::from_vector_value` uses the validity bitmap: None calls `append_null`, which appends **40 zero bytes**, while Some calls the Decimal cell writer. A hidden Default Decimal with int_digits 1 must not be serialized as NULL. The new test manually constructs the fixed-cell golden rather than deriving expected NULL bytes from Default or from the encoder under test. Non-NULL seven's word bytes use the existing raw cell's native endianness; headers remain little-endian. No new cross-endian physical format is claimed.

Ordinary vector encoding likewise uses the bitmap and emits the single NIL flag. Column/vector **production bytes and control flow are unchanged**; only tests were added there. These assertions do not test wide Decimal yet.

## B2.0 source verification performed by E

No compilation, formatter, clippy, `make dev`, or product test execution was performed. Parent owns serialized Cargo execution. Read-only checks:

```bash
git -C /home/agent/tidb/expression-unification/tikv diff --check -- \
  components/tidb_query_datatype/src/codec/data_type/mod.rs \
  components/tidb_query_datatype/src/codec/data_type/chunked_vec_sized.rs \
  components/tidb_query_datatype/src/codec/mysql/duration.rs \
  components/tidb_query_datatype/src/codec/chunk/column.rs \
  components/tidb_query_datatype/src/codec/data_type/vector.rs
```

Exit 0. Scoped git diff/stat and `python3 -B` hash/reconstruction assertions verify:

* EvaluableRet unchanged; mod.rs changes only Evaluable's bound/comment.
* Duration change only the six-line Default implementation.
* Column/vector production prefixes byte-identical; only test-module additions.
* Setter byte-identical; unsafe NULL `mem::zeroed()` removed; exactly two container impls gain Default.
* Existing tests retained; scoped declaration counts: mod 2, container **8** (original 5 + safe RED + two lifecycle), duration 14, column **10** (original 9 + golden), vector **4** (original 3 + golden).
* The frozen coverage hashes remain unchanged.

Independent read-only review by agent `731bdba3-9991-4429-801a-11fe22355baa` found no blocking source-level compile/ownership/oracle issue in the five-file diff. It independently checked the Drop counts and both exact byte goldens. That reviewer also ran no builds/tests and confirmed the native-endian limitation; this is not a compiler or runtime receipt.

## B2.0 parent-run GREEN receipt

Working directory: `/home/agent/tidb/expression-unification/tikv`. Use the parent's pinned compiler/native-compat/cache wrapper and resource policy.

Focused commands:

```bash
cargo test --locked -p tidb_query_datatype --lib \
  codec::data_type::chunked_vec_sized::tests::test_push_null_uses_default_payload \
  -- --exact --test-threads=1
cargo test --locked -p tidb_query_datatype --lib \
  codec::data_type::chunked_vec_sized::tests:: -- --test-threads=1
cargo test --locked -p tidb_query_datatype --lib \
  codec::chunk::column::tests::test_decimal_selected_null_cells_are_zero_bytes \
  -- --exact --test-threads=1
cargo test --locked -p tidb_query_datatype --lib \
  codec::data_type::vector::tests::test_decimal_selected_null_datum_bytes_after_clone \
  -- --exact --test-threads=1
cargo test --locked -p tidb_query_datatype --lib -- --test-threads=1
```

**B2.0 GREEN observed:** parent job44, exit **0**, `/home/agent/tidb/expression-unification/logs/tikv-null-default-safety-full-green.log` (334 lines; read by E). Line 333 reports **324 passed, 0 failed, 0 ignored, 0 measured, 0 filtered out**. All five E regressions and B's `codec::mysql::decimal::tests::test_default_is_valid_zero` passed (lines 31,111,112,114,121,147). Parent also checked the five-file pinned rustfmt/scoped diff, exit 0, after only rewrapping E's container header comment. E ran no build or product test. This validates the bounded safety slice with the fixed-nine Decimal, not subsequent wide representation or arithmetic work. Parent separately released B2.1 mechanical ownership/checked-width repairs after this GREEN.

## B2.1 mechanical caller slice — initial twelve-file handoff

Parent released exactly twelve additional Rust files, plus updates to this receipt. The target B-owned ABI is Clone/non-Copy Decimal; `frac_cnt()` / `result_frac_cnt()` return u32; `prec_and_frac()` returns `(usize,u32)`. Parent subsequently approved usize for B's private counts while retaining these external getter types. Legacy Decimal encoder precision/scale parameters remain u8. E does not edit the core or its arithmetic policies.

Paths in the table are relative to TiKV `components/`:

| Released file | Mechanical delta |
|---|---|
| `tidb_query_datatype/src/codec/convert.rs` | Three production clones, two test diagnostic/reused-input clones; wide i128 shape comparisons preserving warning/clamp branches. |
| `tidb_query_datatype/src/codec/datum.rs` | Two checked u8 encoder-header conversions, codec Error import. |
| `tidb_query_datatype/src/codec/datum_codec.rs` | Two checked u8 encoder-header conversions; legacy payload writer signature unchanged. |
| `tidb_query_datatype/src/codec/mysql/time/mod.rs` | Clamp the wide u32 scale to MAX_FSP=6 before checked u8 conversion. |
| `tidb_query_datatype/src/codec/mysql/time/interval.rs` | Clone the borrowed Decimal before consuming round. |
| `tidb_query_datatype/src/codec/row/v2/encoder_for_test.rs` | Two checked u8 encoder-header conversions; this support code is compiled. |
| `tidb_query_expr/src/impl_cast.rs` | Two production clones, six reused-fixture clones; two checked u8 conversions for the existing bounded cast-test helper. |
| `tidb_query_expr/src/impl_math.rs` | Two production round-input clones, one AbsDecimal test-input clone to preserve its assertion diagnostic. |
| `tidb_query_aggr/src/impl_sum.rs` | Clone the two enum/set Decimal state outputs. |
| `tidb_query_aggr/src/impl_avg.rs` | Clone the two enum/set Decimal state outputs. |
| `tidb_query_aggr/src/impl_variance.rs` | Clone the two enum/set Decimal state outputs. |
| `tipb_helper/src/expr_def_builder.rs` | Two checked u8 expectations and the explicit trusted literal's legacy-shape precondition. |

Totals: **14 production clones + 9 test-only clones**, **11 checked u8 conversions** (six fallible codec checks, two trusted helper expectations, one post-clamp time expectation, two bounded-test-fixture expectations). No new test cases, changed expected values, numerical algorithms, operation profiles, string/f64 bridges, public encoder signatures or denominator changes. C separately owns `impl_op`'s one required clone; E did not edit it. The five B2.0 safety source files were untouched by E in this round; no mechanical non-Copy test repair there was needed in the static review. At this initial handoff, no additional compiler-repair file had been edited; the subsequent compiler-diagnosed grant is recorded below.

### Checked widths without changing SQL overflow policy

Parent specifically rejected narrowing natural counts to isize and returning a fresh SQL Overflow on conversion failure: that would bypass the existing context's warning/clamp handling. `produce_dec_with_specified_tp` instead compares natural precision/fraction and declared flen/decimal in i128. The source documents that usize/isize counts are at most 64 bits on supported 32/64-bit targets, so these widenings are lossless. The original `ctx.handle_overflow_err` → `max_or_min_dec` clamp, declaration u8/i8 casts, rounding mode, truncate warnings and unsigned-negative-to-zero policy remain unchanged. No extra representation error branch is introduced there.

The three fallible codec sites reject a header field that cannot fit u8 using `Error::InvalidDataType`; they neither wrap nor classify header narrowing as SQL numeric Overflow. Existing Decimal flag-write and encoder-call order/status handling remain unchanged. The infallible trusted PB test helper documents its bounded legacy input requirement and uses checked `expect`, not `as` wrapping. Cast-test fixture checks retain the original u8 test arithmetic/max helpers; actual fixture values/oracles are unchanged. Time's checked expectation is justified by the preceding explicit MAX_FSP=6 clamp, not by assuming all Decimal scales are already small.

### Source verification and remaining gate

* Scoped twelve-file `git diff --check`: exit 0.
* Main E read-only inverse-delta SHA256 checks reconstruct all seven pre-edit datatype/helper files exactly after removing only the listed changes. This proves unrelated code/tests were preserved.
* Delegated agent `731bdba3-9991-4429-801a-11fe22355baa` owned only the five expression/aggregate files for this slice. Its inverse-delta checks reconstruct all five pre-edit files, preserving the pre-existing `impl_cast.rs` changes. Main E verified all five final source hashes.
* The same agent independently cross-reviewed the seven main files, finding no blocking ownership/checked-width/policy issue. This was read-only, with no builds/tests or further writes.
* All three frozen coverage hashes are unchanged. No Cargo/root/core/Expr-Codegen/guide edits, builds, Cargo commands or product tests by E. Formatter checking was subsequently authorized and run as recorded below.

**At the initial B2.1 handoff:** not compiled or tested by E; the joint parent build was pending B's stable core. The time call targets the released u32 getter. Subsequent actual parent test/compile receipts and the explicitly authorized repair scope appear below. B2.0's 324-pass receipt does not itself transfer to this representation change. E requires an additional grant before editing newly diagnosed repair files. This is a source-stable mechanical handoff, not a wide-arithmetic or full-family parity claim.

### B2.1 pinned formatter receipt and stable source hashes

Parent's initial twelve-file pinned rustfmt check exited 1 with exactly three formatting locations: multiline precision error arguments in `datum_codec.rs`, the cloned round-call chain in `mysql/time/interval.rs`, and fixture-width assignment wrapping in `impl_cast.rs`. E fixed **only** those formatting locations using targeted edits.

E then executed the repository-pinned `nightly-2026-01-30` rustfmt binary named by `tools/cargo-tikv`, with `--check --edition 2021 --config skip_children=true` and **exactly the twelve table files**, from the TiKV worktree. This is compiler-tool reuse only, not inspection of any old implementation tree. The check **passed, exit 0**, followed by scoped twelve-file `git diff --check`, also **exit 0**. No build or product test was run. Read-only inverse checks also verify that these three formatting repairs preserve the previously proven mechanical delta; the frozen coverage fingerprints still match.

Post-format stable SHA256s (paths relative to TiKV `components/`):

| File | SHA256 |
|---|---|
| `tidb_query_datatype/src/codec/convert.rs` | `26cd66ad7e443722322cec6753a077faa2df45744e94778f1028c98735cfdb91` |
| `tidb_query_datatype/src/codec/datum.rs` | `51efc7f6572cfd546529a9e67f046b70d5ca8787ee43138a74987c85f6486238` |
| `tidb_query_datatype/src/codec/datum_codec.rs` | `df1d64d705506f497f9507c9700b4acaf3c46675688abe51b25bcead01ea7ad1` |
| `tidb_query_datatype/src/codec/mysql/time/mod.rs` | `97765cf5d7eec3858c1aa1ca0460d6175be955258c2f13e5944ca57a24a33d78` |
| `tidb_query_datatype/src/codec/mysql/time/interval.rs` | `d4e0913edfdf8e6b51c1ad16cffffa4e57e1b888e9f439e1e9636b061ab2304b` |
| `tidb_query_datatype/src/codec/row/v2/encoder_for_test.rs` | `c3b335590eda71803c02d3eef77eca43776ded4c95d5aac0aaf9dffcff92c49d` |
| `tidb_query_expr/src/impl_cast.rs` | `94edbf13e9c600bd55987073dca207090cd89e9d9ad41d3c34610e5c919ec59d` |
| `tidb_query_expr/src/impl_math.rs` | `9ef3e24030593bfe4221f04db3d78b906326fdf9d0b80702ca3e1c9a73d977b4` |
| `tidb_query_aggr/src/impl_sum.rs` | `7b57619f88936470aabd8ccd9990c63b2016bd6a3cf20ad68810607b7babf1d8` |
| `tidb_query_aggr/src/impl_avg.rs` | `cacd4132ccc43f24f29297eec2aa3cf5d2adbc1309ecfdbcc05b77900c6a0f9d` |
| `tidb_query_aggr/src/impl_variance.rs` | `7d7b0c67c663c429b4a3c42cb61010124526693426f35219dd2b28fd529d87e7` |
| `tipb_helper/src/expr_def_builder.rs` | `fde270281b1559972f0893bb522a51df9c6197e22a11b73cc6110700de8819c8` |

## B2.1 compiler-amended ownership scope

E read the actual parent logs (not E-run builds):

* `logs/tikv-b21-datatype-full-v2.log`: **330 passed, 0 failed/ignored/filtered**, result line 339.
* `logs/tikv-b21-codegen-full.log`: **20 passed, 0 failed/ignored/filtered**, result line 27.
* `logs/tikv-c2a-expr-full.log`: **eight compiler errors**, final line 155. This is not an expression test pass. Six diagnosed moves belong to the three additional E files below; the other two `impl_op.rs` test moves are C-owned. The `compat_v1` fixture clone remains parent-owned.

Parent then explicitly granted only these three additional expression source files. E made the following exact ownership repairs:

| TiKV `components/tidb_query_expr/src/` file | Diagnosed repair |
|---|---|
| `impl_arithmetic.rs` | In `test_int_divide_decimal_overflow`, clone lhs/rhs when passing them into the consuming test evaluator, preserving both original assertion diagnostics. Two test-only clones; unchanged cases/oracle. |
| `impl_time.rs` | Clone borrowed arg0 in `from_unixtime_1_arg` and `from_unixtime_2_arg` before the existing consuming helper. Two production clones; unchanged FSP casts, argument flow and time/error policies. |
| `impl_miscellaneous.rs` | In `uuid_timestamp`, save the shifted `Res<Decimal>`, clone its dereferenced **Decimal** into round, then clone the dereferenced round payload into the existing Some result. Two production clones; unchanged shift -6, round scale 6, Truncate mode and status-discard behavior. |

### Important correction to the initial feasibility census

The initial **15 external production Copy-dependent expressions in eight files** was **incomplete**. The real compiler exposed **four additional production sites in two further files** (`impl_time` and `impl_miscellaneous`), raising the compiler-amended known count to **at least 19 expressions in ten files**. The arithmetic overflow test adds a third repair file but no production site. The original conditional seventeen-file estimate must **not** be read as a complete implementation footprint or compiler-verified upper bound. Further compiler/test configurations may expose more sites. This correction supersedes the completeness implication of the earlier feasibility count; it does not change the frozen logical-family denominator.

Across E's initial caller slice and this amendment, E has added **18 production + 11 test-only clones in fifteen caller files**. This excludes C's production/test repairs, B's numerical core changes, parent-owned fixture repairs, and the earlier five-file NULL safety slice. It is an ownership-work count, not a claim that the entire migration needs only fifteen or seventeen files.

### Preserve `Res` payload policy explicitly

E read `Res`'s Clone derive, consuming `unwrap`, and Deref implementation. `Res::unwrap` extracts payloads from Ok, Truncated **and** Overflow; converting to Result would apply different error/status policy. Cloning the Res wrapper and then calling a consuming Decimal method through Deref still attempts to move the non-Copy payload. The source therefore uses exactly:

```rust
let shifted = Decimal::from(s * 1_000_000 + ((ns as u64) / 1_000)).shift(-6);
let r = (*shifted).clone().round(6, RoundMode::Truncate);
Ok(Some((*r).clone()))
```

These explicit inner clones mirror the former implicit copies. No status propagation, new error branch, `into_result`, altered test oracle, or arithmetic/profile optimization was introduced.

### Three-file static receipt

Pinned `nightly-2026-01-30` rustfmt `--check --edition 2021 --config skip_children=true` on exactly these three files: **exit 0**. Scoped three-file `git diff --check`: **exit 0**. Read-only inverse-delta SHA256 checks reconstruct all three exact pre-edit sources, proving that only these six ownership sites changed. E ran no build or product tests; the expression rerun is parent-owned and pending at this handoff.

| File | Stable SHA256 |
|---|---|
| `impl_arithmetic.rs` | `da2e813e8a1db53f9b6ea19d3702af4fe7c8e19e1bf3cccc764ed41dcc93a727` |
| `impl_time.rs` | `c9248ff340864809fc35050fadd2da8553f73e4f94b4f770dd5910886f34c349` |
| `impl_miscellaneous.rs` | `6c7a0f6223829d9e9ee4429bc1616fb6640e2f5c5610383a7d89f1ee85b8dfe1` |
