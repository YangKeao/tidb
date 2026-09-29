# Full shared Decimal core — feasibility and recommended design

**Validation E, read-only feasibility evidence; not an implementation approval or a second plan.** The sole plan is `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Only this evidence file was written. Frozen M0 inventory/notes/generator were not changed; the denominator remains **245**, with **221** complete families required. No product edits, builds, Cargo commands, tests, commits, or old reference implementations were used.

## Recommendation

**Expand the existing TiKV `Decimal` into one `Clone`, non-`Copy`, inline-nine-word plus owned-wide value, and make that the only numerical core used by both repositories. Keep a separate fixed 40-byte physical cell.** Reuse the existing `ScalarValue::Decimal`, `VectorValue::Decimal(ChunkedVecSized<Decimal>)`, Datum and RPN/reference machinery. Do not add another value/column enum, wide evaluator, generic evaluator framework, host arithmetic fallback, or coefficient-string mirror.

Use `SmallVec<[u32; 9]>` for the existing base-1e9 words, widen internal digit counts/index arithmetic, and parameterize the **one set of numerical algorithms** by explicit capacity/result-precision policy. Existing bounded TiKV operator APIs retain their contracts. Additional exact APIs support TiDB's existing wide literals/intermediates. The TiDB value wrapper eventually contains only this core plus `declared_shape`; its current numerical methods and fast paths must delegate or disappear.

This is a substantial numerical-core refactor, **not** a three-line buffer substitution. However, the premise that non-`Copy` Decimal requires a sweeping scalar/vector/RPN redesign is false in the audited code. External ownership and layout dependencies are finite and localized: **15 production Copy-dependent expressions in eight files**, **two external physical-width uses**, **one unsafe generic NULL initializer**, and a small set of width-to-codec boundaries. A conditional implementation footprint is **17 named existing Rust files**, plus the SmallVec manifest entry, tests and numerical compatibility extensions described below—not a claimed compiler-error count or guaranteed upper bound.

## Authority and moving-source qualification

* TiDB fixed source: `364aef2bab5cc633ecb76a775ae8f36f86a6687d`.
* TiKV fixed source: `548812e1ef57aef077a2062a9cc356640a6347f5`.
* `DB/` below means `/home/agent/tidb/expression-unification/tidb/rust/crates/`; `KV/` means `/home/agent/tidb/expression-unification/tikv/components/`.
* Read Foundation B's `evidence/datatype-contract.md`, both worktree rules, TiKV maintenance guidance including the coprocessor data-contract guide, and the actual implementations/consumers. Two independent read-only child audits supplied the external TiKV and TiDB consumer censuses.
* Baseline line references are used for the TiKV core unless explicitly marked current. Its fixed source SHA256 is `c3a0eaa2ebb9c23d801255b650514e0fdf8af072b0f3f8497fa6ee9a35bb6aea`. DB Decimal/physical/aggregate files in this audit remained unchanged against the fixed source during inspection.
* B changed the current TiKV core during this audit: it now has `DecimalParts`, checked import/export, a public result-scale getter, zero-product-scale work and seven additional tests in the observed snapshot. It **still** has a nine-word `repr(C)+Copy` Decimal, the size assertion and the two raw-memory chunk routines. Current line numbers move; use the named symbols. Current `DecimalParts` is explicitly bounded logical transport, not the final wide representation or an FFI layout.

## Why the alternatives are inferior

| Option | Can have one algorithm owner? | Decisive implication |
|---|---|---|
| **A. Keep fixed-nine `Decimal` as the evaluator type; add generic/slice word algorithms and a wide facade** | Only if every operation really uses the same workers | It cannot carry wide values through the existing Decimal ScalarValue/VectorValue. Keeping wide values outside those carriers requires an additional evaluator/type path or pervasive storage generics. If the evaluator is changed to the wide facade anyway, the design is effectively B plus an unnecessary second numerical facade. |
| **B. Expand existing Decimal; retain only physical fixed-nine adapters** | **Yes; recommended** | The existing Clone/reference evaluator accepts it. Private slice/word helpers are useful, but storage generics need not escape the Decimal module. Fixed-nine is a capacity/projection policy, not a second engine. |
| Always-owned `Vec<u32>` instead of inline-plus-wide | Yes; genuinely simpler storage | Same safety/width/codec changes, but heap allocation for ordinary literals and clones, unlike both current hot paths. SmallVec removes this avoidable common-case regression with one dependency entry. |
| Bigger fixed array, separate BigDecimal backend, or a Copy arena handle | Not an acceptable simplification here | A larger finite cap does not preserve unbounded value APIs; a second BigDecimal arithmetic backend duplicates algorithms/policies; a Copy handle needs a new arena/lifetime/ownership framework. None solves the task with fewer clear contracts. |

A fixed-nine *compatibility facade* may remain if an external API demonstrably needs it, but it must contain fields and delegate to the same workers. It must not remain the runtime Decimal and cannot retain its own parser/add/mul/div/round/compare/hash/packing implementations. No stringify/parse or f64 transport is acceptable. Parsing actual SQL text and producing final textual SQL/key output are legitimate operations, not a bridge between two numerical implementations.

## Exact dependency census

Counts below have defined units. They are static source-expression/type/call-site counts, not expanded macro instantiations, future compiler errors, runtime coverage, or a promise that compilation has been attempted.

### TiKV production ownership dependencies

Outside `codec/mysql/decimal.rs`, **15 expressions across eight files** depend on owned Decimal being Copy:

| Path under `KV/` | Lines / expression | Count |
|---|---|---:|
| `tidb_query_datatype/src/codec/convert.rs` | 446,455: pass `*self` to decimal rounding; 666–668: save `old=dec`, consume `dec.round`, then compare | 3 |
| `tidb_query_datatype/src/codec/mysql/time/interval.rs` | 761: consuming round on `&self` | 1 |
| `tidb_query_expr/src/impl_cast.rs` | 887,907: return/pass `*val` | 2 |
| `tidb_query_expr/src/impl_math.rs` | 510,519: consuming round on borrowed Decimal | 2 |
| `tidb_query_expr/src/impl_op.rs` | 451: `-*val` | 1 |
| `tidb_query_aggr/src/impl_sum.rs` | 220,297: enum/set result moves `self.sum` through `&self` | 2 |
| `tidb_query_aggr/src/impl_avg.rs` | 229,304: same | 2 |
| `tidb_query_aggr/src/impl_variance.rs` | 330,441: same | 2 |
| **Total** | | **15** |

Keep consuming legacy APIs for the initial representation change; add explicit clone/owned transfers at these boundaries. Borrow-aware new worker APIs can avoid extra cloning without broad signature churn. Test-only reused fixtures also need mechanical ownership fixes; they are not included in this production count.

Inside the fixed core there are **six reviewed direct Copy-dependent expression sites**: four borrowed `self.round` calls in `ceil`/`floor` (965,967,974,976), `convert_to`'s `let tmp=self` followed by consuming round (1157–1158), and Display's `let mut dec=*self` (1972). There is **one owned Decimal Copy derive** (926). `Res<T>`'s conditional Copy derive is not a requirement that every T be Copy and need not be removed. Current fixed `DecimalParts` can remain Copy because it owns only fixed fields/words.

No external owned-Decimal Copy trait bound/derive was found. The important generic contracts already support ownership:

* `data_type/mod.rs:203,219`: Evaluable and EvaluableRet require **Clone**, not Copy. There are five owned Evaluable implementors: Int, Real, Decimal, DateTime and Duration.
* `scalar.rs:29,33`: owned ScalarValue is Clone; Decimal payload is `Option<Decimal>`. ScalarValueRef is Copy but holds `Option<&Decimal>` (197–201); conversion to owned already clones (220).
* `vector.rs:14–25`: VectorValue is Clone. ChunkedVecSized stores a normal `Vec<T>` plus bitmap; append/truncate/replace/drop have ordinary ownership semantics. `to_vec` clones values.
* RPN argument types are references. `types/function.rs:614` asserts `usize` has the size of `Option<&Decimal>`, **not** Decimal's size. Codegen reference-word/lifetime transmutes do not reinterpret the Decimal object. Non-Copy words do not invalidate the reference niche.
* Generic SUM/AVG/variance already clone retained results; FIRST/MIN/MAX do too. The six enum/set paths in the table are exceptions, not a generic aggregate prohibition. Fast hash aggregation owns `HashMap<Option<Decimal>,usize>` keys through `into_owned_value`.

### Layout and unsafe assumptions

| Audited assumption | Exact count / scope | Required disposition |
|---|---|---|
| Existing physical `repr(C)+Copy` 40-byte numerical carriers | **2 types**: DB MyDecimal (`mydecimal.rs:69–87`), KV Decimal (`decimal.rs:919–941`) | DB value Decimal is already Clone/non-Copy. Shared core must stop being a physical cell; physical adapters may remain Copy. |
| Core Decimal-to-memory size assertion | **1** at KV core 922 | Assert the physical cell/byte contract, not `size_of::<wide Decimal>()`. |
| KV core unsafe blocks | **3 total; 2 layout-dependent** | Raw chunk writer 2135–2141 and reader 2293–2300 must become explicit field codecs. UTF8 construction 1966 is unrelated to object layout. |
| External `DECIMAL_STRUCT_SIZE` uses | **2 production uses**, excluding imports | `codec/chunk/column.rs:67` fixed slot; `data_type/vector.rs:293` chunk byte budget. Preserve **40 physical bytes**, independent of Rust value size. |
| Generic NULL initialization | **1 unsafe block** at `chunked_vec_sized.rs:79` | Replace `mem::zeroed<T>()` before introducing owned/enum/Vec storage. Its separate unsafe lifetime extension at 132 is not a layout copy. |
| DB unsafe in the audited physical/value path | **0 blocks, 0 unsafe fn declarations**, including test bodies in the ten-file scope below | DB raw storage is already field-by-field, not a Decimal pointer cast. |
| DB external lossy value-to-chunk calls | **2** | `tidb-chunk/src/chunk.rs:821`, `mutrow.rs:385`; do not move their lossy policy into literal construction or evaluation. |

The DB ten-file unsafe-count scope is datatype `{decimal/mod.rs,decimal/codec.rs,mydecimal.rs}`, chunk `{column.rs,chunk.rs,mutrow.rs,row.rs}`, codec `column.rs`, util `serialization.rs`, exec `aggregate/runtime/spill.rs`. Other unsafe code elsewhere in either repository is not covered by that zero count. KV row-v2 raw readers constrained to `PrimInt` are not Decimal-layout dependencies.

### Mandatory safe NULL change, including exact physical bytes

Use a **narrow Default contract**, not a global return-value framework change:

1. Add Default to **Evaluable only**, not EvaluableRet (which also covers variable-size JSON/bytes/etc.).
2. Int/Real already default; Time derives Default. Implement Decimal Default as numerical zero and Duration Default through `Duration::zero` (`duration.rs:396`).
3. Add Default to the two `ChunkedVecSized` impl bounds: `ChunkedVec<T>` (59) and `From<Vec<Option<T>>>` (125). `push_null` stores `T::default()`. Its struct can remain `T:Sized`; getters remain Clone; current `set` already uses Default. Evaluable bounds supply the remaining reference impl requirements.
4. Correct the container comment claiming all data must reside inline. Vec can safely own wide Decimal after this change.

**Default is an invisible valid NULL payload, not its serialized representation.** Default Decimal may have integer digit count 1; the old zeroed backing value had count 0. Do not encode Default for a NULL row.

The existing byte path already respects this distinction: `chunk/column.rs:228–235` branches on `vec.get_option_ref(row_index)`/bitmap. None calls `append_null`; `append_null:461–465` clears validity and appends **40 all-zero bytes** for a Decimal cell. Some calls `append_decimal`. Default-encoding `vector.rs:396–403` similarly emits the NIL datum flag for None, not a Decimal payload. Preserve both paths. Add a byte-exact test after mixed inline/wide/NULL push, replacement and selected-row encoding: NULL must still have zero physical cell bytes even if its hidden safe payload has different metadata. Never serialize a live owning word buffer or its pointers.

### External width API calls

Exact calls outside the TiKV core, with role separation:

| API | Runtime | cfg(test) | Publicly compiled row test-support | Dev-helper crate | Fuzz |
|---|---:|---:|---:|---:|---:|
| `frac_cnt` | 1 | 4 | 0 | 0 | 0 |
| `prec_and_frac` | 3 | 4 | 1 | 1 | 1 |
| `result_frac_cnt` | 0 | 4 | 0 | 0 | 0 |
| `max_decimal` | 0 | 1 | 0 | 0 | 0 |
| `write_decimal` | 2 | 0 | 1 | 1 | 0 |
| `max_or_min_dec` | 1 | 3 | 0 | 0 | 0 |
| `write_datum_payload_decimal` | 1 | 0 | 0 | 0 | 0 |

Runtime getter sites: `codec/mysql/time/mod.rs:806`; `codec/convert.rs:656`; `codec/datum.rs:1016`; `codec/datum_codec.rs:284`. Encoder sites: datum 1017 and datum_codec 206; its wrapper signature at 205 is explicitly `prec:u8,frac:u8`. Time must clamp the **wide** fraction count to MAX_FSP before converting to u8. Shape comparisons and serialization require checked conversions, not `as u8` wrapping.

Support sites `codec/row/v2/encoder_for_test.rs:464–465` and `tipb_helper/src/expr_def_builder.rs:60–61` each add one precision call and encoder call. The former is publicly compiled despite its name. No getter-width propagation into a generic RPN schema is required. Legacy SQL ROUND casts/clamps in `impl_math.rs` and `convert.rs` still require semantic review; widening storage does not automatically change SQL ROUND policy.

### TiDB coefficient API: exactly five production callers

Workspace census: **15 occurrences in ten Rust files = one definition + five production calls + nine test/support calls**. The production callers are:

| Path under `DB/` | Purpose | Shared replacement |
|---|---|---|
| `tidb-datatype/src/datum_convert.rs:1315` | Source-bound coefficient length paired with visible scale | Retained coefficient digit count; preserve its mixed-scale convention. |
| `tidb-expr/src/cast.rs:450` | Rounded integer width and overflow/truncation diagnostics | Integer digit-count helper with the existing zero convention. |
| `tidb-expr/src/math_fn/mod.rs:807` | CEIL/FLOOR integer width over 18 | Same numerical count, while host result type/declared shape still selects the domain. |
| `tidb-codec/src/decimal.rs:136` | Natural storage precision/scale | Shared natural shape; keep column declaration separate. |
| `tidb-exec/src/aggregate_distinct.rs:81` | Normalized coefficient key bytes | Shared normalization/output, preserving the exact existing key framing. |

Four callers need **no string at all**. A borrowed `coefficient_digits(&self)->&str` cannot survive a core-plus-metadata-only wrapper without a second numerical representation/cache. Approve a narrow internal API change instead. Test/support users can use an owned rendering or inspect parts; do not preserve the borrowed accessor by smuggling a coefficient mirror into the wrapper.

## One coherent proposed numerical API and representation

These are proposed signatures/semantics for owner approval, **not claims about existing APIs**. Keep them in TiKV's existing Decimal module; parent owns Cargo/shared exports.

```rust
// Value, not FFI/chunk layout; Clone, never Copy or repr(C).
pub struct Decimal {
    int_digits: usize,
    storage_frac: u32,
    result_frac: u32,
    negative: bool,
    words: SmallVec<[u32; 9]>, // existing MS-word-first base-1e9 convention
}

// B's existing DecimalParts remains explicitly bounded: u8 fields + [u32; 9].
impl Decimal {
    pub fn try_from_parts(p: DecimalParts) -> Result<Self>; // bounded import
    pub fn try_to_parts(&self) -> Result<DecimalParts>;     // checked, NOT truncating
    pub fn words(&self) -> DecimalWordsRef<'_>;             // wide counts + &[u32]
    pub fn try_from_words(p: DecimalWordsRef<'_>) -> Result<Self>;

    pub fn add_exact(&self, rhs: &Self) -> Self;
    pub fn sub_exact(&self, rhs: &Self) -> Self;
    pub fn mul_exact(&self, rhs: &Self) -> Self;
    pub fn div_rem_exact(&self, rhs: &Self) -> Option<(Self, Self)>;
    pub fn div_round_exact(&self, count: i64, result_scale: u32) -> Self;

    // Existing bounded operators/round/shift/div remain wrappers over the same
    // workers, returning Res<Self> / Option<Res<Self>> as they do today.
    // New source-compatible wrappers use explicit precision/capacity policies.
    pub fn storage_scale(&self) -> u32;
    pub fn result_scale(&self) -> u32;
    pub fn integer_digits(&self) -> usize;
    pub fn coefficient_digit_count(&self) -> usize;
    pub fn natural_storage_shape(&self) -> (usize, u32);
    pub fn coefficient_i128(&self) -> Option<(i128, u32)>;
    pub fn fold_coefficient_i128(&self) -> Option<(i128, u32)>;
    pub fn spill_capacity_bytes(&self) -> usize;
}
```

`DecimalWordsRef` is one borrowed logical view, not another owned value or numerical backend. Its scale/count/partial-word validation is performed before indexing. Keep the used-word count separate from allocation length; numerical loops must not treat inactive capacity as digits. Preserve all nine inactive words for exact fixed-parts round trips when imported, as B's current contract requires. A fresh arithmetic result may initialize its inactive capacity independently.

“Unbounded” means **not limited to nine words or SQL DECIMAL(M,D)**, subject to checked representable allocation/scale sizes, not mathematical infinite memory. Widen integer/digit/word indexing to usize and signed displacement arithmetic to checked wide types. Storage/result scales must cover TiDB's u32 domain. Never use an old u8/i8 cast to admit a wide value. Legacy codec field widths remain explicitly checked boundaries.

**Retire current infallible `to_parts(self)->DecimalParts` for the expanded core.** Keep B's bounded import type and semantics; publish `try_to_parts(&self)` and migrate its initial bridge callers in a coordinated interface revision. Local wide values must travel directly as the shared Decimal, not be forced through this fixed-parts transport. No implicit projection, modulo-u8 count conversion, panic-on-wide getter, or extra ScalarValue variant.

Internally, use one private `WordLimit::{Grow, Fixed(usize)}`/result-precision selection around existing word workers. This is numerical policy, not a `caller_is_tidb` switch. Add/sub/mul/div/remainder/round/shift/parser/compare/normalization/encoding each have one algorithm owner. Borrow input word slices; reserve/resize a destination/scratch buffer before writing. Existing fixed-nine wrappers call those same workers with the old capacity/scale policy; exact APIs select Grow.

### Capacity and scale are different policies

| Semantic surface | Required behavior |
|---|---|
| Literal/value construction | Preserve TiDB `from_literal` precision beyond nine words. Actual text parsing belongs to the shared core; no intermediate TiDB digit string representation. |
| Exact intermediate add/mul, div-rem and AVG helper division | Grow as needed; preserve distinct retained/storage and visible/result scales. Add/mul are not SQL assignment checks. |
| Fixed-nine MySQL operation | Preserve operation-specific preselection/truncation/overflow payload rules through the shared workers; return status plus value. |
| DECIMAL(M,D) cast/assignment | Apply declaration limits/rounding/overflow/warning policy at the existing typed boundary, separately from buffer capacity. |
| Raw/result formatting | Add explicit non-clamping result/storage format helpers. Do not run every raw display through SQL ROUND's 30-digit cap. Legacy SQL wrappers may retain their cap while sharing the formatter/rounder. |
| Legacy chunk/storage/spill | Apply the exact existing boundary's checked or lossy projection, not a universal clamp. Preserve physical bytes and cursor/failure contracts. |

**Do not implement bounded arithmetic as “exact result, then universal clip.”** TiDB bounded multiplication projects operands through `MyDecimalWords::from_decimal` (`decimal/codec.rs:214+`), then chooses/truncates word ranges before multiplication (`decimal/mod.rs:895–1008`); TiKV does the same kind of preselection (`do_mul:803+`). Overflow may deliberately carry signed zero. Carry/rounding behavior can differ from multiplying all digits then clipping. Keep one multiplication loop with selected operand views/output bounds. Exact mode selects full views; bounded mode applies the reviewed legacy operand/output policy in the same owner. Similar operation-specific carry heuristics exist in bounded add/sub.

Division uses one quotient/remainder worker, with explicit requested retained precision and visible result precision. Public TiDB `true_div` is **bounded** (`1321→1328→1195`); `div_round` used by `avg_of_with_div_precision` is a distinct unbounded intermediate policy. Do not accidentally label all division “exact” because the new storage can grow.

### Status, sign, metadata and codec contracts

* Preserve `Res::{Ok,Truncated,Overflow}` and `None` for divide-by-zero. Core computes disposition; the semantic caller controls diagnostics. `Res::unwrap` silently returns any payload, whereas conversion to Result errors on Truncated/Overflow—neither is a universal adapter.
* Keep storage fraction, result fraction and declared column `(flen,decimal)` separate. Value-producing operations clear host declared shape as today; transport preserves it. Do not consume displayed digits in subsequent arithmetic.
* Normal literal/successful zero normalization differs from error/raw zero: `0.000 * -1` must retain scale while clearing its successful zero sign; overflowing `(-10^60)*10^60` has an intentional negative-zero payload. Raw `-0.00` has sign-sensitive Ord/DISTINCT behavior. No global “zero must have positive sign” invariant.
* Current B notes that some legacy overflow payloads export counts beyond nine words and cannot pass bounded parts import. The final owned core must keep **every status payload structurally safe** for its active counts; allocation capacity and an arithmetic nine-word limit are different things. Do not convert a real numeric Overflow into a transport `Unsupported` error or blindly import invalid parts. Preserve observable status/sign/scale and characterize any previously invalid payload before normalizing it.
* TiKV's `RoundMode::HalfEven` currently means first-discarded-digit >=5/half-away-from-zero, not banker rounding. Non-word-aligned ceiling has a source-specific one-digit quirk. One generalized rounder must retain requested policy; renaming or widening must not silently “correct” it.
* Parser prefix acceptance, exponent failures, partial value and status precedence need one shared status-bearing parser. Preserve TiDB's distinct BadNumber/TruncatedWrongValue wrappers, not just an Option value. Binary decode likewise needs consumed length and receiver/error disposition; public legacy Result wrappers can adapt this richer single-core result.
* Fixed 40-byte encoding must read/write explicit fields/words, never an owning Decimal's memory. Match existing native-endian raw-cell bytes; do not imply a new cross-endian wire guarantee. Invalid bool/count/word validation and raw-like-Go storage import remain distinct APIs. Preserve inactive bytes/words where the raw-cell contract requires them.
* Existing chunk-lossy behavior keeps **low 81 integer digits**, or available leading fractional words; e.g. `10^81` becomes zero **at the physical boundary**, not during literal/evaluator transport. Reimplement this numerical projection over shared words, not `format!` plus a second MyDecimal parser.
* Existing exec aggregate spill uses exact `to_my_decimal().expect(...)` (`spill.rs:68–75`), not the chunk-lossy conversion. Do not silently change spill policy. To extend wide spill later, its format/admission requires an explicit compatibility decision; this existing exact-boundary failure is not evidence of a supported wide spill format.

There are **two distinct normalized key protocols** in addition to declared-shape storage encoding: datatype `Decimal::to_hash_key` emits normalized binary payload plus fraction byte; exec DISTINCT emits tag 6, sign, LE-u32 scale and framed normalized ASCII coefficient (`aggregate_distinct.rs:79–96`). Shared core owns normalization and digit/word encoding; host may frame tags. `std::hash::Hash` is neither stable byte protocol. Producing final normalized coefficient bytes for the latter is not a stringify/parse transport bridge.

## Aggregate and fast-path obligations

TiDB exact arithmetic is a real production domain. The audit verified **18 direct Decimal::add calls in seven production files** outside the implementation: datatype `datum_convert.rs` (1), exec `aggregate/runtime/sum.rs` (3), executor `hash_agg.rs` (7), `hash_agg/parallel.rs` (2), `access_path.rs` (2), `kv_table/table_scan.rs` (2), unistore `cophandler.rs` (1). This is a type-reviewed census in those files, not a claimed whole-workspace compiler call graph.

Policies differ and must remain explicit:

* Hash SUM/AVG/merge commonly use unbounded add. Hash AVG finalization uses bounded true_div and discards soft status (`hash_agg.rs:2439–2456`). Public expr `avg_of_with_div_precision` uses unbounded div_round (`lib.rs:749–771`).
* Window numeric state processes arrivals before departures; add/sub are bounded and **both** Truncated/Overflow are fatal (`window_numeric.rs:171–198`). SUM rounds state in place. Do not replace the state machine with generic TiKV SUM.
* TiKV Summable Decimal also errors on both statuses (`summable.rs:31–54`). TiKV AVG emits `(count,sum)`, not the final mean. Sharing numerical workers does not unify executor policies by accident.

Nine concrete fast-path files require migration: six implement arithmetic/packing—datatype `decimal/mod.rs`, `mydecimal.rs`; expr `scalar_function.rs`, `ops.rs`; executor `hash_agg.rs`, `hash_agg/parallel.rs`—and three expose/read it—executor `hash_agg/input.rs`, `stream_agg.rs`, chunk `column.rs`.

Exact raw extraction census: **seven invocation sites across four files**: `to_i128_scaled` at scalar_function 3426 and stream_agg 178/226; `i128_scaled_from_raw_bytes` at chunk/column 610 and hash_agg/input 647; `get_my_decimal_i128_scaled` at hash_agg/input 517/532. `MyDecimal::from_scaled_i128` has **two** production callers, scalar_function 3498 and hash_agg 2667.

Initially delegate/remove host arithmetic shortcuts rather than moving them into another host accumulator. Optimize within the single TiKV owner only after parity/performance measurement. Two existing discrepancies need explicit baseline/oracle adjudication before claiming preservation:

1. Per-row SUM/AVG uses checked i128 addition with fallback, but parallel SUM/AVG merge uses **wrapping_add** (`parallel.rs:2807,2833`). Replacing both with exact add changes overflow behavior. This may be an intended correction, but is not automatically behavior-preserving.
2. Raw MyDecimal i128 extraction uses storage scale without rejecting a result/storage-scale mismatch; `Decimal::fold_coefficient_i128` deliberately declines hidden digits. Treating every fast lane as interchangeable loses evidence.

## File footprint and ownership boundaries

### TiKV: concrete initial representation footprint

The conditional **17-file** set (retaining consuming legacy round, adding narrow Default, widening getters while preserving checked legacy codecs) is:

1. `tidb_query_datatype/src/codec/mysql/decimal.rs` — representation, widened private algorithms, exact/bounded APIs, status/format/parts/physical codecs and tests.
2–9. The **eight ownership files** in the Copy table.
10. `tidb_query_datatype/src/codec/data_type/mod.rs` — Evaluable Default bound only.
11. `tidb_query_datatype/src/codec/data_type/chunked_vec_sized.rs` — two impl bounds, safe NULL and container tests/comments.
12. `tidb_query_datatype/src/codec/mysql/duration.rs` — Default zero.
13. `tidb_query_datatype/src/codec/datum.rs` — checked natural-shape encoding boundary.
14. `tidb_query_datatype/src/codec/datum_codec.rs` — same, with explicit u8 legacy payload boundary.
15. `tidb_query_datatype/src/codec/mysql/time/mod.rs` — clamp wide scale before FSP narrowing.
16. `tidb_query_datatype/src/codec/row/v2/encoder_for_test.rs` — compiled support encoder shape checks.
17. `tipb_helper/src/expr_def_builder.rs` — dev-helper shape checks.

Parent additionally owns the SmallVec dependency declaration in `tidb_query_datatype/Cargo.toml` and any lock update. No new public storage generic or numerical crate is needed. Keep `DECIMAL_STRUCT_SIZE=40` as a documented physical compatibility constant: column/vector can remain unchanged if that alias is retained; renaming it also touches those two files. Add NULL-byte regression coverage in the chunk/vector tests. New heap accounting, full compatibility wrappers, additional tests, and mechanical non-Copy fixture repairs can add files. There is **no proposed codegen or generic RPN redesign**.

Fixed core complexity is measurable: 3,881 total baseline lines; 2,083 nonempty comment/test-masked production lines; 90 production function declarations. It contains 21 `WORD_BUF_LEN` token occurrences, 44 `word_cnt!` invocations on 36 lines, 131 u8 tokens and 45 i8 tokens. These are **audit search counts**, not promised edit counts—wire bytes legitimately remain narrow. The capped `word_cnt!` macro, count arithmetic, temporary indexes, round/shift, parser, division, formatter and codecs must all be reviewed. This is the principal effort, not external Copy repair.

### TiDB: remove numerical ownership, not physical/executor ownership

* `tidb-datatype/src/decimal/mod.rs`: replace digit representation and numerical operations with thin forwarding plus declared-shape metadata. Remove digit-by-digit add/sub/mul/div/round and native i128 lanes once their shared counterparts are proven.
* `tidb-datatype/src/decimal/codec.rs`: delegate numeric projection/normalization/packing/decoding to the single owner; retain host error/shape interfaces. Do not keep MyDecimalWords as a second arithmetic engine.
* `tidb-datatype/src/mydecimal.rs`: retain the exact physical cell/raw-storage interfaces; numerical parsing/rounding/shift/comparison/conversion delegate to shared words. Physical byte validation/packing is not a license to retain a second rounder.
* Change the five coefficient callers and relevant tests. Keep both normalized key formats and declared-shape encoding.
* Replace the nine fast-path files' numerical computations or delegate them; preserve scheduling, column binding, warnings, aggregate/window state and storage layout. Plain count increments, selection and row ownership are not Decimal algorithms.
* B's initial `tikv_compat/value.rs` bridge is an intermediate adapter. Final Datum/RPN transport wraps/clones/moves the shared Decimal directly; no round trip through the bounded DecimalParts path and no resurrected digit-string backend.

### Memory/performance consequences

Do not promise that the new Decimal is 40 bytes. SmallVec plus wide counters may enlarge each inline value and enum; measure the actual target layout and hot-row/aggregate costs. Common <=9-word values should remain allocation-free; spilled clones genuinely copy owned words unless a later, measured scratch/ownership optimization avoids them.

The audited TiKV Decimal/scalar/vector/RPN/aggregate paths have **no existing per-value heap-accounting implementation** to extend. Existing `approximate_encoded_size`, vector default/chunk estimates and runner reserve calls measure **wire output**, not owned memory. Do not add spill bytes to wire budgets.

Expose actual spill-capacity bytes from the core and, if required for integration resource guarantees, account owned scalar/Datum values, each vector element, retained aggregate state and hash keys separately; references do not own another allocation. Mixed inline/wide collections need summed/tracked spill capacity, including clone/replacement/truncate/drop. `tikv_util::memory::HeapSize`'s generic Vec/HashMap implementations sample the first element/key/value; that can undercount heterogeneous wide Decimal collections and must not be treated as exact.

## Validation needed before representation approval/completion

**Nothing in this audit was compiled or executed as a product test.** Source feasibility is not parity. Parent has not approved the full representation change yet. The full-domain acceptance burden is real; it cannot be reduced by shrinking 245 or classifying wide Decimal as host-only.

Required focused evidence:

1. Existing 26-test fixed TiKV Decimal suite; current B parts/zero tests separately; safe parts/status imports, including rejected invalid legacy payloads. Do not replace them with only a few happy-path arithmetic vectors.
2. Inline↔wide boundary around **separately rounded integer/fraction word counts**, not merely 81 total digits; counters/scales >255; 100-digit integers and 101-digit fractions; exact grow add/mul/div-rem and comparisons across representations.
3. Bounded multiplication pretruncation, carry/Overflow payloads, successful zero scale, raw/Overflow negative zero, independent storage/result scales, scale31..81 and larger exact presentation, ties/ceiling/truncate behavior.
4. Hidden-precision 8/7 and 9/7 at result scale7/storage9: AVG **1.21428571350000**; never average displayed strings. Keep `fold_coefficient_i128` rejection tests and characterize raw fast lanes/parallel wrapping separately.
5. Byte-for-byte declared storage keys, both normalized key protocols, raw40 cell goldens, low81 integer clamp and leading-fraction clamp at the original boundary; malformed bool/counts/words; failure consumed length and partial receiver.
6. Mixed None/inline/wide push/set/append/clone/truncate/drop. NULL chunk cells remain **40 zero bytes**, independent of DefaultDecimal metadata. Wire NULL remains NIL. Wide values cannot leak pointers or be silently squeezed into legacy codec widths.
7. Scalar and vector RPN, selected/dead rows, six enum/set aggregate output paths, generic SUM/AVG/variance/FIRST/MIN/MAX, hash-group keys and spill boundary. No second expression engine should be needed to exercise wide values.
8. Source/origin audit proving the host digit algorithms/i128 computations are gone or delegated; no bounded-versus-wide duplicate engine in TiKV. Measure inline allocation count, value width, spilled cloning and aggregate throughput.

### Existing command targets (not run here)

From `/home/agent/tidb/expression-unification/tidb/rust`, through the parent's pinned-toolchain/cache wrapper, or the repository's ordinary equivalent shown below:

```bash
cargo test --locked --offline -j12 -p tidb-datatype --lib decimal_tests:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb-datatype --lib mydecimal::tests:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb-codec --test all decimal_fixed_source:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb-codec --test all unsigned_decimal_key_order:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb-exec --test all core_aggregate_runtime_source:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb-executor --lib hash_agg::window_numeric::tests:: -- --test-threads=1
```

Static declarations for those filters: 58,20,8,5,8,4, with no ignored declarations in the audited groups. **Parent reports the original 58-test Decimal run hit a 20s harness timeout at `test_from_string_my_decimal` after prior successes; that is neither a full pass nor a numerical failure conclusion.** Isolate/bound that test separately; do not repeat a suite timeout as acceptance. Parent is investigating and testing multiplication separately.

From `/home/agent/tidb/expression-unification/tikv`, using the parent's native-compat environment and serialized resource policy:

```bash
cargo test --locked --offline -j12 -p tidb_query_datatype --lib codec::mysql::decimal::tests:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb_query_datatype --lib codec::data_type::chunked_vec_sized::tests:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb_query_aggr --lib impl_sum::tests:: -- --test-threads=1
cargo test --locked --offline -j12 -p tidb_query_aggr --lib impl_avg::tests:: -- --test-threads=1
```

Fixed declarations:26,5,4,5. Current B snapshot had33 Decimal tests, not a redefinition of the original26. Additional chunk regressions include `decimal_datum_overflow_uses_go_truncation_without_panicking`, `decimal_datum_read_back_matches_go_to_string_after_clamp`, `decimal_datum_append_preserves_hidden_fraction_words`, `decimal_cells_round_trip_as_raw_struct_bytes`, `decimal_get_datum_preserves_hidden_fraction_words_and_result_scale`; codec lib has `decimal_decode_keeps_the_cell_result_frac_as_the_visible_scale`. Use actual target discovery for execution; do not mistake a green empty filter for coverage.

## Audit validation and final decision boundary

Executed only read-only discovery/analysis: `pwd`; both `git -C <new-worktree> rev-parse HEAD`; scoped `git diff --stat/--name-only/--numstat`; file `read`/`grep`/`glob`; and `python3 -B` counting fixed `git show SHA:path` sources with the existing read-only lexer. Source counts excluded balanced test modules; the core's cfg(test)-only getter was separately excluded from production counts. Final checks verify this file exists and the three frozen evidence hashes remain unchanged. No generator was added and no product test result is inferred from the source analysis.

**Decision requested of parent:** approve B's follow-on interface revision to one wide-capable existing Decimal, with narrow Copy/NULL fixes and explicit40-byte codecs, then assign its numerical-policy and host-deletion work. The initial bounded bridge remains useful but cannot be called full Decimal unification. The difficult work is preserving one algorithm across exact/bounded operations and observable status/scale/codec policies—not an inherent inability of TiKV's evaluator to hold a non-Copy wide value.
