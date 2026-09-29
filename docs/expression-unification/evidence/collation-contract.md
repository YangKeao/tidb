# Foundation A — collation/key/LIKE contract and implementation evidence

## Status, scope, and authority

2026-09-28, evidence revision **A-M1-r3 first admitted-domain checkpoint accepted**; interface **A-r1 frozen by parent** after review. The M0 audit below records baseline behavior and proposals; the final M1 section records subsequently authorized implementation and current validation. Parent released implementation after original baselines and four RED expression regressions. Shared TiKV key/pattern APIs, TiDB facade delegation, migrated algorithm deletion, and source-authority checks are implemented. Observed parent logs now show **TiKV collation 17/17; TiDB collation 25/25; public contracts 9/9; original Go key fixture 1/1; expression LIKE 11/11 including all four prior RED regressions**. Parent additionally reports full TiKV RPN 438 passing and caller Decimal bridge 11/11. Parent subsequently reported stringutil 13/13, cache 1/1, and JSON helper suite 18/18, and explicitly released generator-owned pruning. A verified the five obsolete images against HEAD, removed exactly **2,128,092 bytes**, and reran all Python source/retained-GB/absence checks successfully. Post-prune parent gates now pass **61/61 across all 12 nonempty datatype/codec/executor/planner/session/statistics/unistore filters**; A independently inspected their result lines. Parent also reports post-prune Python checking and both repository diff checks exit 0. A ran scoped pinned formatting, diff checks, Python verification, and explicitly authorized generator pruning; **no Cargo/Go builds, commits, resets, manifest/lock/root-export/main-plan edits**. Parent accepts this **first admitted-domain shared implementation + deletion checkpoint** only. Whole-collation, performance, package-transcreation, and overall-plan completion are not claimed.

Pinned worktrees, independently checked with `git rev-parse HEAD` and `git status --short`:

- TiDB root `T = /home/agent/tidb/expression-unification/tidb`, HEAD `364aef2bab5cc633ecb76a775ae8f36f86a6687d`, initially clean.
- TiKV root `K = /home/agent/tidb/expression-unification/tikv`, HEAD `548812e1ef57aef077a2062a9cc356640a6347f5`, initially clean.
- All `T/...` and `K/...` paths below expand against these exact roots. `D` below means `K/components/tidb_query_datatype/src/codec/collation`.

Read in full: `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; both root `AGENTS.md`; TiDB `PLANS.md`, `docs/agents/testing-flow.md`, and `docs/agents/notes-guide.md`; TiKV `doc/maintenance-guides/README.md`, `repo-overview.md`, and `src/coprocessor.md`. Discovery found no deeper AGENTS/PLANS for the affected Rust directories and no `doc.go` in Go `pkg/util/{collate,stringutil}`. The coprocessor guide is the nearest covered subsystem; no dedicated query-datatype guide exists. Parent owns any coordinated guide update for evaluator/ownership contracts. No old `expression-reuse` source or backend was used.

Parent decisions received during M0:

1. Foundation A may own domain-specific `D/mod.rs` trait/exports in addition to the collator and LIKE domain, **after explicit implementation release**. Crate-root exports, Cargo/lock, and main plan remain parent-owned.
2. Normal expression LIKE converges on existing datatype/TiKV semantics. Differing old expression cases are explicit **corrections**, with pre-fix red regression runs, not claims of unchanged behavior.
3. JSON_SEARCH's dangling-escape no-match behavior must be preserved with an explicit matcher policy, not a second native matcher or blanket exemption. Once released, A may change only `like_matches`/`like_matches_from` and directly related tests in the mixed `binary_json_ops.rs` file.

## Actual baseline APIs — not old-experiment APIs

`D/mod.rs:95–153` already provides `Charset::{validate,decode_one,charset}`, `LikePatternMode::{Bytes,BinaryRunes,CollatorDefined}`, and this `Collator` contract:

    type Charset: Charset;
    type Weight: Unsigned;
    const IS_CASE_INSENSITIVE: bool;
    const LIKE_PATTERN_MODE: LikePatternMode;
    fn char_weight(ch: <Self::Charset as Charset>::Char) -> Self::Weight;
    fn like_pattern_compare(a: &[u8], b: &[u8]) -> Result<bool>;
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize>;
    fn sort_key(bstr: &[u8]) -> Result<Vec<u8>>;
    fn sort_compare(a: &[u8], b: &[u8], force_no_pad: bool) -> Result<Ordering>;
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()>;

`like_pattern_compare` defaults to `sort_compare(..., true) == Equal`, with a UCA-specific override. `sort_key` currently allocates then calls `write_sort_key`. There is no key-options/Cow/max-key/raw-key API. There is no `pattern` module yet.

`D/mod.rs:193–245` makes `SortKey::new`, `new_ref`, and option constructors validate through `C::Charset::validate`. `CharsetUtf8mb4::validate` calls `str::from_utf8`. **Do not use SortKey to bridge TiDB raw byte operations.** The raw collator methods currently accept malformed bytes without this up-front validation; `SortKey::new_unchecked` is also not the appropriate adapter.

`K/components/tidb_query_expr/src/impl_like.rs:7–100` already contains malformed-UTF8 canonicalization and one streaming `%` backtracking loop, wrapped in `#[rpn_fn] like<C: Collator, CS: Charset>(BytesRef, BytesRef, &i64) -> Result<Option<i64>>`. Keep the wrapper and its NULL/result integration; move the matching loop, not RPN machinery.

## Frozen semantic boundaries

### Host facade and signed IDs

Keep `T/rust/crates/tidb-datatype/src/collation.rs` public `Collator`, `Collation` methods, `WildcardPattern`, exact-name lookup/fallback, process mode, registry helpers, and wrapping rewrite/restore functions. Keep name derivation/coercibility outside the shared kernels.

TiDB's positive **registry** IDs and TiKV's signed **wire** IDs are different APIs. `Collation::id()` from TiDB must not simply be passed to TiKV `Collation::from_i32`. Use an explicit closed enum mapping for the facade; preserve the existing signed wire mapping for PB/local fields. **Never take abs of a collation ID**, including `i32::MIN`.

Verified in `K/components/tidb_query_datatype/src/def/field_type.rs:109–145` and `K/components/tidb_query_expr/src/lib.rs:102–160`:

| Wire ID | TiKV compare/key collator | LIKE dispatch significance |
| --- | --- | --- |
| `46` | UTF8 binary, no padding | Nonnegative means legacy rune-identity pattern |
| `-46`, `-83`, `-65` | UTF8 binary, ASCII-space padding | Rune identity |
| `63`, `-63`, `47` | Binary bytes, no padding | `63`/`47` still legacy rune pattern; `-63` byte pattern |
| `-47` | Latin1 binary, padding | Rune identity, despite its key Charset being Binary |
| `-33`, `-45` | General CI | Collator-defined |
| `-192`, `-224` | UCA 4.0 | Collator-defined |
| `-255` | UCA 9.0, no padding | Collator-defined |
| `-309` | UTF8 0900 binary, no padding | Rune identity |
| `-87`, `-28` | GBK bin/CI | Rune identity / collator-defined |
| `-249`, `-248` | GB18030 bin/CI | **Bytes** / collator-defined |
| Other nonnegative IDs | Legacy UTF8 binary, no padding | Legacy rune pattern |
| Other negative IDs, including `i32::MIN` | UnsupportedCollation error | Do not silently normalize/fallback |

`map_like_sig` uses the original return field's sign before dispatch. For collator-defined matching, equal argument charsets select that argument charset; otherwise it selects the return charset. Preserve this existing behavior and `like<C,CS>` monomorphizations. Do not infer LIKE mode from a name ending `_bin`, key encoding, or `Collation::is_bin_collation`.

Facade mapping for the first migration slice: DerivedBinary -> `CollatorUtf8Mb4BinNoPadding`; New Binary -> `CollatorBinary`; AsciiBin/Utf8Bin/Utf8Mb4Bin -> `CollatorUtf8Mb4Bin`; Latin1Bin -> `CollatorLatin1Bin`; Utf8GeneralCi/Utf8Mb4GeneralCi -> `CollatorUtf8Mb4GeneralCi`; Utf8Mb40900Bin -> `CollatorUtf8Mb4BinNoPadding`. UCA aliases use `CollatorUtf8Mb4UnicodeCi` and `CollatorUtf8Mb40900AiCi` after their gate. Mapping is metadata only, never a second operation algorithm.

### Padding, ownership, lengths, and hashing

- PAD SPACE means trailing byte `0x20` only. NUL, tab, NBSP, fullwidth space, and malformed suffixes are not generic whitespace to trim.
- Default key respects the collator's padding policy. NoPad is an override to preserve trailing ASCII spaces, **not** an instruction to change weights, collation identity, LIKE, or hashing. 0900 AI/0900 bin and Binary are already no-pad.
- Binary-like `immutable_key` must return `Cow::Borrowed` with the original pointer; padded binary may borrow a shortened prefix. Weighted families return owned keys. `can_use_raw_mem_as_key` is stricter than Cow borrowing: only DerivedBinary/New Binary/New 0900Bin return true in the facade, not PAD binary.
- Preserve Go's reported `max_key_len`: raw binary families use input byte length; General CI uses Go rune count times 2; UCA uses Go rune count times 16; GBK uses times 2; GB18030 uses times 4. Count before trimming and count each malformed byte as one RuneError. Do not use valid-prefix character count or key length.
- The deferred GB18030 PUA path is a historical exception to a universal upper-bound claim: its key can have 5 bytes while `max_key_len` reports 4. Do not add `key.len() <= max_key_len` assertions indiscriminately across deferred GB domains.
- `Collator::sort_hash` is **not** `Hash::hash(sort_key)`. General hashes native `u16` weights; UCA currently hashes `(weight & 0xffff)` as **u128**, not u16. Keys emit big-endian u16 byte streams. Binary hashing uses slice Hash framing. Keep these protocols, even when changing shared preprocessing. TiDB group/join/index consumers keep hashing/encoding **key bytes** via their current codec, not TiKV sort_hash.

### Malformed UTF8 and per-character equality

`D/charset.rs:43–64` decodes malformed UTF8 for LIKE as U+FFFD consuming one byte; `D/collator/mod.rs:37–51` strict `next_utf8_char` instead stops on malformed encoding. `T/.../collation.rs:791–825` contains the same two conventions through strict decode plus tolerant rune counting.

- Binary and padded binary raw compare/key operate on bytes, including malformed bytes.
- General CI and UCA raw key generation stops at the first malformed sequence. Raw compare returns Equal upon encountering malformed input in its comparison loop. For example General `compare([ff], b"x") == Equal`, but its keys are empty and `[00,58]`, respectively. Algebraic compare/key/hash properties are therefore restricted to appropriate valid domains.
- LIKE rune modes equate `[e4]`, `[aa]`, and encoded U+FFFD as single RuneError characters; byte modes keep those bytes distinct. `%` backtracking and `_` use the same unit as literal matching.
- UCA 4.0 whole-string compare considers supplementary characters to have the replacement weight, but LIKE requires supplementary character identity. Expanding one character's key into several weights does not make it match multiple pattern characters (`ß` does not LIKE `ss` under Unicode CI).
- UCA 4.0 long-rune markers use character identity in LIKE. Preserve the existing `UnicodeVersion::like_pattern_match`; never replace it with whole-key equality.
- Escape takes precedence over `%`/`_`, including when the escape is a wildcard or NUL. TiKV escape remains `*escape as u32`; TiDB's byte API widens its u8, while JSON's char API can use a non-ASCII scalar. A normal trailing escape is literal; JSON helper trailing escape rejects.

## Concrete incompatibilities and approved treatment

These minimal cases were first derived from the real code paths. Parent subsequently ran the four test-only regression functions RED, and A read `/home/agent/tidb/expression-unification/logs/tidb-like-regression-red.log:453–498`: all four fail with the exact cached/uncached expression divergences below. GB table bytes are independently observed by the read-only commands later in this document. The JSON helper policy remains source-audited, not runtime-tested by A.

| Minimal call/input | Current TiDB expression/helper | Datatype/TiKV or required result | Treatment |
| --- | --- | --- | --- |
| `like_match_with_collation("", "%", Some(b'%'), Binary)` | true: `like.rs:201–203` skips trailing `%` without checking escape | false | Parent-approved expression correction; cached and uncached tests |
| Same call with `text="a", pattern="a%", escape='%'` | true, same trailing-skip bug | false: final escaped `%` is literal | Parent-approved correction |
| Unicode CI `text="\u{3000}", pattern=" ", escape='\\'` | false: `collation_char_equal` PAD-trims ASCII space through whole-string compare | true: both characters have the same UCA weight | Parent-approved correction; test both Unicode aliases and cached path |
| Unicode CI `text=" ", pattern="\0", escape='\\'` | true: PAD-trimmed empty compares equal to ignorable NUL | false | Parent-approved correction; no PAD preprocessing in LIKE |
| GBK bin `text="😀", pattern="😁", escape='\\'` | true: expression callback compares '?' encoding weights | false: rune identity | Parent-approved correction, although GB compare/key migration is deferred |
| GB18030 bin `text="中", pattern="_", escape='\\'` | true: expression path decodes runes | false: byte wildcard consumes one of three UTF8 bytes | Parent-approved correction; also `_ _ _` without spaces -> true |
| `tidb_datatype::like_matches("\\", "\\", '\\')` | false: trailing escape requires a following char | Normal LIKE is true; **JSON/helper must remain false** | Shared tokenizer has explicit Reject vs Literal trailing-escape policy |
| GB18030 bin key of U+E78D | TiDB `84 31 82 36 00` | TiKV table/kernel `a6 d9` | Defer this compare/key family; do not retake fixtures |
| GB18030 bin key of U+E7C7 | TiDB `81 35 f4 37 00` | TiKV `81 35 f4 37` | Defer; missing NUL plus other PUA mapping differences |
| GB18030 bin key of U+E864 | TiDB `82 35 91 34 00` | TiKV `fe a0` | Defer |

The expression divergence is in `T/rust/crates/tidb-expr/src/like.rs:39–67,123–239`, not the datatype facade. Go corroboration read: `T/pkg/util/collate/bin.go:98–136` explicitly says trailing spaces significant; `unicode_0400_ci_impl.go:64–81` uses raw rune-weight equality/identity; `gbk_bin.go:98–106` embeds derived rune pattern; `gb18030_bin.go:115–123` embeds byte pattern.

Additional preexisting boundaries:

- Both TiDB UCA 0900 `uca_0900_weight` and TiKV `data_0900.rs::char_weight` use a `>` boundary then index a 183969-entry table. U+2CEA1 (183969) therefore panics; this is an existing shared boundary, not silently fixed by M1. Surrogate-codepoint helper/table tests are not public valid-Rust-char inputs; do not use them to claim a valid-input mismatch.
- Pinyin is already a `panic!("implement me")` stub in TiDB for compare/key/pattern/max length, with a should-panic test, and absent in TiKV's supported enum. Preserve and register as preexisting unimplemented, not a successfully migrated collation.
- `T/rust/crates/tidb-unistore/src/cophandler.rs:4886–4918` currently converts bad UTF8 to empty string, lowercases text for CI, and uses the JSON helper for `SimpleSig::Like`. Sharing that helper removes a matcher, **not** this incorrect surrounding unistore evaluator. Parent/Integration D must remove this SimpleSig path in M4 and test real raw bytes/collator behavior. A does not own cophandler.rs.
- Read-only binary-image comparison found GBK CI tables byte-identical after endian normalization and GB18030 CI tables exactly byte-identical. This supports sharing LIKE character equality for those families; it does not prove all encoded compare/key/transcoding behavior. GBK/GB18030 compare/key residuals remain explicitly deferred for the first slice, with a smaller subsequent CI-only migration possible after actual tests.

## Proposed interface revision A-r1

Parent reviewed and froze this exact public shape as A-r1; this is not a statement these APIs already exist or that production edits are released. Preserve every old public signature. Do not introduce another dynamic backend or feature switch.

### Key APIs and one preparation/writer path

In `D/mod.rs`, add:

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum KeyOptions { #[default] Default, NoPad }

Extend `Collator` with:

    const SORT_KEY_IS_BYTES: bool = false;
    const CAN_USE_RAW_MEM_AS_KEY: bool = false;
    fn preprocess_sort_key(bstr: &[u8], options: KeyOptions) -> &[u8];
    fn write_sort_key_unpadded<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize>;
    fn write_sort_key_with_options<W: BufferWriter>(writer: &mut W, bstr: &[u8], options: KeyOptions) -> Result<usize>;
    fn sort_key_with_options(bstr: &[u8], options: KeyOptions) -> Result<Vec<u8>>;
    fn sort_key_cow(bstr: &[u8], options: KeyOptions) -> Result<Cow<'_, [u8]>>;
    fn max_sort_key_len(bstr: &[u8]) -> usize;

`preprocess_sort_key` and `write_sort_key_unpadded` are documented implementation hooks. The defaults implement the entire ownership/options contract: `write_sort_key_with_options` prepares once then calls the unpadded writer; old `write_sort_key` calls it with Default; both owned key methods call it; Cow calls the same preparation and borrows if `SORT_KEY_IS_BYTES`, otherwise calls the same unpadded writer. No recursion between defaults and no second trim loop. The writer does not validate and does not allocate a key just to write it. Existing compare can use the same preparation with `force_no_pad` translated to the options, but its comparison loop stays specialized (not `key(a).cmp(key(b))`). Hash can reuse Default preparation while preserving exact existing hash writes.

The byte families set `SORT_KEY_IS_BYTES=true`; only unpadded byte families set `CAN_USE_RAW_MEM_AS_KEY=true`. General/GB/UCA keep it false. Padded families use existing ASCII-only `trim_end_padding` for Default and identity for NoPad; UCA uses `T::preprocess` for Default and identity for NoPad. Rename/retarget the existing collator writer bodies rather than retaining an old and new writer algorithm. GB writer adaptation is API conformance only and must not alter old default bytes or imply TiDB GB migration.

Add shared decoder/count functions under collation, with concrete exports decided in the same owned `mod.rs`:

    pub fn decode_utf8_rune_strict(input: &[u8]) -> Option<(char, usize)>;
    pub fn utf8_rune_count(input: &[u8]) -> usize;

Move the existing strict prefix decoder behind the first function. `next_utf8_char` can be a thin tail-slice adapter; `CharsetUtf8mb4::decode_one` adds one-byte RuneError fallback; count uses that tolerant decoder. TiDB's crate-private `decode_rune`, `rune_width`, and `go_rune_count` retain signatures as adapters because `char_length.rs` and `datum_convert.rs` consume them. They must not retain a duplicate decoder/count loop or compute migrated max lengths locally.

### One LIKE tokenizer and backtracking loop

New `D/pattern.rs`, exported by domain `D/mod.rs`:

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum PatternType { Match, One, Any }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum TrailingEscape { Literal, Reject }
    #[derive(Clone, Copy, Debug)]
    pub struct MatchOptions { pub escape: u32, pub trailing_escape: TrailingEscape }

    pub fn matches_raw<C: Collator, CS: Charset>(target: &[u8], pattern: &[u8], options: MatchOptions) -> Result<bool>;
    pub fn compile<C: Collator, CS: Charset>(pattern: &[u8], options: MatchOptions) -> CompiledPattern;
    // CompiledPattern is Clone + Debug, immutable, Send + Sync, non-generic.
    impl CompiledPattern { pub fn is_match(&self, target: &[u8]) -> Result<bool>; }

    pub fn compile_runes(pattern: &[u8], escape: u8) -> (Vec<char>, Vec<PatternType>);
    pub fn compile_bytes(pattern: &[u8], escape: u8) -> (Vec<u8>, Vec<PatternType>);
    pub fn matches_compiled_runes_with(target: &[u8], units: &[char], types: &[PatternType], equal: impl Fn(char, char) -> bool) -> bool;
    pub fn matches_compiled_bytes(target: &[u8], units: &[u8], types: &[PatternType]) -> bool;
    pub fn matches_runes(target: &[u8], pattern: &[u8], options: MatchOptions) -> bool;

Internal design: a raw-token cursor and compiled-token cursor feed **one** generic greedy/backtracking loop; raw and compiled target sources decode as they advance, not into per-row vectors. One raw escape tokenizer is used by raw matching and compilation. Compiled tuple APIs preserve current normalization (`%%` collapse; `%_` reorder to `_%`) because stringutil callers observe the tokens and regex rendering. The raw path need not materialize normalized tokens. Reject can be represented as an always-failing terminal token; it is not a whole second matching loop.

Compiled collator patterns can hold one owned original byte buffer, token literal ranges/codepoints, and monomorphized decode/equality function pointers. Preserve original literal bytes as well as decoded codepoints: for the existing exotic `C=GeneralCi, CS=Binary` combination, re-encoding a decoded byte as a Unicode scalar would change the current `like<C,CS>` behavior. Move `char_bytes_for_compare` into this module and retain its exact condition/canonicalization. Unicode U+FFFD originating from valid three-byte encoding is distinguished from a malformed one-byte unit as today. No unsafe or global shared cache is needed.

`impl_like.rs::like<C,CS>` becomes a call to `matches_raw` with `Literal`, converting bool to `Some(i64)` and mapping the existing codec error. Keep existing NULL behavior generated by `rpn_fn`; do not narrow the escape integer or change `map_like_sig`.

### TiDB adaptation and parent-owned integration

- `WildcardPattern` becomes a thin Clone/Debug wrapper over shared `CompiledPattern`; both facade pattern constructors bind the appropriate decoder/matcher, never implement wildcard loops.
- Add `Collator::like_match(self, target: &[u8], pattern: &[u8], escape: u8) -> bool` for allocation-free dynamic calls. Existing `Collation::pattern` is explicit new-collation behavior, while `Collator::DerivedBinary` is legacy rune behavior; do not unexpectedly reconsult the global mode in these explicit APIs.
- Expr `CompiledLikePattern` stores the datatype pattern instead of weights/types/raw duplicate algorithm state; preserve its constructor/is_match and existing statement cache lifecycle. `like_match_with_collation` delegates the dynamic facade. Remove both the binary fast-path algorithm and `collation_char_equal`. Keep ILIKE ASCII folding/escape preprocessing and wrappers unchanged; their actual matching goes through the same shared path. ILIKE folding itself is not claimed migrated in M1.
- Stringutil retains all public function signatures and token shape. Replace its token enum with a re-export of the shared enum; delegate compile and matching entrypoints. Keep unrelated unquote/regex rendering/ASCII utilities. `decode_go_runes` also serves `escape_glob_question_mark`; if retained there, make it collect the shared decoder rather than leave a second UTF8 decoder for matching. `decode_utf8_prefix` used by unquote is outside the collation algorithm and must not be deleted accidentally.
- `binary_json_ops.rs::like_matches(&str,&str,char)` delegates `matches_runes(..., Reject)`, removing `like_matches_from`, the memo HashMap, and target/pattern Vec<char> allocations. No JSON-path/search traversal is changed.
- Recommended dependency plumbing: parent adds `tidb_query_datatype` only to `tidb-datatype` and re-exports a narrow `tidb_datatype::wildcard` module containing the shared pattern primitives needed by stringutil/JSON. `tidb-util` already depends on datatype; no new util->TiKV manifest dependency is necessary. Domain `collation.rs` can declare the narrow module; parent adds its name to crate-root `pub use collation::{...}`. Concrete TiKV collator types remain in the datatype bridge.
- Codec errors from raw operations are not fallback signals. Vec output and supported collators have no ordinary encoding error to propagate through the existing infallible facade; use a documented expect at that narrow boundary, not lossy UTF8 conversion, catch-and-native replay, or new error swallowing. Writer APIs still propagate actual buffer errors.

## Exact requested ownership after release

No product ownership here authorizes writes before the parent's explicit baseline release. Request these exact files, not whole crates:

**TiKV existing files:**

- `K/components/tidb_query_datatype/src/codec/collation/mod.rs`
- `K/components/tidb_query_datatype/src/codec/collation/charset.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/mod.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/binary.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/latin1_bin.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/utf8mb4_binary.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/utf8mb4_general_ci.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/utf8mb4_uca/mod.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/gbk_collation.rs`
- `K/components/tidb_query_datatype/src/codec/collation/collator/gb18030_collation.rs`
- `K/components/tidb_query_expr/src/impl_like.rs`

**TiKV new file:** `K/components/tidb_query_datatype/src/codec/collation/pattern.rs` (including its unit tests).

**TiDB existing files:**

- `T/rust/crates/tidb-datatype/src/collation.rs` (facade/deletion plus adaptation of embedded source tests)
- `T/rust/crates/tidb-datatype/src/collation_tests.rs`
- `T/rust/crates/tidb-util/src/stringutil.rs` (tests are inline; no separate stringutil test directory exists)
- `T/rust/crates/tidb-expr/src/like.rs` (including inline red/green regressions)
- `T/rust/crates/tidb-datatype/src/binary_json_ops.rs` **only matcher delegation and directly related tests**, not other JSON operations

**TiDB new file:** `T/rust/crates/tidb-datatype/tests/shared_collation_contract.rs` (public API cross-facade tests; aggregate harness discovers it without changing `all.rs`).

**Generated artifact deletion, after corresponding shared domain passes:** parent assigned A ownership of `T/rust/crates/tidb-datatype/scripts/generate_collation_data.py` and coordinated General/UCA image removal once implementation is released and each domain passes its gate. Remove these generated production images through that generator cleanup, not by silently breaking the existing check:

- `T/rust/crates/tidb-datatype/src/collation_data/general_ci_u16_le.bin`
- `T/rust/crates/tidb-datatype/src/collation_data/unicode_0400_u64_le.bin`
- `T/rust/crates/tidb-datatype/src/collation_data/unicode_0400_long_u64_le.bin`
- `T/rust/crates/tidb-datatype/src/collation_data/unicode_0900_u64_le.bin`
- `T/rust/crates/tidb-datatype/src/collation_data/unicode_0900_long_u64_le.bin`

Unchanged ownership: all Cargo manifests/locks, crate-root lib.rs/mod.rs, main plan, shared registry/PB mapping, fixture generator/fixtures, consumer packages, and maintenance-guide coordination stay with parent/assigned owners. Existing `D/collator/utf8mb4_uca/data_{0400,0900}.rs` weight data and GB `.data` files need no initial product changes.

## Old implementation -> single-owner deletion map

| Old production implementation | Single owner after M1 | What remains / deletion condition |
| --- | --- | --- |
| TiDB `collation.rs::general_weight/general_ci_compare/general_ci_key`, GENERAL_CI image | TiKV `collator/utf8mb4_general_ci.rs` | Delete together after General key/compare/pattern/max gates |
| TiDB byte compare/key/immutable/no-trim/max branches, `trim_trailing_spaces` use for migrated families | TiKV binary/Latin1/UTF8 collators with shared key preparation | TiDB retains only enum dispatch; trim may remain solely for explicit deferred GB until that migration |
| TiDB `uca_weight`, `uca_0900_weight`, `long_uca_weight`, `long_weight`, `UcaCursor`, `unicode_*`, `weighted_compare`, `weighted_key`, four UCA images | TiKV `collator/utf8mb4_uca/{mod.rs,data_0400.rs,data_0900.rs}` | Delete only after each UCA source/edge gate, preserving tests via shared API/original Go source checks |
| TiDB datatype `PatternType/PatternMatcher`, `compile_pattern`, `wildcard_match`, matching `go_runes` | TiKV `pattern.rs` + existing collator literal equality | Public WildcardPattern remains a wrapper; GB pattern equality can use shared weights even while GB keys defer |
| TiDB util `compile_pattern_units`, `do_match_units`, match-time `decode_go_runes` | Same TiKV tokenizer/loop/decoder | Keep public tuple adapters, regex generation, and nonmatching utilities |
| TiDB expr `do_match_binary_pattern`, `collation_char_equal`, duplicate CompiledLikePattern matching | Same TiKV pattern implementation via datatype | Keep default escape, ILIKE ASCII preprocessing, statement metadata/cache wrappers |
| TiDB `binary_json_ops::like_matches_from` recursion/memoization | Same pattern loop with Reject trailing escape | Preserve JSON-specific policy and public char escape API |
| TiKV `impl_like.rs` matching loop + char canonicalization | TiKV datatype `pattern.rs` | Only RPN result/NULL/error wrapper remains |
| TiDB `go_rune_count/decode_rune/rune_width` algorithms | Shared decoder/count functions | Existing crate-private helper signatures remain for `char_length.rs`/`datum_convert.rs` |
| TiDB GBK/GB18030 encoded key/compare/transcoder, `chinese_ci_*`, GB images | **Explicit first-slice residual, not migrated** | Retain original behavior; GB18030 counterexamples above block blanket switch; no hidden fallback |
| TiDB Pinyin panic arms | Preexisting unsupported stub | Do not count as implemented/migrated |

Do not delete tests because their former private helper/data constant disappears. `collation.rs` embedded image hash/long-map/source-generator tests need retargeting: test shared public char weights/keys against original Go authority, keep source-generation consistency checks for still-produced GB images, and retain source-table-only assertions (including surrogate map-zero behavior) as source verification rather than add an invalid-Rust-char product API. The exact generator handoff must precede image deletion. Do not regenerate fixtures to match TiKV and do not leave obsolete images as a feature-off production backend.

## Baseline and post-change tests — exact commands, not execution claims

Every command below is **not run by A**. Parent must run under its pinned toolchain/cache/target setup after required fresh-workspace `make bazel_prepare` handling; A does not start a competing heavy build. Counts below are source-discovered test counts where unambiguous, not observed execution counts. Record actual matched/passed/ignored counts, and reject zero-test success. The two Cargo workspaces must both be verified; caller-toolchain success does not replace TiKV-toolchain success.

### Small baseline set

Cwd `K`:

    cargo test --locked -p tidb_query_datatype --lib codec::collation::
    cargo test --locked -p tidb_query_datatype --lib def::field_type::tests::test_collate_from_i32 -- --exact
    cargo test --locked -p tidb_query_expr --lib impl_like::tests

The first filter includes the existing compare/key/Latin1/GB18030/UTF8 decoder tests plus encoding tests (8 `test_*` functions discovered). The signed-ID filter targets one test. `impl_like::tests` has four tests: `test_like`, `test_like_invalid_utf8`, `test_like_pattern_modes`, `test_like_wide_character`. These already exercise legacy `63` vs new `-63`, GB18030 byte matching, GBK/Latin1 rune modes, malformed bytes, non-ASCII escapes, UCA supplementary/long-character distinctions, and wildcard backtracking.

Cwd `T/rust`:

    cargo test --locked -p tidb-datatype --lib collation_tests:: -- --test-threads=1
    cargo test --locked -p tidb-datatype --lib collation::tests:: -- --test-threads=1
    cargo test --locked -p tidb-datatype --test all collation_key_go_vectors::collation_sort_keys_match_go_byte_for_byte -- --exact --test-threads=1
    cargo test --locked -p tidb-util --lib stringutil::tests:: -- --test-threads=1
    cargo test --locked -p tidb-expr --lib like::tests:: -- --test-threads=1
    cargo test --locked -p tidb-expr --lib like_pattern_cache_reuses_only_within_context -- --test-threads=1

Source counts: datatype `collation_tests` 11, embedded `collation::tests` 9, fixture test 1 with 84 collation/sample assertions, util stringutil 9, expr `like::tests` 7, cache test 1. The main-plan broader `--lib collation` remains useful but its count is not inferred here. Global collation-mode tests must be serialized. Read actual manifests: datatype/codec/expr use `autotests=false` and `rust/scripts/aggregate-tests.rs` with `--test all`; util tests used here are inline `--lib`. The codec mode test's old comment claiming per-file process isolation is stale under aggregation: run its exact filter separately.

### Real consumer checks retained, parent schedules after core baseline

Cwd `T/rust`:

    cargo test --locked -p tidb-codec --lib collation -- --test-threads=1
    cargo test --locked -p tidb-codec --test all collation_keys:: -- --test-threads=1
    cargo test --locked -p tidb-codec --test all runtime_collation_mode_source::operational_keys_follow_exact_name_and_process_mode -- --exact --test-threads=1
    cargo test --locked -p tidb-codec --test all codec_package_source::source_enum_set_hash_modes -- --exact --test-threads=1
    cargo test --locked -p tidb-executor --test all index_entry_go_bytes:: -- --test-threads=1
    cargo test --locked -p tidb-datatype --lib enum_set_tests:: -- --test-threads=1
    cargo test --locked -p tidb-planner --lib ranger::points::tests::like_prefix_builds_the_increment_range -- --exact --test-threads=1
    cargo test --locked -p tidb-session --lib tests_collation:: -- --test-threads=1
    cargo test --locked -p tidb-session --lib tests_partition_prune_collation:: -- --test-threads=1
    cargo test --locked -p tidb-stats --test all sample_collector_source::source_sample_builder_collator_gate_and_index_order_match -- --exact --test-threads=1
    cargo test --locked -p tidb-unistore --lib like_follows_collation_case_sensitivity -- --test-threads=1

Representative read source/contracts: codec `package.rs:453–480` preserves COMPACT_BYTES_FLAG/key encoding for String/Bytes/Enum/Set; `join_keys.rs:390–399,515–532` uses max length, borrowed keys, and little-endian variable-length framing; planner `ranger/points.rs:1580–1598` preserves no-trim bounds; datatype `enum_set.rs:160–173,307–345` uses compare for ENUM and key-based HashSet dedup for SET. Executor `tests/index_entry_go_bytes.rs` has 6 tests covering restored data, one captured collation mode, padding, prefix, and handles. Session `tests_collation.rs` includes CI ORDER BY/GROUP BY, implicit/exact collation join ties, LIKE, and string consumers. Statistics gate test checks callback/NULL/order plumbing, not comprehensive shared-key bytes; add independent stats key fixtures if needed, do not overstate this test's coverage.

Existing Go fixture `T/rust/difftests/transaction-tests/fixtures/collation_key_vectors.tsv` and its `generate_collation_key_vectors.go` cover 7 names x 12 samples, but **not** UCA 0900 AI, malformed inputs, or no-trim/max/Cow. The fixture test uses registry ID 192 for the Unicode family; include both 192 and 224 in new mapping tests. Original fixture is unchanged.

### New precise tests to write after release

In TiKV `D/collator/mod.rs` tests, proposed stable names:

1. `test_shared_key_options_and_cow`: Binary/UTF8 no-pad/padded/Latin1/General/UCA matrix on empty, `a `, `a\t`, NBSP, fullwidth space, embedded NUL, and `[ff,20]`; compare old writer, options writer, owned key, Cow content, and emitted length; pointer identity for all borrowed cases. General `a ` Default = `00 41`, NoPad = `00 41 00 20`; Unicode4 = `0e33` / `0e33 0209`; 0900 = `1c47 0209` in both. Sentinel-prefilled Vec proves writer append/returned-byte-count behavior.
2. `test_shared_max_key_len_go_rune_count`: valid multibyte, supplementary, `[ff]`, `[c3,28]`, spaces; General `[ff]` reports 2 though key is empty, UCA reports 16. No trimming in max calculation. Keep deferred GB 5-byte-key caveat separate.
3. `test_shared_raw_kernels_do_not_validate_utf8`: `SortKey<_,CollatorUtf8Mb4GeneralCi>::new([ff])` errors; raw General compare `[ff]` vs `x` returns Equal and key is empty; UTF8 binary raw key preserves ff. Key/compare/hash property checks only on valid supported domains.
4. `test_shared_hash_protocol_is_not_key_hash`: recording Hasher captures typed u16/u128/slice framing on General/UCA/Binary. Preserve old sort_hash writes exactly and distinguish hashing serialized key bytes; do not assert hash collision impossibility.
5. `test_shared_signed_ids_keep_padding_and_like_modes`: exercise `from_i32` and raw kernels without modifying shared field_type.rs; 46/-46, 63/-63, -45, -192/-224, -255/-309, -47/-65/-83, i32::MIN. Existing RPN mode tests remain the signed-63 LIKE check.

In TiKV `D/pattern.rs` tests:

- `test_shared_pattern_raw_compiled_equivalence`: deterministic small alphabet/input/pattern enumeration across supported mode bindings, malformed bytes, `%`, `_`, escapes `0`, `\\`, `%`, `_`, and wide Unicode escape. Include `b"%__X"` vs `"中X"` (byte true, rune false); `%_`/`_%` token normalization; dangling escapes; no byte/rune array per target.
- `test_shared_pattern_trailing_escape_policy`: Literal `("\\","\\",'\\') = true`; Reject same = false; Reject `(r"a%b",r"a\%b",'\\') = true`; Reject `("a", "a\\", '\\') = false`; wide escape `('é', pattern='é', escape='é')` false in Reject and true in Literal.
- `test_shared_pattern_character_equality`: General `😀` vs `😁` true, UCA4 false; UCA4 `ß` vs `ss` false; UCA4 U+321D vs itself true and U+321E false; fullwidth-space vs space true under UCA; space vs ignorable NUL false; `[e4]` vs `[aa]` true in rune modes and false in byte modes.
- `test_shared_pattern_mixed_charset_preserves_literal_bytes`: pin current `C=GeneralCi, CS=Binary` behavior separately, proving compiled literals are not reconstructed from Unicode codepoints.

TiDB tests:

- `expr/src/like.rs::tests::{shared_like_regression_wildcard_escape_at_end, shared_like_regression_unicode_literal_spaces, shared_like_regression_gbk_binary_rune_identity, shared_like_regression_gb18030_binary_byte_units}`: pin the expression correction rows above through both `CompiledLikePattern` and `like_match_with_collation`. These four tests were subsequently added under the parent's test-only release and observed RED in the parent log (details below). No expected-value re-recording.
- `datatype/src/binary_json_ops.rs` inline `shared_like_json_trailing_escape_policy`: use public helper calls for Reject policy and existing JSON_SEARCH cases; this must remain green throughout migration.
- `datatype/tests/shared_collation_contract.rs`: public facade comparison/key/no-trim/Cow/max/raw-key/mode-ID tests using literal independent expected bytes, plus public WildcardPattern normal-LIKE tests, existing positive registry fallback behavior, and signed rewrite/restore with i32::MIN. Proposed filter `cargo test --locked -p tidb-datatype --test all shared_collation_contract:: -- --test-threads=1`.
- Extend existing stringutil inline tests with binary invalid-byte differences, RuneError equality, literal trailing escape, observed token normalization, and customized equality; retain regex-rendering tests.
- Preserve cache test `tests/ilike_info_cast_source.rs::like_pattern_cache_reuses_only_within_context`; it ensures same-context Arc reuse and context replacement. Do not alter the cache owner to accomplish kernel deduplication.
- UCA promotion additionally needs shared weight/key comparison to original Go for valid scalar coverage (including long expansions, ignorable characters, Hangul, implicit weights and the exact existing table-end panic), beyond the 84 existing fixture rows. Test-only oracle generation must not produce another production backend.

After product changes, parent also schedules both-workspace formatting, TiDB `make lint`, and relevant TiKV quality gates per repo instructions. No claim here that those checks are satisfied.

## Commands actually executed and evidence

All shell commands used cwd `/home/agent/tidb`. Read/glob/grep tools supplied the line-numbered source inspection above. No shell command exited nonzero. Two speculative file lookups (`tidb-util/tests/stringutil`, datatype `src/set.rs`) returned no-such-path tool errors and were corrected by glob discovery to inline stringutil tests and `src/enum_set.rs`; these are not build failures.

Repository check command (exit 0):

    git -C expression-unification/tidb rev-parse HEAD && git -C expression-unification/tidb status --short && git -C expression-unification/tikv rev-parse HEAD && git -C expression-unification/tikv status --short

Output was only the two pinned SHA lines above; both status outputs were empty. Initial `pwd` and `ls -la` inspected the workspace and both worktree roots (exit 0).

Read-only binary weight inspection (exit 0, no Rust/Go code execution):

    python3 -B - <<'PY'
    from pathlib import Path
    base = Path('/home/agent/tidb/expression-unification/tikv/components/tidb_query_datatype/src/codec/collation/collator')
    for name, width, endian, codes in [
        ('gb18030_bin.data', 4, 'little', (0xE78D, 0xE7C7, 0xE864, 0x4E2D)),
        ('gbk_bin.data', 2, 'big', (0x20AC, 0x1E3F, 0x4E2D)),
    ]:
        data = (base / name).read_bytes()
        print(name, 'bytes=', len(data), 'endian=', endian)
        for cp in codes:
            weight = int.from_bytes(data[cp*width:(cp+1)*width], endian)
            print(f'  U+{cp:04X}: {weight:0{width*2}X}')
    PY

Observed: GB18030 table 4,456,448 bytes, E78D=0000A6D9, E7C7=8135F437, E864=0000FEA0, 4E2D=0000D6D0; GBK table 131,072 bytes, 20AC=0080, 1E3F=003F, 4E2D=D6D0. GBK table reads are **big-endian u16**, GB18030 **little-endian u32**. Emitted key bytes are a distinct format.

Read-only CI table equality check (exit 0):

    python3 -B - <<'PY'
    from pathlib import Path
    root = Path('/home/agent/tidb/expression-unification')
    tidb = root/'tidb/rust/crates/tidb-datatype/src/collation_data'
    tikv = root/'tikv/components/tidb_query_datatype/src/codec/collation/collator'
    a = (tidb/'gb18030_chinese_ci_u32_le.bin').read_bytes()
    b = (tikv/'gb18030_chinese_ci.data').read_bytes()
    print('GB18030 CI exact little-endian image equality:', a == b, len(a), len(b))
    a = (tidb/'gbk_chinese_ci_u16_le.bin').read_bytes()
    b = (tikv/'gbk_chinese_ci.data').read_bytes()
    nb = b''.join(b[i:i+2][::-1] for i in range(0,len(b),2))
    print('GBK CI endian-normalized image equality:', a == nb, len(a), len(b))
    if a != nb:
        differences = [(i//2, a[i:i+2].hex(), nb[i:i+2].hex()) for i in range(0,min(len(a),len(nb)),2) if a[i:i+2]!=nb[i:i+2]]
        print('First GBK CI differences:', differences[:8], 'count=',len(differences))
    if (tidb/'gb18030_chinese_ci_u32_le.bin').read_bytes() != (tikv/'gb18030_chinese_ci.data').read_bytes():
        a = (tidb/'gb18030_chinese_ci_u32_le.bin').read_bytes(); b = (tikv/'gb18030_chinese_ci.data').read_bytes()
        differences = [(i//4,a[i:i+4].hex(),b[i:i+4].hex()) for i in range(0,min(len(a),len(b)),4) if a[i:i+4]!=b[i:i+4]]
        print('First GB18030 CI differences:',differences[:8], 'count=',len(differences))
    PY

Observed: `GB18030 CI exact little-endian image equality: True 4456448 4456448`; `GBK CI endian-normalized image equality: True 131072 131072`. This is table evidence, not runtime equality or performance evidence.

## Parent baseline checkpoint and test-only release

Parent reports original TiDB baseline: broad datatype collation filter **25/25 pass**, Go key fixture **1/1 pass**, complete expression lib suite **1226 pass, 4 unrelated preexisting failures, 93 ignored**. Those are parent-reported results; A did not run or independently review the logs. They supersede source-only count estimates for those particular parent filters, not the narrower proposed commands above.

Parent authorized only test additions in `T/rust/crates/tidb-expr/src/like.rs`, with stable prefix `shared_like_regression`. A added four test functions and one assertion helper inside the existing `#[cfg(test)]` module, **74 inserted lines and one test import line replaced**. There are 22 oracle rows, each exercising both cached and uncached matching and reporting all mismatches before failure. No production prefix changed. A checked `git diff --check -- rust/crates/tidb-expr/src/like.rs` (exit 0), plus `git diff --stat`/`--numstat` for that same path. No formatter or build was run. Parent was notified to run, from `T/rust` under its baseline toolchain/environment:

    cargo test --locked -p tidb-expr --lib shared_like_regression -- --test-threads=1 --nocapture

**Observed RED receipt:** parent ran the regression filter and reported exit 101. A read `/home/agent/tidb/expression-unification/logs/tidb-like-regression-red.log:453–498`: `0 passed; 4 failed; 0 ignored; 0 measured; 1323 filtered out`. All four names match the intended tests, and all 16 divergent oracle rows report the predicted result on both cached and uncached paths; the six control rows do not report a mismatch. The requested CLI is above; exact parent environment/launcher is owned by the parent build ledger. No production algorithm has changed. A also reviewed `git diff --unified=0 -- rust/crates/tidb-expr/src/like.rs`; the only hunk is in the test module.

TiKV baseline initially stopped before datatype compilation on modern CMake rejecting old grpcio-sys/c-ares minimum-policy compatibility. Parent reports the tool-suggested build environment `CMAKE_POLICY_VERSION_MINIMUM=3.5` got past that issue; native gRPC Abseil then failed on GCC16 transitive-header errors. Parent is diagnosing build environment only, with no algorithm/vendor source edits. All TiKV files and TiDB production code remain read-only to A.

## Historical M0 handoff

Parent froze A-r1 and assigned the generator/image-cleanup handoff, with crate-root wildcard export and Cargo retained by parent. The four tests were captured RED before production release. Parent subsequently completed original TiKV baselines (reported collation 8/8, RPN 428/428, targeted Decimal exit 0) and released M1 implementation. The earlier baseline paragraphs describe that point in time, not current write authority.

## M1 implementation checkpoint — A-M1-r1

### Implemented authority and deletion map

| Old owner/algorithm | Current authority / retained adapter |
| --- | --- |
| TiKV `Collator::write_sort_key` implementations | One `write_sort_key_unpadded` hook per collator, reached through shared preprocessing/options; original `write_sort_key`/`sort_key` delegate Default; COW/no-trim/max/raw-memory capabilities are additive trait APIs |
| Duplicate strict UTF8 decoders in TiKV collator/charset and TiDB collation | `D/charset.rs::decode_utf8_rune_strict`; tolerant Go rune decoding/counting is centralized beside it; TiDB crate-private decode/rune-count entrypoints remain wrappers |
| TiKV `impl_like.rs` matcher | `D/pattern.rs` has ONE escape tokenizer and ONE greedy/backtracking loop, generic over raw byte offsets or compiled token indices; public RPN signature/escape i64-to-u32 cast are unchanged |
| TiDB collation `PatternMatcher`, `compile_pattern`, `wildcard_match`, `go_runes`, native WildcardPattern storage | Thin `WildcardPattern(shared::pattern::CompiledPattern)`; explicit registry enum mapping and LIKE modes select shared bindings; `Collator::like_match` is allocation-free raw matching |
| TiDB `general_weight`, `general_ci_compare`, `general_ci_key` | Shared General CI char weights/compare/writer |
| TiDB `uca_weight`, `uca_0900_weight`, `long_weight`, `long_uca_weight`, `UcaCursor`, `weighted_compare`, `weighted_key`, Unicode compare/key wrappers | Shared UCA 4.0/9.0 char weights/compare/writer; all five General/UCA include_bytes references removed |
| TiDB expression `do_match_binary_pattern`, `collation_char_equal`, native compiled pattern fields | Raw facade call and immutable shared WildcardPattern wrapper; statement cache ownership and ILIKE ASCII transformation unchanged |
| TiDB stringutil `compile_pattern_units`, `do_match_units` | Shared rune/byte tuple compiler/matcher; public `PatternType` re-export; 25 public signatures preserved; regex renderer and unquote decoder untouched |
| TiDB JSON helper `like_matches_from` recursion/memoization | Shared rune matcher with explicit `TrailingEscape::Reject`; normal SQL uses Literal |
| General/UCA image generation | Generator emits ONLY GBK/GB18030 residual images; mandatory source verifier compares shared static weights against original Go authority before any generation/check succeeds |

`D/pattern.rs` stores original literal byte ranges as well as decoded codepoints, preserving mixed `C=GeneralCi, CS=Binary` behavior. Raw matching allocates neither compiled tokens nor target-rune arrays; compiled matching stores immutable pattern metadata once and also decodes targets in place. These are source-level allocation properties, **not measured performance claims**.

GB key/compare routines and two GB CI images remain explicitly deferred. Shared GB max-length and LIKE APIs are used, but there is no silent retry/fallback to a TiDB General/UCA algorithm. Facade `expect` calls name the infallible raw/Vec operation; they do not replay another backend after an error.

Parent added the crate-root `wildcard` re-export and dependency; A edited only domain-owned files. Runtime C's core/lib/types/mod and other validators, Foundation B's Decimal, Cargo/lock files, and all old experimental source remain untouched by A.

### Source-authority and test coverage

- Added TiKV 4 pattern tests and 5 key/options/hash/ID tests under the exact names proposed above. Existing TiKV collation and RPN LIKE test names/bodies remain.
- Parent ran the early shared TiKV checkpoint: **12/12 passed**, 295 filtered, log `expression-unification/logs/tikv-collation-shared-first-checkpoint.log`. That checkpoint included the 4 new pattern tests but preceded the 5 additional key tests. This count is parent-reported; A did not run Cargo.
- Added TiDB public `tests/shared_collation_contract.rs` with 9 tests, including explicit literal key bytes, no-trim/COW/max, mode/ID distinction, byte/rune/CI LIKE, valid-domain key/compare relation, existing U+2CEA1 panic, and JSON trailing-escape policy.
- Retained every old embedded `collation.rs` test name. Runtime unique-weight/Hangul/long-prefix checks now invoke shared char weights; original static marker/long-map/hash/length checks invoke a cached mandatory source verifier. The original Go UCA4 fixture remains the authority. No original Go key fixture was regenerated.
- UCA source verifier compares **65,536 General entries; 65,536 UCA4 entries and 22 long arms; 183,969 UCA9 entries and 27 long arms**, including all raw surrogate slots. It preserves seven original source-pinned lengths/SHA256, General plane references, original UCA4 fixture, markers, uniqueness, Hangul Jamo, nonzero first packed-u64, boundary/implicit metadata.
- Surrogate distinction remains explicit: all 2,048 D800..DFFF raw UCA9 entries are 0xFFFD on both sides; Go absent long-map lookup yields zero, whereas TiKV's unreachable non-scalar helper fallback is 0xFFFD. Rust char entrypoints cannot receive those values. The former Go-private helper test now verifies the source contract plus `char::from_u32` rejection; no unsafe char or false runtime equivalence claim.
- Added 1 JSON helper test and retained 4 pre-fix RED expression regressions with their original expectations.
- Stringutil helper retained all 10 old inline test names and added 4 tests, covering malformed rune-vs-byte behavior, escape/NUL/trailing policies, exact tuple normalization, and custom literal callback ordering. `escape_glob_question_mark` gained malformed/multibyte cases.
- Helper agent `8e1e365a-3ef3-46cb-9e5b-9443219cdda1` owned the generator first, then ONLY stringutil.rs. It reported 25/25 in-memory negative/mocked-generation tests (4.852s); no test artifact written and no Rust builds. A independently reran the actual generator `--check` successfully and reviewed the full script.

### Checks actually run after implementation release

A ran the following exact scoped commands (exit 0), with each `git diff --check` scoped to A-owned files; parent supplies Cargo environments and commands:

From `K`:

```sh
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --config skip_children=true components/tidb_query_datatype/src/codec/collation/mod.rs components/tidb_query_datatype/src/codec/collation/charset.rs components/tidb_query_datatype/src/codec/collation/pattern.rs components/tidb_query_datatype/src/codec/collation/collator/mod.rs components/tidb_query_datatype/src/codec/collation/collator/binary.rs components/tidb_query_datatype/src/codec/collation/collator/latin1_bin.rs components/tidb_query_datatype/src/codec/collation/collator/utf8mb4_binary.rs components/tidb_query_datatype/src/codec/collation/collator/utf8mb4_general_ci.rs components/tidb_query_datatype/src/codec/collation/collator/utf8mb4_uca/mod.rs components/tidb_query_datatype/src/codec/collation/collator/gbk_collation.rs components/tidb_query_datatype/src/codec/collation/collator/gb18030_collation.rs components/tidb_query_expr/src/impl_like.rs
git diff --check -- components/tidb_query_datatype/src/codec/collation components/tidb_query_expr/src/impl_like.rs
```

The TiKV formatter was run again on `collator/mod.rs` alone after adding the 5 key tests. One attempted edit after formatting was rejected as a stale file observation; A re-read the file before editing. One large targeted replacement in expr/like.rs did not match and made no change; A read the exact current region then replaced it successfully. Neither was a sandbox denial or build result.

From `T`:

```sh
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-08-22-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-datatype/src/collation.rs rust/crates/tidb-datatype/src/binary_json_ops.rs rust/crates/tidb-datatype/tests/shared_collation_contract.rs rust/crates/tidb-expr/src/like.rs
# Helper ran the same formatter/options on rust/crates/tidb-util/src/stringutil.rs only.
git diff --check -- rust/crates/tidb-datatype/src/collation.rs rust/crates/tidb-datatype/src/binary_json_ops.rs rust/crates/tidb-datatype/tests/shared_collation_contract.rs rust/crates/tidb-datatype/scripts/generate_collation_data.py rust/crates/tidb-expr/src/like.rs rust/crates/tidb-util/src/stringutil.rs
python3 -B rust/crates/tidb-datatype/scripts/generate_collation_data.py --check
```

Source check stdout reports exact shared General/UCA agreement, all listed invariants, explicit surrogate distinction, and retained GB-image agreement. Scoped diff/stat review found JSON changes confined to its LIKE helper and new adjacent test. Stringutil helper independently compared unchanged unquote/regex bodies and all public signatures to HEAD. At that initial checkpoint no image had been regenerated or deleted; the later explicit pruning receipt below supersedes that hold. No running A-owned process exists.

### Parent first cross-caller gate receipt

After the initial checkpoint, parent ran the shared implementations and reported success. A independently inspected the following log results (no A-run Cargo command):

| Gate | Observed result | Log under `expression-unification/logs/` |
| --- | --- | --- |
| TiKV complete collation filter | 17 passed, 301 filtered; all 5 key and 4 pattern additions pass | `tikv-collation-shared-key-contracts.log:7–26` |
| TiDB datatype collation filter | 25 passed, 403 filtered; all old embedded test names retained | `tidb-collation-shared-first-checkpoint.log:1481–1508` |
| Public shared facade contracts | 9 passed, 78 filtered | `tidb-shared-collation-contracts.log:1480–1491` |
| Original Go key fixture | 1 passed, 86 filtered; unchanged fixture | `tidb-shared-collation-go-fixture.log:1481–1483` |
| TiDB expression LIKE | 11 passed, 1316 filtered; all four `shared_like_regression_*` tests are now GREEN | `tidb-like-shared-first-checkpoint.log:2066–2079` |

This closes the intended RED→GREEN proof for the four expression corrections without changing their oracle expectations. Parent also reports caller Decimal bridge 11/11 and full TiKV RPN 438 passing (including the unchanged-signature LIKE wrapper); those are parent-reported, not independently inspected here. The parent's first stringutil invocation used nonexistent `--test all`, so its command-selection exit 101 is **not a product/test failure**; correct target is `--lib stringutil::tests::`. Parent is running that correction plus cache/JSON supplemental gates and a source-verifier check. The five migrated images remain held until parent explicitly releases deletion.

### Gate commands and explicit residuals

All A product files remain held stable. These are the exact targeted gate commands; several have now passed as recorded above, while consumer gates remain separately scheduled:

```sh
# K, using parent's pinned build environment:
cargo test --locked -p tidb_query_datatype --lib codec::collation:: -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_like::tests:: -- --test-threads=1
# T/rust, using parent's pinned build environment:
cargo test --locked -p tidb-datatype --lib collation -- --test-threads=1
cargo test --locked -p tidb-datatype --test all shared_collation_contract:: -- --test-threads=1
cargo test --locked -p tidb-datatype --lib shared_like_json_trailing_escape_policy -- --test-threads=1
cargo test --locked -p tidb-expr --lib like::tests:: -- --test-threads=1
cargo test --locked -p tidb-util --lib stringutil::tests:: -- --test-threads=1
cargo test --locked -p tidb-expr --lib like_pattern_cache_reuses_only_within_context -- --test-threads=1
```

The final parent receipt below closes the targeted post-prune Rust/source-check integration and 12 selected caller filters for this first admitted domain. Broader `make lint`/clippy and performance/allocation/package-size gates remain the parent's overall-plan responsibility; no broader validation is inferred from this checkpoint. A has run no Rust/Go builds; passing runtime receipts were produced by parent and inspected as indicated. Raw deleted source-image bytes are measured below, not a claim about final binary size. No benchmark or whole-package transcreation claim is made.

Read-only post-change search found two genuine native byte wildcard loops outside frozen A ownership: `T/rust/crates/tidb-session/src/privilege/registry_ops.rs::wildcard_match` and `T/rust/crates/tidb-server/src/auth_identity.rs::wildcard_match` (host/grant matching, not expression evaluation). Parent was notified; these mixed security paths are **unmodified explicit non-expression residuals**, not hidden under a repository-wide 'one wildcard loop' claim. `result_schema_projection::wildcard_matches` is unrelated SELECT-star expansion.

Known preserved semantic exceptions: deferred GB compare/key including GB18030 5-byte PUA keys versus 4-byte max estimate; unreachable Go/TiKV UCA9 surrogate-helper distinction; existing U+2CEA1 table-boundary panic; Pinyin unimplemented behavior; malformed-input compare/key algebra differs intentionally. New features do not reinterpret any of these.

## Explicit image-pruning receipt — A-M1-r2

Parent reported the remaining pre-prune gates GREEN: `tidb-util --lib stringutil` **13/13**, LIKE cache reuse **1/1**, `binary_json_ops` **18/18** including trailing-escape policy. Parent independently ran the generator `--check` successfully, then explicitly authorized A to add generator-owned cleanup and prune only the five named obsolete General/UCA images. These supplemental runtime counts are parent-reported, not A-run.

### Generator safety and verification order

`generate_collation_data.py` now accepts mutually exclusive `--check` and `--prune-obsolete` modes. All modes first run the same `encoded_files` source verification: General/UCA original Go/static-table equivalence, pinned lengths/hashes, original UCA4 fixture, markers/long arms, Hangul, surrogate distinction, strict boundary/implicit metadata, and retained GB source hashes. Check and prune then validate the current retained GB image bytes. Only after these gates may prune inspect its exact five-name allowlist.

Before unlinking **any** file, prune validates **every present** obsolete image as a regular nonsymlink file with its original source-pinned byte length and SHA256. Any modified image, directory, or symlink aborts before the first deletion. No glob, recursion, unrelated filename, or GB image is passed to unlink. Prune never calls mkdir/write_bytes; repeated pruning of an already-clean tree is a no-op. `--check` is read-only and now rejects reintroduced obsolete files, including dangling symlinks. Default generation still creates only the two retained GB images.

### Actual pre-cleanup and cleanup commands

From `T`, A ran `pwd` and `git diff --quiet HEAD --` with the five exact obsolete paths (exit 0), then a read-only Python command comparing all seven current image bytes to `git show HEAD:<path>` (exit 0). Every obsolete and retained image was a regular nonsymlink file and byte-identical to HEAD. Thus no unexpected user edit was overwritten/deleted.

The exact destructive command was the generator, not shell removal:

```sh
git diff --quiet HEAD -- rust/crates/tidb-datatype/src/collation_data/general_ci_u16_le.bin rust/crates/tidb-datatype/src/collation_data/unicode_0400_u64_le.bin rust/crates/tidb-datatype/src/collation_data/unicode_0400_long_u64_le.bin rust/crates/tidb-datatype/src/collation_data/unicode_0900_u64_le.bin rust/crates/tidb-datatype/src/collation_data/unicode_0900_long_u64_le.bin
python3 -B rust/crates/tidb-datatype/scripts/generate_collation_data.py --prune-obsolete
python3 -B rust/crates/tidb-datatype/scripts/generate_collation_data.py --check
git diff --check -- rust/crates/tidb-datatype/scripts/generate_collation_data.py rust/crates/tidb-datatype/src/collation_data
git diff --stat -- rust/crates/tidb-datatype/scripts/generate_collation_data.py rust/crates/tidb-datatype/src/collation_data
```

A executed these as one `&&`-chained Bash command; **exit 0**. Both generator invocations reported all original source invariants, GB agreement, and absence of all five obsolete images. `git diff --stat` shows exactly script + five deleted images for this scope: **6 files, script +395/−64 relative to pinned HEAD**, with the image byte deltas below.

| Exact obsolete filename under `rust/crates/tidb-datatype/src/collation_data/` | Deleted bytes | Verified pre-delete SHA256 |
| --- | ---: | --- |
| `general_ci_u16_le.bin` | 131,072 | `787ea411c0600e485ae7dd52ce4b609848b5b832c179f2aed6deaf1e3a173d61` |
| `unicode_0400_u64_le.bin` | 524,288 | `87fbb2751d6afe9ff48b4f19136204846e778dd88a1ba8ef8b2d5398354852b6` |
| `unicode_0400_long_u64_le.bin` | 440 | `fc2ea60aa8caa70d615fcdffaf1d8e1d3d2438eae11847d719266be88bb5d776` |
| `unicode_0900_u64_le.bin` | 1,471,752 | `5ff4831e13e7485cff183e4e9971fd17e2719da0d675f8b38db8f02e89aaee7b` |
| `unicode_0900_long_u64_le.bin` | 540 | `8329421bd84ef04ad3ff5650e6b946d2cb22934d1fded231b7938bb094155c6f` |
| **Total** | **2,128,092** | No final executable-size claim |

A then independently checked `git diff --name-only --diff-filter=D HEAD -- <collation_data>` in Python: its exact set equals the five-name allowlist, all five paths are absent, and both retained files still equal their HEAD blob byte-for-byte. Retained GBK is **131,072 bytes**, SHA256 `f6f63c33fa57eeaffa5d46841694adab58bd9cddfac3f92389dec4564a6036d6`; retained GB18030 is **4,456,448 bytes**, SHA256 `64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d`. File discovery now returns exactly those two `.bin` files.

### Negative/no-mutation checks

Before actual deletion, A ran an in-memory `python3 -B -` unittest harness: **16/16 passed in 0.022s**. It loaded the actual module and real verified outputs, then mocked filesystem mutations rather than changing worktree fixtures. Coverage:

- Exactly five approved removals; retained GB and an unrelated filename preserved.
- Idempotent no-op on an already-pruned tree.
- Last obsolete image with modified length or same-length wrong hash aborts before deleting the earlier valid files.
- Regular symlink, dangling symlink, directory, or stale retained GB aborts without mutation.
- Read-only check rejects all obsolete images, one reintroduced image after mocked pruning, and dangling obsolete symlinks; clean check succeeds without mutation.
- Invalid source aborts all three modes without unlink/write/mkdir; an additional test uses an actual missing shared-source root, not just a mocked verification exception.
- Default generation writes only the two GB outputs; conflicting check/prune flags fail before source processing.

No test file, Rust/Go source, Cargo/root export, original Go fixture, or other product file was changed during this prune release. Only the generator, its five owned obsolete artifacts, and this evidence document changed. No heavy build ran and no background job remains. Parent was notified that the pruned checkpoint was stable for its post-prune collation and downstream gates.

## Final first admitted-domain acceptance receipt — A-M1-r3

Parent accepted the **first admitted-domain shared implementation + five-image deletion checkpoint** after its post-prune Python source check and both repository `git diff --check` commands returned 0. Parent's `bash42` bundle ran all 12 selected runtime filters with nonzero counts: **61 passed total, zero failed/ignored in every filter**. A independently inspected each log's `test result` line with the read-only content-search tool; A did not launch those commands.

All log paths below are relative to `/home/agent/tidb/expression-unification/logs/`:

| Post-prune gate | Passed | Observed log result line |
| --- | ---: | --- |
| Datatype collation/source-authority suite | 25 | `tidb-collation-post-prune.log:1507` |
| Datatype ENUM/SET | 4 | `tidb-enum-set-shared-collation.log:1486` |
| Codec collation library | 4 | `tidb-codec-collation-lib.log:1514` |
| Codec collation keys | 1 | `tidb-codec-collation-keys.log:1511` |
| Codec captured collation mode | 1 | `tidb-codec-collation-mode.log:1510` |
| Codec ENUM/SET key hashing | 1 | `tidb-codec-enum-set-hash.log:1510` |
| Executor Go-byte index entries | 6 | `tidb-executor-collation-index-bytes.log:3785` |
| Planner LIKE range bound | 1 | `tidb-planner-collation-like-range.log:2363` |
| Session collation consumers | 13 | `tidb-session-shared-collation.log:4631` |
| Session collation partition pruning | 3 | `tidb-session-shared-collation-prune.log:4556` |
| Statistics collator gate | 1 | `tidb-stats-shared-collation.log:1761` |
| Unistore LIKE | 1 | `tidb-unistore-shared-like.log:2091` |
| **Total, 12 nonempty filters** | **61** | **All green** |

### Acceptance boundary and continuing ownership

Accepted scope is the explicitly migrated binary/General/UCA raw compare/key/options/COW/max APIs, the shared normal/JSON LIKE tokenizer/matcher and TiDB facades, preserved source-authority/regression contracts, deletion of exactly five obsolete production images, and the targeted caller gates recorded above. It is **not** a whole-collation-family migration, whole-repository wildcard unification, performance acceptance, complete Go-package transcreation, or overall expression-unification completion.

Explicit residuals remain:

- **GB key/compare and charset encoding** are deferred/native compatibility paths; retaining shared GB LIKE/max-length bindings does not admit the whole GB key or charset domain. GB18030 PUA mapping/trailing-NUL differences and the historical 5-byte key versus 4-byte max estimate remain documented.
- **Two security host/grant wildcard loops** remain outside A's released scope: `tidb-session/src/privilege/registry_ops.rs::wildcard_match` and `tidb-server/src/auth_identity.rs::wildcard_match`. Neither was changed or hidden under a repository-wide single-matcher claim.
- The unreachable UCA9 non-scalar helper distinction, U+2CEA1 panic, Pinyin stub, and malformed-input algebra are preserved, not silently redefined.
- Broader quality/performance/packaging gates and future domain admission remain in the parent-owned main ledger.

Parent's next main-ledger phases are **C2a / E-default / B-wide / D-lowering**, not additional A scope. All product files remain held; this acceptance update changed **only this contract receipt**. No further builds or product changes were performed.
