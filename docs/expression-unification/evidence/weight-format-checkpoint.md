# WEIGHT_STRING and FORMAT checkpoint

`weight-format-64` follows `date-core-63`: **204/245** functional delegation/deletion, strict final-audited acceptance **0**.41 eligible families remain;17 more reach221. Overall goal remains active.

## Ownership and shared implementation

Eight exclusive owners, parent integration:23 Rust files (TiKV11/native12), four new CPP sources,13 new tests. Parent additionally owns manifest/lock resolution, generated-output execution, formatting, guides, receipts, Plan and paired publication.

WEIGHT_STRING has three fixed Bytes2 profiles (plain/CHAR/BINARY). The worker owns padding and existing NativeCollation keys. Preparation transports original bytes, signed length, conditional actual packet budget, original collation tag and the global mode captured after original callbacks. Shared classifiers only determine getter/warning demand: equal lengths still read max_allowed_packet; only newly added padding counts against it. Metadata is2 bytes for plain,19 for padded forms. Suppression sends actual non-NULL input/budget with global mode explicitly undemanded; the worker independently returns NULL. No operation selector or ready key/padded answer is encoded in data.

CHAR truncation alone reencodes lossy runes; growth preserves original bytes and adds spaces before keying. BINARY truncates/pads bytes, retains its1292 warning and forces binary collation. Disabled new-collation mode remains unpadded raw comparison policy. Pinyin's original actual-key panic is not converted to an admission refusal.

Numeric-null has a separate Int profile receiving actual static/runtime numeric type metadata, not a fake SQL NULL. Typed evaluation still does not demand the skipped numeric argument; AST still evaluates it and returns NULL even with AS BINARY, unlike typed evaluation. Padding parameters, cast/charset demand and argument collation stay in original order. True SQL NULL uses the existing GetFormatNullNative **Bytes(None)** carrier.

FORMAT's fixed BytesBytesInt profile consumes actual number text, actual nullable locale and unclamped precision. Locale coercion precedes number then precision coercions within one guard. Actual number/precision NULL uses the existing byte-NULL recipe; locale NULL instead means en_US fallback. TiKV owns clamping, half-away rounding and grouping. Original precision conversion, including ties-even/saturation/wrap, is untouched. NULL-locale1649 occurs in preparation; unknown non-NULL1649 occurs only after successful computed bytes via the same shared locale classifier. No JsonReport, invented ErrorFallback or infrastructure-to-SQL-error folding.

Public tidb-mysql locale types/APIs are thin shared facades, preserving arbitrary strings, byte grouping, Unicode Nd and indexing panics. One existing workspace dependency on tidb_query_datatype was added, without a reverse edge or version changes. Go simple lower reuses existing CPP encoding. The pinned source generator moves the64-range Nd authority to CPP locale/digits.rs; native charset delegates. The711-range graph, upper/lower and error tables are unchanged. No hand-edited generated table or fixture rerecording; original round helpers were moved byte-exactly and native copies deleted.

No new driver/carrier/result kind/runtime binding/cause/NoArgs profile, PB/legacy/parser/wire admission or wire policy change. No whole-Go-package transcreation or PR-readiness claim.

## Validation and retained failures

[Exact commands/hashes](../logs/weight-format-summary.txt), [manifest](../checkpoint.json), [ledger](../migration-progress.json).

Twelve Cargo launches:11 completed nonzero test runs (eight green, two known old full-suite RED, one corrected new test RED), plus one corrected SQL-test compile failure. No interruption or zero-match run. Final13 new tests all pass.

- CPP datatype1, weight3, native_format3, local315/1ignored. Filters overlap; these are not unique-test totals.
- Native original public locale SDK2, adapter SDK2, FORMAT roots2, SQL2.
- Full expression1546/4old/94ignored, unistore208/1old/13ignored. Entire prior failure sections/lists remain identical after only numeric panic-thread-ID normalization. All six new expression tests, including both WEIGHT source/demand tests, are explicitly `ok` in the full log.
- SQL pins12 WEIGHT byte results and8 FORMAT results/warnings plus14 direct zero-slot cases. Raw byte comparison uses the unchanged StringDatum::bytes API because SQL chunk materialization can produce String even for binary output. No HEX/provider-output oracle, CAST/WHERE/ORDER masking, or old expectation change.
- All23 sources pass pinned rustfmt and both diff checks. All original test bodies in changed files remain exact. Existing coercions/casts/catalog/PB/legacy/collation kernels, public locale test bank, precision helpers and unrelated generated tables are unchanged. Regenerated Nd ranges equal the old table exactly; moved round-helper bodies also compare byte-exactly.

Corrections are disclosed, not hidden:
1. New FORMAT test incorrectly expected `invalid UTF-8 string datum` for DatumBytes. Unchanged coerce.rs134–140 and an old same-module expectation establish `invalid UTF-8 byte datum`; only two new literals changed. Initial1pass/1fail receipt retained; rerun2pass.
2. New SQL test used nonexistent StringDatum::as_bytes (E0599). Actual datum/mod.rs185–188 exposes bytes(); only the new accessor changed, no expected bytes changed. Original compile log retained; rerun2pass.
3. Initial generation/check succeeded, but formatter wrapped the generated header and the post-format check reported stale output (bash-626). Generator template was fixed, then output regenerated and checks repeated; table data never changed.
4. A lock audit guessed version0.1.0 instead of actual0.0.0; resolved metadata was already correct. The system Go was1.26.5; provisioning pinned1.26.0 hit an outside-workspace cache denial, then succeeded on the exact approved retry. Pins/hashes were not relaxed. Minor source-path/observation prerequisites were corrected without production changes.

## Limits and next work

No runtime production repair was needed after tests began. Moving pure rounding across NULL-locale preparation diagnostics has not established identical physical-allocation/OOM timing. Extra classification/metadata costs, performance neutrality, zero-copy and concurrent global-mode mutation are not claimed verified.

Prior JSON_KEYS aggregate mismatch is unresolved/not rerun. Deep JSON decode precedence/performance, M6/default-NoColumns whole-root closure, workspace/lint/dev/bazel_prepare, release/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and prior parser/GB/vector/Decimal exceptions remain deferred.

Next RO preference: ANY_VALUE+NAME_CONST with all19 actual Datum payloads reconstructed from worker output. No full19-kind codec was found; no cache-original/fakeNULL/UnaryPlus fallback is acceptable. INTDIV is a separate closure of native integer/bounded-decimal and legacy i128/exact-decimal policies, public division SDKs and0/1/2 precision-getter demand. None of these candidates is implemented or credited.

TiKV publishes first; TiDB pins its exact SHA and the common Plan hash. Both guides are updated. No force push/PR; preexisting untracked client differential BUILD.bazel remains excluded.
