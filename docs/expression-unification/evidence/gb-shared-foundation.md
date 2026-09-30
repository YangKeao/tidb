# GB shared foundation: ownership migration

## Status and scope

**Source-coherent handoff; Rust compilation/runtime gates remain parent-owned.**
This moves existing Rust GB implementation/data ownership to TiKV; it does not
add an evaluated function family, complete another Go package, or establish
full-domain equivalence between different codecs. No build or formatter was run
by B or its children. Parent owns both lockfiles, formatting, build/test gates,
maintenance guides, indexes and publication.

B owns `tikv/components/tidb_query_datatype/src/codec/collation/gb.rs` and the
collation/encoding seams plus the component manifest. Native facade/test author
N (`48f7b254-ca85-4cd3-80f5-4c32570dc58a`) and data/generator author G
(`957fcad6-ab44-43ef-94d1-dbb03c9a95f4`) froze their scoped files. N also completed
a read-only cross-review of the final shared kernel and wire delegation: no
source-level blocker found; that is not a compile/run result.

## Verified data, not assumed table equivalence

The four TiKV `collation/collator/*.data` images retain their original bytes:

| Image | Slots / encoding | SHA256 |
| --- | --- | --- |
| `gbk_bin.data` | 65,536 / BE-u16 | `31690e4a2ae6b64d0c801b1ac81ae894c6f572e8d917bfc2570a8d38bfd49215` |
| `gbk_chinese_ci.data` | 65,536 / BE-u16 | `936a6495ad2f211980bfb80cd1a52efb0cfbf04f68bfdd75f898d0aeba5336df` |
| `gb18030_bin.data` | 1,114,112 / LE-u32 | `7e97b5ed85a68b81b5322ad33e654ab3b0f0243ae6c6075abf0477a96d2567b7` |
| `gb18030_chinese_ci.data` | 1,114,112 / LE-u32 | `64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d` |

Read-only Python compared every CI slot. GBK native LE versus TiKV BE differs
in byte representation only: **0/65,536 numeric differences**; the old native
LE SHA remains `f6f63c33fa57eeaffa5d46841694adab58bd9cddfac3f92389dec4564a6036d6`.
GB18030 CI is byte-identical, **0/1,114,112 differences**. Original native
length/hash tests retain their oracles, reading canonical files at test time
and endian-converting GBK, not embedding another copy.

The unchanged `encoding/gb18030_data.rs` has 2,103 unique pairs and SHA
`0fe60b01bdfa12c6f25c46470deb8a25bd3c6da925c7f9b83a92329f6b5fcf9d`.
Both removed native tables represented the same 2,094 pairs in opposite lookup
orders: all 2,094 match canonical data; zero conflicting or native-only pairs.
Their byte-key-sorted LE `(u32 encoded, u32 rune)` tuple SHA is
`35b0bfe4bda90a06a2a1a9dc24e7b3d480f99d72143d899d8183ac2821193c08`.
The nine explicit wire-only pairs are `FD9C→F92C`, `FD9D→F979`, `FD9E→F995`,
`FD9F→F9E7`, `FDA0→F9F1`, `FE40→FA0C`, `FE41→FA0D`, `FE47→FA18`, `FE49→FA20`.
Native lookup excludes them and uses its pinned codec fallback, not wire
lookup. The shared runtime adds only a derived rune index into canonical rows;
byte lookup searches the canonical sorted table. No complete mapping is copied.

## Explicit policies and single runtime owner

`gb::{GbCollation,GbPolicy,GbEncoding,compare,key}` owns GB operations; native
`collation.rs` only maps identities and delegates. Wire collator signatures,
weight tables, LIKE modes, and hash implementations are unchanged. Their
compare/emitter methods delegate to the same shared workers with `Wire` policy.

- Wire compare orders numeric weights; native BIN compare orders encoded bytes.
  For GB18030, U+0080 versus 中 is a regression proving these differ. Compare
  does not call the public key function.
- The native override subset differs from the wire GB18030 BIN table at **36
  checked slots**. This is an override-subdomain count, not a full-Unicode codec
  comparison. GBK euro is rejected/replaced by native encoding but retains wire
  weight `0x80`.
- Native GB18030 keys append NUL for the original **19 PUA runes**; comparison
  encodes those runes without the key-only NUL. Source NUL remains observable.
- CI invalid-input comparison/key truncation, wire BIN per-byte replacement,
  native lead-byte-group replacement, PAD SPACE/NoPad, owned GB immutable keys,
  and explicit disabled new-collation mode retain their existing contracts.
- Shared `foreach_native`, `peek_native`, and `mb_len_native` own native GB
  encoding/grouping. Native TransformPolicy, first-error objects, collection
  precedence and non-GB/case/registry behavior remain unchanged. The existing
  native MbLen panic for truncated four-byte prefixes is deliberately preserved.

The component adds `native_codec = { package = "encoding_rs", version =
"=0.8.35" }`. Wire continues to use git `encoding_rs 0.8.29` at
`68e0bc5a72a37a78228d80cd98047326559cf43c`. Other native codecs retain their
existing registry dependency; lock resolution details and any incidental
resolver-edge movement are recorded by the parent, not claimed absent here.

## Generator migration and source provenance

Modified generators, not hand-edited artifacts, removed the two native CI bins
and `gb18030_by_{rune,bytes}.rs`. The generated `charset_data.rs` differs only by
removing their two includes. Canonical data, five other charset generated files,
and the three relevant Go authorities retained both bytes and mtimes in G's
before/after check. Existing package inventories/authority records remain in
`rust/testport/receipts/{util_collate,parser_charset}.md`, with runtime ownership
mapping addenda; `src/collation_data/README.md` retains source attribution.

Executed by G from the TiDB root, all post-migration commands exited 0:

```text
python3 -B rust/crates/tidb-datatype/scripts/generate_collation_data.py --prune-gb-images
python3 -B rust/crates/tidb-datatype/scripts/generate_collation_data.py --check
python3 -B rust/scripts/generate-parser-charset.py --gb-only
python3 -B rust/scripts/generate-parser-charset.py --check-gb
python3 -B rust/scripts/generate-parser-charset.py --check
```

Both migration modes were idempotent. Collation normal mode is now read-only;
its existing General/UCA checks and separate original prune scope are retained.
Parser `--check` is explicitly a **GB-only alias**, not a full catalog audit.
Before cleanup, checks rejected the stale duplicate outputs (migration RED);
after generator cleanup they accepted absence/stub/canonical data (migration
GREEN). G's in-memory negative checks rejected authority/hash/weight/pair drift,
wrong nine-pair values, stale stubs and unsafe cleanup targets before mutation.
These are generator/data results, not numeric-expression or Rust runtime RED/GREEN.

## Remaining parent gates

New shared tests cover the exact 2,094 subset, finite 36/19 deltas, euro,
compare-versus-key ordering, padding, invalid groups and preserved MbLen panic.
Seven new native integration tests cover literal GB keys/compare, malformed
bytes, NoPad/mode, transform policies and grouping; the original PUA test was
extended without changing its six existing rows. Original wire tests and native
hash/source-package fixtures are retained. Scoped diff checks passed, and B
recomputed unchanged canonical/four-image hashes at handoff.

**Still unexecuted by this author cohort:** Rust compilation, the shared GB
unit gate, original wire collation/encoding gates, native charset/collation
regressions, and parent joint datatype/expression gates. No allocation,
performance, physical OOM, full-domain cross-codec, or additional 4–6-family
acceptance is inferred from this ownership step.
