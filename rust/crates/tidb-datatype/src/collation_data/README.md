# Shared TiDB collation data provenance

The former native binary images were mechanical, lossless conversions of
TiDB's Apache-2.0-licensed Go authorities, which remain the verification inputs:

- `pkg/util/collate/general_ci.go`
- `pkg/util/collate/ucadata/unicode_ci_data_generated.go`
- `pkg/util/collate/ucadata/unicode_0900_ai_ci_data_generated.go`
- `pkg/util/collate/gbk_chinese_ci_data.go`
- `pkg/util/collate/gb18030_weight.data`

`../../scripts/generate_collation_data.py` now verifies the TiKV-owned tables;
it does not emit native collation images. General/UCA checks are unchanged:
UCA 4.0 is checked against
`pkg/util/collate/ucadata/unicode_ci_data_original_test.go`, both long-rune maps
and every static slot are compared, and all original source-pinned record
lengths and hashes remain oracles.

The canonical GB CI files live under the sibling TiKV checkout's
`components/tidb_query_datatype/src/codec/collation/collator/`:

| Canonical file | Format / bytes | SHA-256 |
| --- | --- | --- |
| `gbk_chinese_ci.data` | 65,536 big-endian u16 / 131,072 | `936a6495ad2f211980bfb80cd1a52efb0cfbf04f68bfdd75f898d0aeba5336df` |
| `gb18030_chinese_ci.data` | 1,114,112 little-endian u32 / 4,456,448 | `64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d` |

Every GBK weight equals the Go numeric authority; its former native
little-endian image SHA-256
`f6f63c33fa57eeaffa5d46841694adab58bd9cddfac3f92389dec4564a6036d6`
is still checked record-by-record, without constructing that image. GB18030
canonical bytes equal the Go embedded data exactly. The canonical files are
verified in place, never regenerated or endian-swapped on disk.

Verify from the TiDB repository root (normal mode is also read-only):

```sh
python3 -B rust/crates/tidb-datatype/scripts/generate_collation_data.py --check
```

Every mode requires the sibling TiKV checkout, or `--tikv-root <path>`.
Normal/`--check` reject stale duplicate native GB images. To migrate an old
checkout, `--prune-gb-images` deletes only
`gbk_chinese_ci_u16_le.bin` and `gb18030_chinese_ci_u32_le.bin`, after shared
source verification and validation of both exact old lengths/hashes. Symlinks,
non-files, and modified images are refused before deletion. The existing
`--prune-obsolete` remains limited to its five General/UCA filenames; neither
cleanup mode broadens the other's scope. Do not hand-edit canonical data.

The Go `ucaimpl` templates generate repeated collator plumbing. Rust replaces
that support generator with the single typed `UcaCursor` implementation in
`collation.rs`; there is no generated Rust implementation file to drift. The
Go `ucadata/generator` outputs and retained original UCA 4.0 fixture remain the
inputs checked by this Rust generator, so both Go generator paths have an
explicit executable equivalent rather than a copied second authority.
