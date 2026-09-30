# Complete `pkg/parser/charset` package

## Authority and inventory

- Go authority: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`origin/master`).
- The checkout's `pkg/parser/charset` is byte-identical to that authority.
- Atomic inventory: 14 files and 3,319 Go/Bazel lines: 10 production files,
  the generated GB18030 data
  input, 2 original test files, and `BUILD.bazel`.
- Go executable inventory: 9 `Test*` functions and
  `BenchmarkGetCharsetDesc`; the manifest's 10-function count is exact.

The Rust owner is `tidb-datatype`. The exact Go simple-rune Unicode table is
owned by the dependency-leaf `tidb-mysql` crate, matching the Go package's
dependency on `parser/mysql` and the standard `unicode` table.

## Production mapping

- `charset.go` maps to `src/charset.rs` and the generated
  `src/charset_data/{known_charsets,collations}.rs` catalogs.
- `encoding.go`, the base, binary, ASCII, Latin-1, UTF-8, GBK, and GB18030
  implementations map to `src/encoding_base.rs`, `ascii_encoding.rs`,
  `utf8_encoding.rs`, and `multibyte_encoding.rs`.
- `encoding_gb18030_data.go` overrides are verified by
  `scripts/generate-parser-charset.py` against the single TiKV-owned
  `encoding/gb18030_data.rs` dataset (the GB ownership addendum below records
  the exact Native/wire distinction). Both source special-case tables still
  generate `src/charset_data/{gbk_cases,gb18030_cases}.rs` unchanged.
- `encoding_table.go` maps through the same generator to
  `src/encoding_labels.rs`; `src/encoding_table.rs` supplies the source lookup
  and codec behavior.
- Both Go test files map to
  `tests/parser_charset_package_source.rs` plus leaf regression tests. The Go
  benchmark maps to the `parser_charset` benchmark target.
- No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Closed gaps

- Encoding upper/lower conversion now uses Go's Unicode 15 simple-rune
  mappings instead of Rust full mappings that expanded `ß` to `SS`.
- Registry, collation, and HTML encoding-label normalization use Go simple
  lowercase; encoding labels also use Unicode `strings.TrimSpace` rather than
  an ASCII-only trim policy.
- GB18030 `MbLen` retains Go's short-input return for fewer than two bytes and
  its observable bounds panic for truncated two/three-byte four-byte prefixes.
- The exported TiFlash-supported charset set is present and exact.
- `RemoveCharset` now preserves Go's original-length range/delete behavior,
  including its name comparison and mutation edge cases.

## Validation

Ready profile was used because this receipt updates the complete atomic
package boundary while the repository-wide parity campaign continues.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./charset -count=1` from `pkg/parser` — all source
  package tests passed after the pinned nested-module dependencies were made
  available. The package has no failpoint imports or injections.
- `python3 scripts/generate-parser-charset.py` from `rust` followed by a clean
  generated-table diff — all generated images match the pinned Go sources.
- `cargo +nightly-2026-08-22 test -p tidb-datatype --test all parser_charset -- --test-threads=1` — 11 source-derived tests passed.
- `cargo +nightly-2026-08-22 test -p tidb-datatype --lib encoding -- --test-threads=1` — 21 encoding tests passed.
- `cargo check -p tidb-datatype --benches --locked` — benchmark compiled.
- `cargo check -p tidb-ast -p tidb-protocol -p tidb-expr --lib --locked` — all
  immediate encoding consumers compiled.
- `cargo check -p tidb-executor -p tidb-exec --lib --locked` — both execution
  layers compiled; only pre-existing warnings were emitted.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, pinned-Go `make lint`, and
  `git diff --check` — clean.

No live TiKV/TiFlash service behavior was exercised; this package has no such
direct dependency.

## GB override ownership move (Foundation B; no new package completion claim)

The full Go package boundary, authority/inventory, and historical validation
above are retained. This is a dataset ownership move, not a new package parity
or transcreation completion claim. The Apache-2.0 Go authority
`pkg/parser/charset/encoding_gb18030_data.go` remains unchanged, SHA-256
`d620a5a0e124d135c21a152a5a4b476af847bbe78e0ee3fd70886941d2aceed8`.

The sole override dataset is the existing TiKV file
`components/tidb_query_datatype/src/codec/collation/encoding/gb18030_data.rs`:
2,103 pairs, 54,905 bytes, frozen SHA-256
`0fe60b01bdfa12c6f25c46470deb8a25bd3c6da925c7f9b83a92329f6b5fcf9d`.
The generator verifies the exact Go 2,094 unique byte/rune pairs, bijection,
canonical byte-key ordering, source/file hashes, and equality with those pairs
plus exactly these nine wire-only entries:

| Encoded bytes | Unicode rune |
| --- | --- |
| `FD9C` | `U+F92C` |
| `FD9D` | `U+F979` |
| `FD9E` | `U+F995` |
| `FD9F` | `U+F9E7` |
| `FDA0` | `U+F9F1` |
| `FE40` | `U+FA0C` |
| `FE41` | `U+FA0D` |
| `FE47` | `U+FA18` |
| `FE49` | `U+FA20` |

There are no conflicting or Native-only pairs. Native behavior excludes these
nine overrides explicitly rather than materializing another mapping table;
Foundation B owns that runtime policy. The original Native pair oracle is
SHA-256 `35b0bfe4bda90a06a2a1a9dc24e7b3d480f99d72143d899d8183ac2821193c08`
over byte-key-sorted little-endian `(u32 encoded, u32 rune)` tuples.

`rust/scripts/generate-parser-charset.py` no longer produces
`src/charset_data/gb18030_by_rune.rs` or `gb18030_by_bytes.rs`. Its narrow
`--gb-only` mode generates `src/charset_data.rs` with only the two corresponding
include lines removed, and prunes only those two exact old maps after checking
both original lengths/SHA-256. It refuses modified maps, symlinks, non-files,
and unexpected stub edits before writing or deleting. It never writes the
canonical TiKV map. Registry, case, and encoding-label generation remains
unchanged and is not run in either narrow GB mode.

Commands run from the TiDB repository root:

- `python3 -B rust/scripts/generate-parser-charset.py --gb-only` — status 0; generated the two-include-only stub diff and removed the two verified duplicates. A second run changed nothing, including the stub modification time.
- `python3 -B rust/scripts/generate-parser-charset.py --check-gb` — status 0 after migration; before migration, expected status 1 rejected the old native maps.
- `python3 -B rust/scripts/generate-parser-charset.py --check` — status 0; explicit alias for the narrow, read-only GB authority/stub/obsolete-map check, not a new non-GB audit.

Read-only SHA-256 and modification-time comparisons proved that canonical
`gb18030_data.rs`, all four TiKV collator `.data` files, and the native
`known_charsets.rs`, `collations.rs`, `gbk_cases.rs`, `gb18030_cases.rs`, and
`encoding_labels.rs` were not rewritten. Targeted in-memory fault checks
confirmed fail-closed cleanup for a modified second map, symlinks, non-files,
and unexpected stub contents. Changed authority hashes, an incorrect wire-only
rune, a missing canonical pair, a wrong Native tuple hash, and a stale stub were
also rejected; all CLI modes failed before writes on canonical authority drift.
The non-GB renderer was additionally compared in memory with all five existing
outputs, byte-for-byte, with writes intercepted: no files were rewritten and
no mapping output was produced. The original Go inputs and table oracles remain
intact; no full mapping was generated/copied again.

No builds, formatting, `make lint`, Go/Rust runtime tests, or package-readiness
gates were rerun for this subowner's data/generator scope. Foundation B owns
runtime integration and its validation; the earlier complete-package receipt
is not expanded by these targeted checks.
