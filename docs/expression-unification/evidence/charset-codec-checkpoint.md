# Shared charset byte policy: M0 prerequisite, no family credit

**charset-codec-92**, after **timestamp-diff-91**. Functional228/245, strict0, remaining17. Every prior family object and partial CAST/type record is retained; only a type prerequisite record is appended.

## Ownership and preserved distinctions

TiKV `codec/collation/native_encoding.rs` owns:

- Opaque u16 TransformOp with original Debug name and any-bit `contains`; generic first-error factory, TRIM-before-REPLACE and COLLECT_FROM-before-COLLECT_TO policy.
- ASCII lead-width grouping, UTF8 strict/mb3 groups, seven-encoding peek/mb_len/is_valid/foreach/transform operations and valid source-prefix counting.
- Separate primitive ASCII/UTF8 valid-input fastpaths. Valid `abc` with operation0 remains `abc` for primitives but empty for registry transforms; Latin1/Binary always preserve arbitrary bytes.

ASCII peek remains one byte while its invalid foreach groups can consume a clipped2/3/4-byte lead group. UTF8 invalid sequences advance one byte; valid RuneError is not invalid. Strictmb3 rejects a valid four-byte rune as one group, but mb_len still reports4. GB operations use existing native GB helpers; UTF8 uses the existing strict decoder rather than a copied decoder. Prefix counting measures source bytes, not decoded output length.

Native `encoding_base.rs`, `ascii_encoding.rs`, `utf8_encoding.rs` and `multibyte_encoding.rs` remove these byte algorithm bodies. Error/result structures, private fields, Display/Debug and ZST/enum carriers remain local; generic error factories preserve primitive `utf8` versus registry `utf8mb4` diagnostic names. The private policy proxy is test-only and delegates. Encoding metadata/name lookup and upper/lower/case tables are untouched. No dependency, Expr/C4 profile, general driver, carrier or wire Encoding policy changes.

## Core evidence

[Exact commands and hashes](../logs/charset-codec-summary.txt); [manifest](../checkpoint.json).

Eight locked single-threaded launches include one compile RED: the new native test's byte-array conditional required an explicit `&[u8]` local annotation. Only that annotation changed; expected values and production were unchanged. The failed raw log/hash remains recorded. Seven actual nonzero test runs: five green, two only-old full RED. Both new tests pass on first actual execution; no oracle correction, new execution failure, zero-match, interruption or fixture recording.

- CPP collation28/0; native encoding22/0; convert3/0+1ignored.
- Original charset SQL7/0 and UTF8-write SQL1/0:21SELECTs plus write refusals, covering storedGBK and implicit binary boundaries, HEX/LENGTH/CHAR_LENGTH/ASCII, CAST AS BINARY, CONVERT retag/replacement, latin1 raw bytes, introducers/CHAR USING, ordering and invalid UTF8 versus VARBINARY. This is existing consumer evidence, not new C4, zero-slot or vector/pool proof.
- Full expression1602/4old/94ignored; unistore220/1old/13ignored. Whole failure sections remain byte-equal to R94 after numeric panic-heading thread-ID normalization only: `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95` / `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Two CPP/four native Rust files; one new module and one new test per repository. Fourteen original native test bodies are byte-identical; the changed CPP module contained no original tests. Pinned formatting/diff checks pass. Independent D source review found no blocker. Agent-doc additions describe ownership without adding policy; paths checked. No Go/Bazel/Cargo/lock/generated changes; unrelated BUILD excluded.

## Next work is not implied complete

Charset runtime workers still need original nullable bytes and charset metadata, preserving direct-helper NULL→empty versus caller NULL, target validation before coercion, Bytes versus retag String versus computed NULL/error, and unchanged metadata passthrough for ordinary implicit wrappers. Existing source review found no GB-tagged old facade-count fixture requiring alteration, but actual next-step gates remain necessary. Native PB admission is not added merely because TiKV has wire ToBinary/FromBinary signatures.

DATE_ADD/SUB is a separate larger paired migration: ordinary single/composite coercion order, AST NoColumns policy, typed Duration, distinct interval parsers and late casts, plus48 calculating legacy signatures. Eight accepted legacy Duration→Datetime paths return no value without child demand and must not be widened. Shared PB has no kernel; wire math is not a policy alias.

[Remaining review](remaining-acceptance.md) keeps5core,6ordinary and6complex candidates open, along with real request-root/default-NoColumns/live-DAG ownership. Known CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps and full-suite failures remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-tzdata, complete Go-package transcreation and PR-readiness are not claimed.
