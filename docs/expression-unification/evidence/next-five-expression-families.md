# Next five candidate families — source-only selection

Round9 independent C review; no edits, builds or tests were performed by C. These are candidates after the ASCII public-activation boundary, not migrated families. Frozen denominator245 and completed-family count0 are unchanged. LENGTH/OCTET_LENGTH is one family; LTRIM and RTRIM are separate.

Paths below abbreviate `tidb/rust/crates/tidb-expr/src/` as T and `tikv/components/tidb_query_expr/src/impl_string.rs` as K; anchors are pre-change source lines. The five families currently have no admitted TiDB typed-PB/unistore signatures in the frozen implemented domain. Do not add PB support merely to fill a migration row.

| Family | Current native ownership / official TiKV kernel | Whole-family constraints / existing regressions |
|---|---|---|
| LENGTH / OCTET_LENGTH | T/build.rs:240–255, func.rs:113–131, scalar_function.rs:2382–2395, public BuiltStringLength::eval; Length7029 → K::length:93 | NULL; raw bytes including FF/NUL/bit/hex, existing BinAware encoding; signed Int/flen10 rather than ASCII flen3. Tests length_always_counts_raw_bytes, go_test_length_and_octet_length, K::test_length include UTF8/GBK/numeric/NULL/raw bytes. |
| BIT_LENGTH | T/string_fn.rs:150–156, func.rs:789; BitLength7001 → K::bit_length:160 | NULL; byte count×8, not character count; signed Int/flen10. UInt first follows native text coercion, never reinterpret as signed. Remove caller multiplication. Tests bit_length_source_vectors_preserve_utf8_byte_count and K::test_bit_length. |
| LTRIM | T/builtin_ext/string2.rs:172–176, shared trimmed:193–202, dispatch:37; LTrim7026 → K::ltrim:251 | NULL; only leading0x20, preserve tab/CR/LF/invalid UTF8. Owned Bytes output, then native binary/text and metadata handling. Tests ltrim_and_rtrim_preserve_non_space_whitespace, go_test_ltrim_rtrim, an_etstring_argument_is_read_as_bytes_not_as_utf8, K::test_ltrim. |
| RTRIM | Same T file:182–186/shared trimmed, dispatch:38; RTrim7042 → K::rtrim:260 | Same constraints but trailing0x20 only; preserve leading spaces and newline order. Output can equal the full input length. Same native test groups and K::test_rtrim. |
| UNHEX | T/string_fn.rs:682–700, hex_nibble:839, func.rs:786; UnHex7062 → K::unhex:99 | NULL; odd digits left-pad0; invalid hex/FF gives SQL NULL without warning/error; empty gives empty binary Bytes. Binary result metadata and width use pre-conversion declared type. Tests unhex_source_vectors_preserve_odd_digit_left_padding, unhex_matches_go, K::test_unhex. |

## Reuse boundary, not five new pools

Use one closed operation enum, official recipe table and shared driver/pool implementation. The two length families reuse ready-Bytes→own signed Int. The three result-string families need one owned-Bytes carrier extension, not three adapter copies. Do not simply relax the existing closed ASCII worker into an unconstrained program API.

Preserve per-call-site argument evaluation/coercion/charset conversion and final result metadata. LENGTH's public helper differs from the generic coerce_str_bytes helper for some invalid ENUM/SET names; do not normalize away that frontend behavior. LENGTH also shares a native branch with CHAR_LENGTH-binary: split ownership deliberately without claiming CHAR_LENGTH completion.

Trimmed bytes are a new computed result even if identical to input, not borrowed input identity. UNHEX always returns binary; trim results keep existing binary/text wrapping and arg0-derived metadata. Existing metadata tests include string_builtins_returning_an_int_match_go, arg_zero_width_family_matches_go and unhex_matches_go. A small API extension does not authorize limiting valid inputs to small strings or claiming allocator peak bounds for UNHEX padding/decoder/result copies.

## Explicit deferrals, not subset credit

- ORD7040: official NULL→0 versus native NULL→NULL, plus declared charset/first-byte fallback/collation differences.
- CHAR_LENGTH/CHARACTER_LENGTH: complete family includes CharLength7065 and CharLengthUtf87005 plus existing PB entry. Official UTF8 rejects invalid bytes; native counts using Go rules. Binary-only is not a complete family.
- QUOTE7041: both return text NULL for NULL input, but official preserves invalid octets while native currently performs UTF8-lossy replacement. No expected-data rewrite or valid-UTF8-only credit.
- HEX7020/7021: full family includes Int/Bytes, typed BIT, UInt/negative cases and numeric cast diagnostics. Bytes-only is incomplete.
- TRIM7059/7060/7061: pattern, direction, AST syntax and ltrim_with/rtrim_with need a shared multi-argument boundary; it is not an alias of LTRIM/RTRIM.
- TO_BASE647058 and FROM_BASE647019: separate families with packet/order diagnostics; FROM also differs on padding and VT/FF whitespace handling.

Actual migration still requires normal result/metadata/error comparison, official provenance, complete native deletion, all implemented entrypoints and release performance acceptance. No coverage credit follows from this table.
