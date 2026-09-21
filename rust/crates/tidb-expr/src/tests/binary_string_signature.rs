// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! The binary-vs-UTF-8 signature pairs of `pkg/expression/builtin_string.go`,
//! checked against answers captured from real TiDB.
//!
//! Every case uses `aéb`: FOUR bytes and THREE characters, so a byte answer
//! and a character answer can never coincide — the trap a CJK-only fixture
//! (three bytes per character) silently passes with the wrong signature.
//!
//! Captured with `go run ./rust/difftests/gorun` on this tree, e.g.
//!
//! ```text
//! select substring('aéb', 2, 2), substring(cast('aéb' as binary), 2, 2);
//! RS:éb|é
//! select char_length('aéb'), char_length(cast('aéb' as binary));
//! RS:3|4
//! select reverse('aéb'), hex(reverse(cast('aéb' as binary)));
//! RS:béa|62A9C361
//! ```

use super::assert_string2_refusal;
#[cfg(feature = "tikv-expr")]
use super::engine_e;
use super::{assert_packet_string_refusal, e};

/// One captured `(expression, TiDB answer)` pair per signature, in both the
/// character and the binary spelling of the same call.
#[track_caller]
fn captured(cases: &[(&str, &str)]) {
    for (expression, expected) in cases {
        assert_eq!(&e(expression), expected, "{expression}");
    }
}

#[cfg(feature = "tikv-expr")]
#[track_caller]
fn captured_engine(cases: &[(&str, &str)]) {
    for (expression, expected) in cases {
        assert_eq!(&engine_e(expression), expected, "TiKV engine: {expression}");
    }
}

#[cfg(not(feature = "tikv-expr"))]
#[track_caller]
fn captured_engine(cases: &[(&str, &str)]) {
    for (expression, _) in cases {
        assert_string2_refusal(expression);
    }
}

#[test]
fn substring_selects_bytes_for_a_binary_argument() {
    captured_engine(&[
        // builtinSubstring2ArgsUTF8Sig / builtinSubstring2ArgsSig
        ("hex(substring('aéb', 2))", "STR:C3A962"),
        ("hex(substring(cast('aéb' as binary), 2))", "STR:C3A962"),
        ("hex(substring('aéb', -2))", "STR:C3A962"),
        ("hex(substring(cast('aéb' as binary), -2))", "STR:A962"),
        // builtinSubstring3ArgsUTF8Sig / builtinSubstring3ArgsSig
        ("hex(substring('aéb', 2, 2))", "STR:C3A962"),
        ("hex(substring(cast('aéb' as binary), 2, 2))", "STR:C3A9"),
        ("hex(substr('aéb', 1, 2))", "STR:61C3A9"),
        ("hex(substr(cast('aéb' as binary), 1, 2))", "STR:61C3"),
        // Out-of-range and zero positions are the empty string either way.
        ("hex(substring(cast('aéb' as binary), 0, 2))", "STR:"),
        ("hex(substring(cast('aéb' as binary), 9, 2))", "STR:"),
        ("hex(substring(cast('aéb' as binary), 2, 0))", "STR:"),
        ("hex(substring(cast('aéb' as binary), 2, -1))", "STR:"),
    ]);
}

#[test]
fn left_right_and_char_length_select_bytes_for_a_binary_argument() {
    captured_engine(&[
        ("hex(left('aéb', 2))", "STR:61C3A9"),
        ("hex(left(cast('aéb' as binary), 2))", "STR:61C3"),
        ("hex(right('aéb', 2))", "STR:C3A962"),
        ("hex(right(cast('aéb' as binary), 2))", "STR:A962"),
        ("char_length('aéb')", "INT:3"),
        ("char_length(cast('aéb' as binary))", "INT:4"),
        // LENGTH has ONE Go signature and counts bytes for both.
        ("length('aéb')", "INT:4"),
        ("length(cast('aéb' as binary))", "INT:4"),
    ]);
}

#[test]
fn reverse_selects_byte_order_for_a_binary_argument() {
    captured_engine(&[
        // builtinReverseUTF8Sig reverses runes, keeping `é` intact.
        ("hex(reverse('aéb'))", "STR:62C3A961"),
        // builtinReverseSig reverses BYTES: `C3 A9` comes back as `A9 C3`.
        ("hex(reverse(cast('aéb' as binary)))", "STR:62A9C361"),
    ]);
}

#[test]
fn insert_source_rows_are_explicitly_contracted() {
    for (expression, expected) in [
        ("hex(insert('aébcd', 2, 2, 'X'))", "STR:61586364"),
        (
            "hex(insert(cast('aébcd' as binary), 2, 2, 'X'))",
            "STR:6158626364",
        ),
        (
            "hex(insert('aébcd', 2, 2, cast('X' as binary)))",
            "STR:6158626364",
        ),
        // pos out of range returns the source unchanged, in either signature.
        (
            "hex(insert(cast('aébcd' as binary), 0, 2, 'X'))",
            "STR:61C3A9626364",
        ),
    ] {
        let _ = expected;
        assert_packet_string_refusal(expression);
    }
}

#[test]
fn locate_and_instr_report_byte_offsets_for_a_binary_argument() {
    captured_engine(&[
        ("instr('aéb', 'b')", "INT:3"),
        ("instr(cast('aéb' as binary), 'b')", "INT:4"),
        ("locate('b', 'aéb')", "INT:3"),
        ("locate(cast('b' as binary), 'aéb')", "INT:4"),
        ("locate('é', 'aéb')", "INT:2"),
        (
            "locate(cast('é' as binary), cast('aéb' as binary))",
            "INT:2",
        ),
        // An empty needle matches at 1 and a missing one is 0 either way.
        ("locate(cast('' as binary), 'aéb')", "INT:1"),
        ("locate(cast('z' as binary), 'aéb')", "INT:0"),
    ]);
}

/// The three-argument pair, `builtinLocate3ArgsSig` /
/// `builtinLocate3ArgsUTF8Sig`: `pos` itself is counted in the signature's
/// units, so a binary search may start INSIDE a multi-byte character.
#[test]
fn locate_with_a_start_position_counts_pos_in_the_same_units() {
    captured_engine(&[
        ("locate('b', 'aéb', 1)", "INT:3"),
        ("locate(cast('b' as binary), 'aéb', 1)", "INT:4"),
        ("locate('b', 'aéb', 3)", "INT:3"),
        ("locate(cast('b' as binary), 'aéb', 3)", "INT:4"),
        ("locate(cast('b' as binary), 'aéb', 4)", "INT:4"),
        ("locate(cast('b' as binary), 'aéb', 5)", "INT:0"),
        ("locate('é', 'aébé', 3)", "INT:4"),
        ("locate(cast('é' as binary), 'aébé', 3)", "INT:5"),
        // An empty needle answers `pos` itself; a missing one and an
        // out-of-range `pos` are both 0.
        ("locate(cast('' as binary), 'aéb', 2)", "INT:2"),
        ("locate('', 'aéb', 2)", "INT:2"),
        ("locate(cast('z' as binary), 'aéb', 1)", "INT:0"),
        ("locate('b', 'aéb', 0)", "INT:0"),
    ]);
}

/// The signatures whose binary branch predates this seam, pinned so the
/// converged `is_binary_str` rule cannot quietly change them — including the
/// `CONVERT(... USING binary)` spelling, which reaches the same rule through a
/// binary COLLATION rather than through a bytes datum.
#[test]
fn case_pad_and_ord_keep_their_binary_answers() {
    captured_engine(&[
        // builtinUpperSig/builtinLowerSig return binary bytes untouched --
        // not even ASCII-folded.
        ("hex(upper(cast('aéb' as binary)))", "STR:61C3A962"),
        ("hex(lower(cast('aÉb' as binary)))", "STR:61C38962"),
    ]);
    captured(&[
        // ORD reads the argument charset: one byte for binary, the whole
        // first character folded base-256 otherwise.
        ("ord(cast('éb' as binary))", "INT:195"),
        ("ord('éb')", "INT:50089"),
        ("ord(convert('éb' using binary))", "INT:195"),
        ("char_length(convert('aéb' using binary))", "INT:4"),
    ]);
    // CONVERT USING is not lowerable as a child. With no native UPPER/LEFT
    // fallback, these shapes are explicit structured contractions.
    for expression in [
        "hex(upper(convert('aéb' using binary)))",
        "hex(left(convert('aéb' using binary), 2))",
    ] {
        assert_string2_refusal(expression);
    }
    // Keep the former binary/character signature fixtures, but assert their
    // explicit contraction after the packet-limited kernels were deleted.
    for expression in [
        "hex(lpad(cast('aéb' as binary), 5, 'z'))",
        "hex(rpad(cast('aéb' as binary), 5, 'z'))",
        "hex(lpad('aéb', 5, 'z'))",
        "hex(lpad('aéb', 5, cast('z' as binary)))",
    ] {
        assert_packet_string_refusal(expression);
    }
}

#[test]
fn utf8_and_case_insensitive_signatures_are_untouched() {
    // The seam must not leak into the character signatures: these are the
    // same answers TiDB gives, with no binary argument anywhere.
    captured_engine(&[("hex(substring('中文测试', 2, 2))", "STR:E69687E6B58B")]);
    captured_engine(&[
        ("hex(left('中文测试', 2))", "STR:E4B8ADE69687"),
        ("hex(reverse('中文测试'))", "STR:E8AF95E6B58BE69687E4B8AD"),
        ("char_length('中文测试')", "INT:4"),
    ]);
}

/// The folding collations still reach `builtinLocate2ArgsUTF8Sig`'s collator
/// path, and only a `binary` derivation switches to the byte signature.
///
/// The native collator helper remains until the embedded TiKV path preserves
/// `utf8mb4_bin`; the test also pins the current engine divergence explicitly.
/// Captured from TiDB:
///
/// ```text
/// select instr('ABC' collate utf8mb4_general_ci, 'b'),
///        instr('ABC' collate utf8mb4_bin, 'b'),
///        locate('É' collate utf8mb4_general_ci, 'aéb');
/// RS:2|0|2
/// ```
#[test]
fn only_a_binary_derivation_switches_locate_to_bytes() {
    use crate::string_fn::locate;
    use tidb_datatype::{Collation, Datum};

    let ci = Collation::Utf8Mb4GeneralCi;
    let bin = Collation::Utf8Mb4Bin;
    let needle = Datum::new_string("b".to_string());
    let haystack = Datum::new_string("ABC".to_string());
    assert_eq!(locate(&needle, &haystack, ci).unwrap(), Datum::Int(2));
    assert_eq!(locate(&needle, &haystack, bin).unwrap(), Datum::Int(0));
    assert_eq!(
        locate(
            &Datum::new_string("É".to_string()),
            &Datum::new_string("aéb".to_string()),
            ci,
        )
        .unwrap(),
        Datum::Int(2),
    );
    assert_eq!(
        locate(
            &Datum::new_string("b".to_string()),
            &Datum::new_string("aéb".to_string()),
            Collation::Binary,
        )
        .unwrap(),
        Datum::Int(4),
    );
    #[cfg(feature = "tikv-expr")]
    assert_eq!(
        engine_e("instr('ABC' collate utf8mb4_bin, 'b')"),
        "INT:2",
        "accepted gap: the embedded RPN path loses utf8mb4_bin case sensitivity"
    );
}
