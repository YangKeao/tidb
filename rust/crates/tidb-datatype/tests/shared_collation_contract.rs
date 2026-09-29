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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Public facade contracts; literal expected bytes are independent of the
//! shared implementation. Registry IDs are not signed TiKV wire IDs.

use std::{borrow::Cow, cmp::Ordering};

use tidb_datatype::{get_collator_by_id, get_collator_with_mode, Collation, Collator};

#[test]
fn shared_key_bytes_no_trim_and_max_lengths() {
    for (collation, key, untrimmed, max_len) in [
        (Collation::Binary, b"a ".as_slice(), b"a ".as_slice(), 2),
        (Collation::Utf8Mb4Bin, b"a", b"a ", 2),
        (Collation::Latin1Bin, b"a", b"a ", 2),
        (Collation::Utf8GeneralCi, b"\0A", b"\0A\0 ", 4),
        (Collation::Utf8Mb4GeneralCi, b"\0A", b"\0A\0 ", 4),
        (
            Collation::Utf8UnicodeCi,
            b"\x0e\x33",
            b"\x0e\x33\x02\x09",
            32,
        ),
        (
            Collation::Utf8Mb4UnicodeCi,
            b"\x0e\x33",
            b"\x0e\x33\x02\x09",
            32,
        ),
        (
            Collation::Utf8Mb40900AiCi,
            b"\x1c\x47\x02\x09",
            b"\x1c\x47\x02\x09",
            32,
        ),
        (Collation::Utf8Mb40900Bin, b"a ", b"a ", 2),
    ] {
        assert_eq!(collation.key(b"a "), key, "{collation:?}");
        assert_eq!(collation.key_without_trim_right_space(b"a "), untrimmed);
        assert_eq!(collation.max_key_len(b"a "), max_len);
    }
    assert_eq!(Collation::Utf8Mb4GeneralCi.max_key_len(b"\xff"), 2);
    assert_eq!(Collation::Utf8Mb4UnicodeCi.max_key_len(b"\xc3\x28"), 32);
    assert_eq!(Collation::Utf8Mb4GeneralCi.key(b"a\xffz"), b"\0A");
    assert_eq!(
        Collation::Utf8Mb4GeneralCi.compare(b"\xff", b"x"),
        Ordering::Equal
    );
}

#[test]
fn shared_cow_borrowing_is_not_raw_key_capability() {
    let input = b"a ";
    for collation in [
        Collation::Binary,
        Collation::AsciiBin,
        Collation::Latin1Bin,
        Collation::Utf8Bin,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb40900Bin,
    ] {
        let key = collation.immutable_key(input);
        assert!(matches!(key, Cow::Borrowed(_)));
        assert_eq!(key.as_ptr(), input.as_ptr());
        assert_eq!(key.as_ref(), collation.key(input));
        assert_eq!(
            Collator::New(collation).can_use_raw_mem_as_key(),
            matches!(collation, Collation::Binary | Collation::Utf8Mb40900Bin)
        );
    }
    for collation in [
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
    ] {
        assert!(matches!(collation.immutable_key(input), Cow::Owned(_)));
        assert!(!Collator::New(collation).can_use_raw_mem_as_key());
    }
}

#[test]
fn shared_pad_space_is_only_ascii_space() {
    for suffix in [
        b"\t".as_slice(),
        b"\0",
        "\u{a0}".as_bytes(),
        "\u{3000}".as_bytes(),
        b"\xff",
    ] {
        let mut input = b"a".to_vec();
        input.extend_from_slice(suffix);
        for collation in [
            Collation::AsciiBin,
            Collation::Latin1Bin,
            Collation::Utf8Bin,
            Collation::Utf8Mb4Bin,
        ] {
            assert_eq!(collation.key(&input), input);
        }
    }
}

#[test]
fn shared_legacy_mode_is_byte_order_but_rune_like() {
    let collator = get_collator_with_mode(false, "utf8mb4_general_ci");
    assert_eq!(collator, Collator::DerivedBinary);
    assert_eq!(collator.compare(b"a", b"A"), Ordering::Greater);
    assert_eq!(collator.key(b"a "), b"a ");
    assert!(collator.can_use_raw_mem_as_key());
    assert!(matches!(collator.immutable_key(b"a "), Cow::Borrowed(_)));
    assert!(collator.pattern(b"_", b'\\').is_match("中".as_bytes()));
    assert!(collator.like_match(b"\xff", b"\xfe", b'\\'));
    assert!(!collator.like_match(b"a", b"A", b'\\'));
}

#[test]
fn shared_registry_ids_are_not_normalized_wire_ids() {
    // Like the unchanged Go-vector fixture, this runs in the default process
    // mode. The parent runs mode-mutating suites with one test thread.
    assert!(tidb_datatype::new_collation_enabled());
    assert_eq!(get_collator_by_id(45).key(b"a "), b"\0A");
    assert_eq!(get_collator_by_id(-45).key(b"a "), b"a");
    assert_eq!(get_collator_by_id(192).key(b"a"), b"\x0e\x33");
    assert_eq!(get_collator_by_id(224).key(b"a"), b"\x0e\x33");
    use tidb_query_datatype::Collation as WireCollation;
    assert_ne!(
        WireCollation::from_i32(46).unwrap(),
        WireCollation::from_i32(-46).unwrap()
    );
    assert_eq!(
        WireCollation::from_i32(63).unwrap(),
        WireCollation::from_i32(-63).unwrap()
    );
    assert!(WireCollation::from_i32(i32::MIN).is_err());
    assert_eq!(
        tidb_datatype::rewrite_new_collation_id_if_needed(i32::MIN),
        i32::MIN
    );
    assert_eq!(
        tidb_datatype::restore_collation_id_if_needed(i32::MIN),
        i32::MIN
    );
}

#[test]
fn shared_patterns_preserve_byte_rune_and_collator_modes() {
    for (collation, single, malformed) in [
        (Collation::Binary, false, false),
        (Collation::Gb18030Bin, false, false),
        (Collation::GbkBin, true, true),
        (Collation::Latin1Bin, true, true),
        (Collation::Utf8Mb4Bin, true, true),
        (Collation::Utf8Mb40900Bin, true, true),
    ] {
        let collator = Collator::New(collation);
        assert_eq!(
            collator.pattern(b"_", b'\\').is_match("中".as_bytes()),
            single
        );
        assert_eq!(collator.like_match("中".as_bytes(), b"_", b'\\'), single);
        assert_eq!(
            collator.pattern(b"\xff", b'\\').is_match(b"\xfe"),
            malformed
        );
        assert!(!collator.like_match(b"", b"%", b'%'));
        assert!(collator.like_match(b"\\", b"\\", b'\\'));
    }
    assert!(Collation::Utf8Mb4GeneralCi
        .pattern("😁", b'\\')
        .is_match("😀".as_bytes()));
    assert!(!Collation::Utf8Mb4UnicodeCi
        .pattern("😁", b'\\')
        .is_match("😀".as_bytes()));
    assert!(Collation::Utf8Mb4UnicodeCi
        .pattern(" ", b'\\')
        .is_match("\u{3000}".as_bytes()));
    assert!(!Collation::Utf8Mb4UnicodeCi
        .pattern("\0", b'\\')
        .is_match(b" "));
    assert!(!Collation::Utf8Mb4UnicodeCi
        .pattern("ss", b'\\')
        .is_match("ß".as_bytes()));
}

#[test]
fn shared_valid_input_compare_and_serialized_key_order_agree() {
    let samples = [
        "", "a", "A ", "a\t", "a\0", "ß", "ss", "ﬀ", "中文", "😀", "\u{3000}", "\u{321d}",
    ];
    for collation in [
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Latin1Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb40900Bin,
    ] {
        for left in samples {
            for right in samples {
                assert_eq!(
                    collation.compare(left.as_bytes(), right.as_bytes()),
                    collation
                        .key(left.as_bytes())
                        .cmp(&collation.key(right.as_bytes())),
                    "{collation:?}: {left:?} vs {right:?}"
                );
            }
        }
    }
}

#[test]
fn shared_uca_0900_preserves_existing_table_boundary() {
    assert!(
        std::panic::catch_unwind(|| Collation::Utf8Mb40900AiCi.key("\u{2cea1}".as_bytes()))
            .is_err()
    );
    assert!(!Collation::Utf8Mb40900AiCi
        .key("\u{2cea2}".as_bytes())
        .is_empty());
}

#[test]
fn shared_json_helper_preserves_reject_trailing_escape() {
    assert!(!tidb_datatype::like_matches("\\", "\\", '\\'));
    assert!(tidb_datatype::like_matches("a%b", "a\\%b", '\\'));
    assert!(!tidb_datatype::like_matches("é", "é", 'é'));
}
