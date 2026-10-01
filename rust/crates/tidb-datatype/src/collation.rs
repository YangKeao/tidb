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

//! TiDB's registry/mode facade over the shared TiKV collation kernels.
//!
//! All comparison, key, and LIKE kernels are shared. GB comparison and keys
//! select the native compatibility policy independently of TiKV's wire policy;
//! registry resolution, legacy mode, and immutable-key ownership stay native.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use crate::charset::{
    get_collation_by_id as charset_collation_by_id,
    get_collation_by_name as charset_collation_by_name,
    get_supported_collations as charset_supported_collations, set_new_collation_defaults,
};
use crate::{CharsetError, Collation, CollationInfo};
use tidb_query_datatype::codec::collation::{
    self as shared, collator::*, native::NativeCollation, Collator as SharedCollator, KeyOptions,
};

/// Shared wildcard primitives used by the source-compatible string utilities.
/// No registry, protobuf, or evaluator types cross this narrow facade.
pub mod wildcard {
    pub use tidb_query_datatype::codec::collation::decode_utf8_rune_strict;
    pub use tidb_query_datatype::codec::collation::pattern::{
        compile_bytes, compile_runes, lower_one_string, lower_one_string_excluding_escape_char,
        matches_compiled_bytes, matches_compiled_runes_with, matches_runes, utf8_len, MatchOptions,
        PatternType, TrailingEscape,
    };
}

// Go initializes this to enabled in `pkg/util/collate.init`; bootstrap later
// replaces it with the cluster's persisted compatibility setting.
static NEW_COLLATION_ENABLED: AtomicBool = AtomicBool::new(true);

/// Source `DefaultLen`, used when a string datum has no known length.
pub const DEFAULT_LEN: usize = 0;

/// Errors owned by the collation package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollationError {
    /// Parser charset-registry lookup failure.
    Registry(CharsetError),
    /// The registry knows the name but new-collation mode has no implementation.
    UnsupportedCollation(String),
}

impl fmt::Display for CollationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => error.fmt(formatter),
            Self::UnsupportedCollation(name) => write!(
                formatter,
                "[ddl:1273]Unsupported collation when new collation is enabled: '{name}'"
            ),
        }
    }
}

impl std::error::Error for CollationError {}

impl From<CharsetError> for CollationError {
    fn from(error: CharsetError) -> Self {
        Self::Registry(error)
    }
}

/// Runtime collator resolution. `DerivedBinary` is the legacy mode authority
/// used for every collation name when new collations are disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collator {
    /// A concrete new-collation implementation.
    New(Collation),
    /// Legacy byte comparison with rune-oriented wildcard matching.
    DerivedBinary,
}

impl From<Collation> for Collator {
    fn from(collation: Collation) -> Self {
        Self::New(collation)
    }
}

impl Collator {
    /// Returns the concrete new-collation implementation, if enabled.
    pub const fn new_collation(self) -> Option<Collation> {
        match self {
            Self::New(collation) => Some(collation),
            Self::DerivedBinary => None,
        }
    }

    /// Compares source Go-string bytes.
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        match self {
            Self::New(collation) => collation.compare(left, right),
            Self::DerivedBinary => CollatorUtf8Mb4BinNoPadding::sort_compare(left, right, false)
                .expect("raw binary comparison cannot fail"),
        }
    }

    /// Returns the source sort key.
    pub fn key(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::New(collation) => collation.key(value),
            Self::DerivedBinary => CollatorUtf8Mb4BinNoPadding::sort_key(value)
                .expect("raw binary key into Vec cannot fail"),
        }
    }

    /// Go's allocation-aware `ImmutableKey`; binary collators borrow input.
    pub fn immutable_key<'a>(self, value: &'a [u8]) -> Cow<'a, [u8]> {
        match self {
            Self::New(collation) => collation.immutable_key(value),
            Self::DerivedBinary => {
                CollatorUtf8Mb4BinNoPadding::sort_key_cow(value, KeyOptions::Default)
                    .expect("raw binary key cannot fail")
            }
        }
    }

    /// Returns the key without PAD SPACE preprocessing.
    pub fn key_without_trim_right_space(self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::New(collation) => collation.key_without_trim_right_space(value),
            Self::DerivedBinary => CollatorUtf8Mb4BinNoPadding::sort_key(value)
                .expect("raw binary key into Vec cannot fail"),
        }
    }

    /// Returns the source upper bound for a key.
    pub fn max_key_len(self, value: &[u8]) -> usize {
        match self {
            Self::New(collation) => collation.max_key_len(value),
            Self::DerivedBinary => CollatorUtf8Mb4BinNoPadding::max_sort_key_len(value),
        }
    }

    /// Compiles a wildcard pattern with this collator's equality semantics.
    pub fn pattern(self, pattern: impl AsRef<[u8]>, escape: u8) -> WildcardPattern {
        let pattern = pattern.as_ref();
        match self {
            // Keep DerivedBinary's existing no-padding UTF-8 rune matcher,
            // not the byte matcher of the distinct Binary identity.
            Self::DerivedBinary => {
                WildcardPattern(NativeCollation::Utf8Mb40900Bin.compile_pattern(pattern, escape))
            }
            Self::New(collation) => collation.pattern(pattern, escape),
        }
    }

    /// Matches a dynamic pattern without compiling or allocating target runes.
    pub fn like_match(self, value: &[u8], pattern: &[u8], escape: u8) -> bool {
        match self {
            Self::DerivedBinary => {
                NativeCollation::Utf8Mb40900Bin.matches_pattern(value, pattern, escape)
            }
            Self::New(collation) => collation
                .native_policy()
                .matches_pattern(value, pattern, escape),
        }
        .expect("raw supported LIKE comparison cannot fail")
    }

    /// Whether the source implementation can use the raw input as its key.
    pub const fn can_use_raw_mem_as_key(self) -> bool {
        match self {
            Self::DerivedBinary => CollatorUtf8Mb4BinNoPadding::CAN_USE_RAW_MEM_AS_KEY,
            Self::New(collation) => collation.native_policy().can_use_raw_mem_as_key(),
        }
    }
}

/// Returns whether new collations are enabled.
pub fn new_collation_enabled() -> bool {
    NEW_COLLATION_ENABLED.load(AtomicOrdering::SeqCst)
}

/// The flag behind [`Self::new_collation_enabled`], for the charset
/// registry's own construction: Go's `collate` package init applies
/// `switchDefaultCollation(NewCollationEnabled())` to the static charset
/// tables before any consumer reads a default collation.
pub(crate) fn new_collation_enabled_flag() -> bool {
    NEW_COLLATION_ENABLED.load(AtomicOrdering::SeqCst)
}

/// Source test/configuration switch. Callers must serialize changes.
pub fn set_new_collation_enabled(enabled: bool) {
    set_new_collation_defaults(enabled);
    NEW_COLLATION_ENABLED.store(enabled, AtomicOrdering::SeqCst);
}

/// Checks source-compatible collation-name equivalence.
pub fn compatible_collate(left: &str, right: &str) -> bool {
    const GENERAL: [&str; 2] = ["utf8mb4_general_ci", "utf8_general_ci"];
    const BINARY: [&str; 3] = ["utf8mb4_bin", "utf8_bin", "latin1_bin"];
    const UNICODE: [&str; 2] = ["utf8mb4_unicode_ci", "utf8_unicode_ci"];
    left == right
        || [GENERAL.as_slice(), BINARY.as_slice(), UNICODE.as_slice()]
            .into_iter()
            .any(|class| class.contains(&left) && class.contains(&right))
}

/// Rewrites a protocol collation ID when new collations are enabled.
pub fn rewrite_new_collation_id_if_needed(id: i32) -> i32 {
    if new_collation_enabled() && id >= 0 {
        id.wrapping_neg()
    } else {
        id
    }
}

/// Restores a protocol collation ID when new collations are enabled.
pub fn restore_collation_id_if_needed(id: i32) -> i32 {
    if new_collation_enabled() && id <= 0 {
        id.wrapping_neg()
    } else {
        id
    }
}

/// Resolves a name, falling back exactly as the Go package does.
pub fn get_collator(name: &str) -> Collator {
    get_collator_with_mode(new_collation_enabled(), name)
}

/// Resolves a name using an explicit collation mode.
pub fn get_collator_with_mode(use_new_collation: bool, name: &str) -> Collator {
    if !use_new_collation {
        return Collator::DerivedBinary;
    }
    Collator::New(exact_new_collation(name).unwrap_or(Collation::Utf8Mb4Bin))
}

fn exact_new_collation(name: &str) -> Option<Collation> {
    Some(match name {
        "binary" => Collation::Binary,
        "ascii_bin" => Collation::AsciiBin,
        "latin1_bin" => Collation::Latin1Bin,
        "utf8_bin" => Collation::Utf8Bin,
        "utf8_general_ci" => Collation::Utf8GeneralCi,
        "utf8_unicode_ci" => Collation::Utf8UnicodeCi,
        "utf8mb4_bin" => Collation::Utf8Mb4Bin,
        "utf8mb4_general_ci" => Collation::Utf8Mb4GeneralCi,
        "utf8mb4_unicode_ci" => Collation::Utf8Mb4UnicodeCi,
        "utf8mb4_0900_ai_ci" => Collation::Utf8Mb40900AiCi,
        "utf8mb4_0900_bin" => Collation::Utf8Mb40900Bin,
        "utf8mb4_zh_pinyin_tidb_as_cs" => Collation::Utf8Mb4ZhPinyinTiDbAsCs,
        "gbk_bin" => Collation::GbkBin,
        "gbk_chinese_ci" => Collation::GbkChineseCi,
        "gb18030_bin" => Collation::Gb18030Bin,
        "gb18030_chinese_ci" => Collation::Gb18030ChineseCi,
        _ => return None,
    })
}

/// Returns the legacy binary collator.
pub const fn get_binary_collator() -> Collator {
    Collator::DerivedBinary
}

/// Returns `n` copies of the singleton-compatible legacy binary collator.
pub fn get_binary_collator_slice(length: usize) -> Vec<Collator> {
    vec![Collator::DerivedBinary; length]
}

/// Resolves a numeric ID, falling back exactly as the Go package does.
pub fn get_collator_by_id(id: i32) -> Collator {
    if !new_collation_enabled() {
        return Collator::DerivedBinary;
    }
    let collation = charset_collation_by_id(id)
        .ok()
        .and_then(|row| Collation::from_name(&row.name))
        .unwrap_or(Collation::Utf8Mb4Bin);
    Collator::New(collation)
}

/// Resolves an ID to its name, with TiDB's default fallback.
pub fn collation_id_to_name(id: i32) -> String {
    charset_collation_by_id(id)
        .map(|row| row.name)
        .unwrap_or_else(|_| Collation::DEFAULT.name().to_owned())
}

/// Resolves a name to its ID, with TiDB's default fallback.
pub fn collation_name_to_id(name: &str) -> i32 {
    charset_collation_by_name(name).map_or(46, |row| row.id)
}

/// Checks both registry existence and new-collation implementation support.
pub fn get_supported_collation_by_name(name: &str) -> Result<CollationInfo, CollationError> {
    let row = charset_collation_by_name(name)?;
    if new_collation_enabled() && Collation::from_name(&row.name).is_none() {
        return Err(CollationError::UnsupportedCollation(
            name.chars().take(64).collect(),
        ));
    }
    Ok(row)
}

/// Substitutes the default for a missing or currently unsupported collation.
pub fn substitute_missing_collation_to_default(name: &str) -> String {
    get_supported_collation_by_name(name)
        .map(|_| name.to_owned())
        .unwrap_or_else(|_| Collation::DEFAULT.name().to_owned())
}

/// Returns the collations exposed by the active mode.
pub fn supported_collations() -> Vec<CollationInfo> {
    if !new_collation_enabled() {
        return charset_supported_collations();
    }
    let mut rows: Vec<_> = [
        Collation::Binary,
        Collation::AsciiBin,
        Collation::Latin1Bin,
        Collation::Utf8Bin,
        Collation::Utf8GeneralCi,
        Collation::Utf8UnicodeCi,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb40900Bin,
        Collation::GbkBin,
        Collation::GbkChineseCi,
        Collation::Gb18030Bin,
        Collation::Gb18030ChineseCi,
    ]
    .into_iter()
    .map(|collation| {
        charset_collation_by_name(collation.name())
            .expect("implemented collation must exist in parser registry")
    })
    .collect();
    rows.sort_by(|left, right| left.name.cmp(&right.name));
    rows
}

/// Whether this is a default UTF8MB4 collation accepted by TiDB migration.
pub fn is_default_collation_for_utf8mb4(name: &str) -> bool {
    matches!(
        name,
        "utf8mb4_bin" | "utf8mb4_general_ci" | "utf8mb4_0900_ai_ci"
    )
}

/// Whether this collation is case-insensitive.
pub fn is_ci_collation(name: &str) -> bool {
    // Preserve the source's exact-name whitelist: uppercase spellings and
    // UTF8MB3 aliases remain false even though registry resolution accepts them.
    Collation::from_name(name)
        .is_some_and(|collation| collation.name() == name && collation.native_policy().is_ci())
}

/// Converts a CI collation to the corresponding binary collation.
pub fn binary_collation_name(name: &str) -> &str {
    match name {
        "utf8_general_ci" | "utf8_unicode_ci" => "utf8_bin",
        "utf8mb4_general_ci" | "utf8mb4_unicode_ci" | "utf8mb4_0900_ai_ci" => "utf8mb4_bin",
        "gbk_chinese_ci" => "gbk_bin",
        "gb18030_chinese_ci" => "gb18030_bin",
        _ => name,
    }
}

/// Converts a name to its binary counterpart and resolves that collator.
pub fn binary_collator(name: &str) -> Collator {
    get_collator(binary_collation_name(name))
}

/// Whether a storage sort key is byte-identical to its raw input.
pub fn is_bin_collation(name: &str) -> bool {
    matches!(
        name,
        "ascii_bin" | "latin1_bin" | "utf8_bin" | "utf8mb4_bin" | "binary" | "utf8mb4_0900_bin"
    )
}

/// Whether the collation uses PAD SPACE semantics.
pub fn is_pad_space_collation(name: &str) -> bool {
    !matches!(name, "binary" | "utf8mb4_0900_ai_ci" | "utf8mb4_0900_bin")
}

/// Converts a name to its protocol ID.
pub fn collation_to_proto(name: &str) -> i32 {
    rewrite_new_collation_id_if_needed(collation_name_to_id(name))
}

/// Converts a protocol ID to its collation name.
pub fn proto_to_collation(id: i32) -> String {
    collation_id_to_name(restore_collation_id_if_needed(id))
}

/// A compiled collation-aware SQL LIKE wildcard pattern.
#[derive(Debug, Clone)]
pub struct WildcardPattern(shared::pattern::CompiledPattern);

impl WildcardPattern {
    /// Matches arbitrary Go-string bytes against the compiled pattern.
    pub fn is_match(&self, value: &[u8]) -> bool {
        self.0
            .is_match(value)
            .expect("raw supported LIKE comparison cannot fail")
    }
}

impl Collation {
    /// Map metadata to its shared native policy without consulting global mode
    /// or converting registry/wire IDs. This is not an input-origin label.
    pub const fn native_policy(self) -> NativeCollation {
        match self {
            Self::Binary => NativeCollation::Binary,
            Self::AsciiBin => NativeCollation::AsciiBin,
            Self::Latin1Bin => NativeCollation::Latin1Bin,
            Self::Utf8Bin => NativeCollation::Utf8Bin,
            Self::Utf8GeneralCi => NativeCollation::Utf8GeneralCi,
            Self::Utf8UnicodeCi => NativeCollation::Utf8UnicodeCi,
            Self::Utf8Mb4Bin => NativeCollation::Utf8Mb4Bin,
            Self::Utf8Mb4GeneralCi => NativeCollation::Utf8Mb4GeneralCi,
            Self::Utf8Mb4UnicodeCi => NativeCollation::Utf8Mb4UnicodeCi,
            Self::Utf8Mb40900AiCi => NativeCollation::Utf8Mb40900AiCi,
            Self::Utf8Mb40900Bin => NativeCollation::Utf8Mb40900Bin,
            Self::Utf8Mb4ZhPinyinTiDbAsCs => NativeCollation::Utf8Mb4ZhPinyinTiDbAsCs,
            Self::GbkBin => NativeCollation::GbkBin,
            Self::GbkChineseCi => NativeCollation::GbkChineseCi,
            Self::Gb18030Bin => NativeCollation::Gb18030Bin,
            Self::Gb18030ChineseCi => NativeCollation::Gb18030ChineseCi,
        }
    }

    /// Compiles this explicit new-collation wildcard matcher.
    pub fn pattern(self, pattern: impl AsRef<[u8]>, escape: u8) -> WildcardPattern {
        WildcardPattern(
            self.native_policy()
                .compile_pattern(pattern.as_ref(), escape),
        )
    }

    /// Compares arbitrary Go-string bytes using TiDB's source semantics.
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        self.native_policy()
            .compare(left, right)
            .expect("raw supported collation comparison cannot fail")
    }

    /// Returns TiDB's sort key for arbitrary Go-string bytes.
    pub fn key(self, value: &[u8]) -> Vec<u8> {
        self.key_with_options(value, KeyOptions::Default)
    }

    fn key_with_options(self, value: &[u8], options: KeyOptions) -> Vec<u8> {
        self.native_policy()
            .key(value, options)
            .expect("raw supported collation key into Vec cannot fail")
    }

    /// Go's allocation-aware `ImmutableKey`; binary collators borrow input.
    pub fn immutable_key<'a>(self, value: &'a [u8]) -> Cow<'a, [u8]> {
        self.native_policy()
            .key_cow(value, KeyOptions::Default)
            .expect("raw supported collation key cannot fail")
    }

    /// Returns the source key without the collation's PAD SPACE preprocessing.
    pub fn key_without_trim_right_space(self, value: &[u8]) -> Vec<u8> {
        self.key_with_options(value, KeyOptions::NoPad)
    }

    /// Returns the allocation estimate exposed by the corresponding Go collator.
    /// Native GB18030 PUA encoding can exceed this historical estimate.
    pub fn max_key_len(self, value: &[u8]) -> usize {
        self.native_policy().max_key_len(value)
    }
}

pub(crate) fn decode_rune(value: &[u8]) -> Result<(u32, usize), ()> {
    shared::decode_utf8_rune_strict(value)
        .map(|(ch, width)| (ch as u32, width))
        .ok_or(())
}

pub(crate) fn go_rune_count(value: &[u8]) -> usize {
    shared::utf8_rune_count(value)
}

/// The width in bytes of the rune starting at `value`, Go's `DecodeRune`
/// convention: a byte that starts no valid sequence is one RuneError one byte
/// wide, never an error.
pub(crate) fn rune_width(value: &[u8]) -> usize {
    decode_rune(value).map_or(1, |(_, width)| width)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path, process::Command, sync::OnceLock};

    use sha2::{Digest, Sha256};

    use super::{Collation, CollatorUtf8Mb40900AiCi, CollatorUtf8Mb4UnicodeCi, SharedCollator};

    // Original Go long-map rune inventory; values now come only from TiKV.
    const LONG_0400: [char; 22] = [
        '\u{321d}', '\u{321e}', '\u{327c}', '\u{3307}', '\u{3315}', '\u{3316}', '\u{3317}',
        '\u{3319}', '\u{331a}', '\u{3320}', '\u{332b}', '\u{332e}', '\u{3332}', '\u{3334}',
        '\u{3336}', '\u{3347}', '\u{334a}', '\u{3356}', '\u{337f}', '\u{33ae}', '\u{33af}',
        '\u{fdfb}',
    ];

    fn long_0900() -> impl Iterator<Item = char> {
        LONG_0400.into_iter().chain([
            '\u{fdfa}',
            '\u{fffd}',
            '\u{1f19c}',
            '\u{1f1a8}',
            '\u{1f1a9}',
        ])
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn verify_source_contracts() {
        static CHECKED: OnceLock<()> = OnceLock::new();
        CHECKED.get_or_init(|| {
            let script =
                Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/generate_collation_data.py");
            let output = Command::new("python3")
                .arg("-B")
                .arg(script)
                .arg("--check")
                .output()
                .expect("run shared collation source check");
            assert!(
                output.status.success(),
                "collation source check failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        });
    }

    /// `pkg/util/collate/ucadata/unicode_ci_data_test.go::TestUnicode0400IsTheSame`.
    #[test]
    fn test_unicode_0400_is_the_same() {
        verify_source_contracts();
    }

    #[test]
    fn generated_images_have_source_pinned_lengths_and_hashes() {
        // The source checker hashes all five migrated General/UCA authority
        // images incrementally and compares shared tables to those sources.
        // No second production copy of those weights is needed for this gate.
        verify_source_contracts();
        // Read the single shared authority, including non-scalar table slots.
        // GBK's shared image is BE u16; retain the original native LE oracle.
        // Runtime reads avoid embedding another copy in the native test binary.
        let shared_tables = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../tikv/components/tidb_query_datatype/src/codec/collation/collator");
        let mut gbk = std::fs::read(shared_tables.join("gbk_chinese_ci.data"))
            .expect("read shared GBK CI table");
        let gb18030 = std::fs::read(shared_tables.join("gb18030_chinese_ci.data"))
            .expect("read shared GB18030 CI table");
        assert_eq!(gbk.len(), 131_072);
        assert_eq!(gb18030.len(), 4_456_448);
        for pair in gbk.chunks_exact_mut(2) {
            pair.swap(0, 1);
        }
        assert_eq!(
            digest(&gbk),
            "f6f63c33fa57eeaffa5d46841694adab58bd9cddfac3f92389dec4564a6036d6"
        );
        assert_eq!(
            digest(&gb18030),
            "64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d"
        );
    }

    /// The UCA 4.0 half of `TestAllItemInLongRUneMapIsUnique`.
    #[test]
    fn all_uca_0400_long_rune_weights_are_unique() {
        verify_source_contracts();
        let rows: Vec<_> = LONG_0400
            .into_iter()
            .map(CollatorUtf8Mb4UnicodeCi::char_weight)
            .collect();
        assert_eq!(rows.len(), 22);
        assert_eq!(rows.iter().copied().collect::<HashSet<_>>().len(), 22);
    }

    /// The UCA 9.0 half of `TestAllItemInLongRUneMapIsUnique`.
    #[test]
    fn all_uca_0900_long_rune_weights_are_unique() {
        verify_source_contracts();
        let rows: Vec<_> = long_0900()
            .map(CollatorUtf8Mb40900AiCi::char_weight)
            .collect();
        assert_eq!(rows.len(), 27);
        assert_eq!(rows.iter().copied().collect::<HashSet<_>>().len(), 27);
    }

    /// `TestHangulJamoHasOnlyOneWeight`.
    #[test]
    fn uca_0900_hangul_jamo_has_only_one_weight() {
        verify_source_contracts();
        for codepoint in 0x1100..0x11FF {
            let weight = CollatorUtf8Mb40900AiCi::char_weight(char::from_u32(codepoint).unwrap());
            assert_eq!(weight >> 16, 0);
        }
    }

    /// `TestFirstIsNotZero`.
    #[test]
    fn every_uca_0900_long_weight_starts_nonzero() {
        verify_source_contracts();
        for rune in long_0900() {
            assert_ne!(CollatorUtf8Mb40900AiCi::char_weight(rune) as u64, 0);
        }
    }

    #[test]
    fn uca_0900_surrogate_marker_uses_go_map_zero_value() {
        // The checker preserves Go's missing-map zero value for all 2048
        // surrogate slots. TiKV's different fallback is unreachable for char;
        // do not add a non-scalar product API merely to inspect table metadata.
        verify_source_contracts();
        assert_eq!(char::from_u32(0xD800), None);
        assert_eq!(char::from_u32(0xDFFF), None);
    }

    #[test]
    fn every_uca_0400_long_marker_has_exactly_one_expansion() {
        // Source checker compares the complete marker and explicit-map sets,
        // including source slots that cannot be passed as a Rust char.
        verify_source_contracts();
        assert_eq!(LONG_0400.into_iter().collect::<HashSet<_>>().len(), 22);
        for rune in LONG_0400 {
            assert_ne!(CollatorUtf8Mb4UnicodeCi::char_weight(rune), 0xFFFD);
        }
    }

    #[test]
    fn max_key_lengths_and_without_trim_follow_go_collators() {
        assert_eq!(Collation::Binary.max_key_len(b"a "), 2);
        assert_eq!(Collation::Utf8GeneralCi.max_key_len("😜".as_bytes()), 2);
        assert_eq!(Collation::Utf8UnicodeCi.max_key_len("😜".as_bytes()), 16);
        assert_eq!(Collation::Utf8GeneralCi.max_key_len(&[0xFF]), 2);
        assert_eq!(Collation::Utf8UnicodeCi.max_key_len(&[0xFF]), 16);
        assert_eq!(Collation::Utf8Mb4Bin.key(b"a "), b"a");
        assert_eq!(
            Collation::Utf8Mb4Bin.key_without_trim_right_space(b"a "),
            b"a "
        );
        assert_eq!(
            Collation::Utf8GeneralCi.key_without_trim_right_space(b"a "),
            [0, 0x41, 0, 0x20]
        );
    }
}
