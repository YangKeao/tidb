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

//! Complete encoding dispatch from `pkg/parser/charset/encoding*.go`.

use std::fmt;

use tidb_mysql::{to_lowercase as go_simple_lowercase, to_uppercase as go_simple_uppercase};
use tidb_query_datatype::codec::collation::native_encoding::SharedNativeEncoding;

use crate::charset::{CaseRange, GB18030_CASES, GBK_CASES};
use crate::encoding_base::{TransformOp, TransformResult};

/// Source `EncodingTp` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i8)]
pub enum EncodingType {
    /// No encoding.
    #[default]
    None = 0,
    /// Four-byte UTF-8.
    Utf8,
    /// Strict three-byte UTF-8.
    Utf8Mb3Strict,
    /// ASCII.
    Ascii,
    /// TiDB-compatible Latin-1.
    Latin1,
    /// Binary.
    Binary,
    /// GBK.
    Gbk,
    /// GB18030-2022.
    Gb18030,
}

/// A stateless supported encoding implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// Four-byte UTF-8.
    Utf8,
    /// Strict three-byte UTF-8.
    Utf8Mb3Strict,
    /// ASCII.
    Ascii,
    /// Byte-preserving Latin-1 compatibility behavior.
    Latin1,
    /// Byte-preserving binary behavior.
    Binary,
    /// GBK.
    Gbk,
    /// GB18030-2022.
    Gb18030,
}

/// The first invalid group returned with transformed bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingError {
    charset: &'static str,
    invalid: Vec<u8>,
}

impl EncodingError {
    /// Returns the canonical charset name.
    pub const fn charset(&self) -> &'static str {
        self.charset
    }

    /// Returns the exact invalid source group.
    pub fn invalid_bytes(&self) -> &[u8] {
        &self.invalid
    }
}

impl fmt::Display for EncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Invalid {} character string: '", self.charset)?;
        for byte in &self.invalid {
            write!(formatter, "{byte:02X}")?;
        }
        formatter.write_str("'")
    }
}

impl std::error::Error for EncodingError {}

/// Bytes and the optional first source conversion error.
pub type EncodingResult = TransformResult<EncodingError>;

impl Encoding {
    const fn shared(self) -> SharedNativeEncoding {
        match self {
            Self::Utf8 => SharedNativeEncoding::Utf8,
            Self::Utf8Mb3Strict => SharedNativeEncoding::Utf8Mb3Strict,
            Self::Ascii => SharedNativeEncoding::Ascii,
            Self::Latin1 => SharedNativeEncoding::Latin1,
            Self::Binary => SharedNativeEncoding::Binary,
            Self::Gbk => SharedNativeEncoding::Gbk,
            Self::Gb18030 => SharedNativeEncoding::Gb18030,
        }
    }

    const fn from_shared(encoding: SharedNativeEncoding) -> Self {
        match encoding {
            SharedNativeEncoding::Utf8 => Self::Utf8,
            SharedNativeEncoding::Utf8Mb3Strict => Self::Utf8Mb3Strict,
            SharedNativeEncoding::Ascii => Self::Ascii,
            SharedNativeEncoding::Latin1 => Self::Latin1,
            SharedNativeEncoding::Binary => Self::Binary,
            SharedNativeEncoding::Gbk => Self::Gbk,
            SharedNativeEncoding::Gb18030 => Self::Gb18030,
        }
    }

    /// Returns the source registry name.
    pub const fn name(self) -> &'static str {
        self.shared().name()
    }

    /// Returns the source encoding type.
    pub const fn encoding_type(self) -> EncodingType {
        match self {
            Self::Utf8 => EncodingType::Utf8,
            Self::Utf8Mb3Strict => EncodingType::Utf8Mb3Strict,
            Self::Ascii => EncodingType::Ascii,
            Self::Latin1 => EncodingType::Latin1,
            Self::Binary => EncodingType::Binary,
            Self::Gbk => EncodingType::Gbk,
            Self::Gb18030 => EncodingType::Gb18030,
        }
    }

    /// Returns the next encoded character group.
    pub fn peek(self, source: &[u8]) -> &[u8] {
        self.shared().peek(source)
    }

    /// Returns a multibyte width, or zero for single-byte/invalid input.
    pub fn mb_len(self, source: &[u8]) -> usize {
        self.shared().mb_len(source)
    }

    /// Checks whether UTF-8 input can be represented by this encoding.
    pub fn is_valid(self, source: &[u8]) -> bool {
        self.shared().is_valid(source)
    }

    /// Visits source groups in order.
    pub fn foreach<F>(self, source: &[u8], operation: TransformOp, visit: F)
    where
        F: FnMut(&[u8], &[u8], bool) -> bool,
    {
        self.shared().foreach(source, operation, visit);
    }

    /// Applies the source transform policy.
    pub fn transform(self, source: &[u8], operation: TransformOp) -> EncodingResult {
        let (bytes, error) = self
            .shared()
            .transform(source, operation, |invalid| EncodingError {
                charset: self.name(),
                invalid: invalid.to_vec(),
            });
        TransformResult::new(bytes, error)
    }

    /// Applies source-compatible upper-case mapping.
    pub fn to_upper(self, source: &str) -> String {
        match self {
            Self::Gbk => map_case(source, GBK_CASES, Case::Upper),
            Self::Gb18030 => map_case(source, GB18030_CASES, Case::Upper),
            _ => go_simple_uppercase(source),
        }
    }

    /// Applies source-compatible lower-case mapping.
    pub fn to_lower(self, source: &str) -> String {
        match self {
            Self::Gbk => map_case(source, GBK_CASES, Case::Lower),
            Self::Gb18030 => map_case(source, GB18030_CASES, Case::Lower),
            _ => go_simple_lowercase(source),
        }
    }
}

/// Checks whether a name has a complete encoding implementation.
pub fn is_supported_encoding(charset: &str) -> bool {
    tidb_query_datatype::codec::collation::native_encoding::is_supported_encoding(charset)
}

/// Finds an encoding; empty and unknown names use binary, exactly as Go does.
pub fn find_encoding(charset: &str) -> Encoding {
    Encoding::from_shared(
        tidb_query_datatype::codec::collation::native_encoding::find_encoding(charset),
    )
}

/// Finds an encoding while treating UTF-8 as byte-preserving binary.
pub fn find_encoding_take_utf8_as_noop(charset: &str) -> Encoding {
    Encoding::from_shared(
        tidb_query_datatype::codec::collation::native_encoding::find_encoding_take_utf8_as_noop(
            charset,
        ),
    )
}

/// Counts the valid UTF-8 prefix representable by an encoding.
pub fn count_valid_bytes(encoding: Encoding, source: &[u8]) -> usize {
    count_valid(encoding, source, TransformOp::FROM_UTF8)
}

/// Counts the valid encoded prefix decodable to UTF-8.
pub fn count_valid_bytes_decode(encoding: Encoding, source: &[u8]) -> usize {
    count_valid(encoding, source, TransformOp::TO_UTF8)
}

fn count_valid(encoding: Encoding, source: &[u8], operation: TransformOp) -> usize {
    encoding.shared().count_valid(source, operation)
}

enum Case {
    Upper,
    Lower,
}

fn map_case(source: &str, cases: &[CaseRange], requested: Case) -> String {
    let default_mapped = match requested {
        Case::Upper => go_simple_uppercase(source),
        Case::Lower => go_simple_lowercase(source),
    };
    let mut output = String::with_capacity(source.len());
    for (character, default_character) in source.chars().zip(default_mapped.chars()) {
        let codepoint = u32::from(character);
        if let Some(range) = cases
            .iter()
            .find(|range| range.lo <= codepoint && codepoint <= range.hi)
        {
            let delta = match requested {
                Case::Upper => range.upper,
                Case::Lower => range.lower,
            };
            output.push(
                char::from_u32(codepoint.wrapping_add_signed(delta))
                    .expect("source special-case delta is a Unicode scalar"),
            );
        } else {
            output.push(default_character);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_validation_rows() {
        for (charset, source, expected, valid_prefix, valid) in [
            ("ascii", b"qwerty".as_slice(), b"qwerty".as_slice(), 6, true),
            ("ascii", "qwÊrty".as_bytes(), b"qw?rty", 2, false),
            ("utf8", "😂".as_bytes(), "😂".as_bytes(), 4, true),
            ("gbk", "À".as_bytes(), b"?", 0, false),
            ("gb18030", "À".as_bytes(), "À".as_bytes(), 2, true),
            ("gb18030", "😂".as_bytes(), "😂".as_bytes(), 4, true),
        ] {
            let encoding = find_encoding(charset);
            assert_eq!(encoding.is_valid(source), valid, "{charset}");
            assert_eq!(
                encoding
                    .transform(source, TransformOp::REPLACE_NO_ERR)
                    .bytes(),
                expected,
                "{charset}"
            );
            assert_eq!(
                count_valid_bytes(encoding, source),
                valid_prefix,
                "{charset}"
            );
        }
        assert!(!Encoding::Utf8Mb3Strict.is_valid("😂".as_bytes()));
    }

    #[test]
    fn source_gbk_and_gb18030_vectors() {
        let gbk = Encoding::Gbk;
        let encoded = gbk.transform("一二三".as_bytes(), TransformOp::ENCODE_REPLACE);
        assert_eq!(encoded.bytes(), b"\xD2\xBB\xB6\xFE\xC8\xFD");
        assert!(encoded.error().is_none());
        let euro = gbk.transform("€a".as_bytes(), TransformOp::ENCODE_REPLACE);
        assert_eq!(euro.bytes(), b"?a");
        assert!(euro.error().is_some());

        let gb18030 = Encoding::Gb18030;
        for (text, expected) in [
            ("🀁", b"\x94\x38\xE1\x31".as_slice()),
            ("€", b"\xA2\xE3"),
            ("ḿ", b"\xA8\xBC"),
        ] {
            let result = gb18030.transform(text.as_bytes(), TransformOp::ENCODE_REPLACE);
            assert_eq!(result.bytes(), expected, "{text}");
            assert!(result.error().is_none());
        }
        let decoded = gb18030.transform(
            b"\xB0\xB2\x84\x31\xA4\x37\x30\x84\x31\xA4\x37\x32",
            TransformOp::DECODE_REPLACE,
        );
        assert_eq!(decoded.bytes(), "安�0�2".as_bytes());
        assert!(decoded.error().is_none());
    }

    #[test]
    fn source_case_conversion_uses_go_simple_rune_mappings() {
        assert_eq!(Encoding::Utf8.to_upper("straße"), "STRAßE");
        assert_eq!(Encoding::Utf8.to_lower("İ"), "i");
        assert_eq!(Encoding::Gbk.to_upper("straßeà"), "STRAßEà");
        assert_eq!(Encoding::Gb18030.to_lower("İ"), "i");
    }

    #[test]
    fn source_gb18030_mb_len_panics_for_truncated_four_byte_prefix() {
        for source in [&[0x81, 0x30][..], &[0x81, 0x30, 0x81][..]] {
            assert!(std::panic::catch_unwind(|| Encoding::Gb18030.mb_len(source)).is_err());
        }
    }
}
