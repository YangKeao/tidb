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

//! Shared byte-preserving policy from TiDB's `encodingBase.Transform`.
//!
//! This module intentionally knows nothing about a charset registry or a
//! decoder.  Encoding leaves supply source and converted groups and an error
//! constructor; this module owns the operation bits, first-error behavior,
//! replacement, truncation, and source/converted collection policy.

pub use tidb_query_datatype::codec::collation::native_encoding::TransformOp;
#[cfg(test)]
use tidb_query_datatype::codec::collation::native_encoding::TransformPolicy as SharedTransformPolicy;

/// Bytes and the optional first invalid-group error returned by Transform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformResult<E> {
    pub(crate) bytes: Vec<u8>,
    pub(crate) error: Option<E>,
}

impl<E> TransformResult<E> {
    pub(crate) fn new(bytes: Vec<u8>, error: Option<E>) -> Self {
        Self { bytes, error }
    }

    /// Returns transformed bytes, including replacement bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the first invalid-group error when suppression was disabled.
    pub fn error(&self) -> Option<&E> {
        self.error.as_ref()
    }

    /// Splits the result into owned bytes and an optional error.
    pub fn into_parts(self) -> (Vec<u8>, Option<E>) {
        (self.bytes, self.error)
    }
}

/// Stateful source-shaped operation policy for one `Transform` call.
///
/// The caller invokes [`TransformPolicy::push`] in source order.  Returning
/// `false` means the caller must stop visiting groups (the trim policy); all
/// other modes return `true`.  The first error is retained even when a
/// replacement byte is emitted, matching Go's `(bytes, error)` result.
#[cfg(test)]
pub(crate) struct TransformPolicy<E, F>
where
    F: Fn(&[u8]) -> E,
{
    shared: SharedTransformPolicy<E, F>,
}

#[cfg(test)]
impl<E, F> TransformPolicy<E, F>
where
    F: Fn(&[u8]) -> E,
{
    pub(crate) fn new(capacity: usize, op: TransformOp, make_error: F) -> Self {
        Self {
            shared: SharedTransformPolicy::new(capacity, op, make_error),
        }
    }

    /// Consumes one `(from, to, valid)` group and returns whether to continue.
    pub(crate) fn push(&mut self, from: &[u8], to: &[u8], valid: bool) -> bool {
        self.shared.push(from, to, valid)
    }

    pub(crate) fn finish(self) -> TransformResult<E> {
        let (bytes, error) = self.shared.finish();
        TransformResult::new(bytes, error)
    }
}

#[cfg(test)]
mod tests {
    use super::{TransformOp, TransformPolicy};

    fn run(op: TransformOp) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut policy = TransformPolicy::new(3, op, |invalid| invalid.to_vec());
        for (from, to, valid) in [
            (&b"a"[..], &b"A"[..], true),
            (&b"!"[..], &b"?"[..], false),
            (&b"b"[..], &b"B"[..], true),
        ] {
            if !policy.push(from, to, valid) {
                break;
            }
        }
        let result = policy.finish();
        (result.bytes().to_vec(), result.error().cloned())
    }

    #[test]
    fn source_modes_preserve_bytes_error_and_truncation() {
        assert_eq!(run(TransformOp::REPLACE_NO_ERR), (b"a?b".to_vec(), None));
        assert_eq!(
            run(TransformOp::REPLACE),
            (b"a?b".to_vec(), Some(b"!".to_vec()))
        );
        assert_eq!(
            run(TransformOp::ENCODE),
            (b"A".to_vec(), Some(b"!".to_vec()))
        );
        assert_eq!(run(TransformOp::DECODE_NO_ERR), (b"A".to_vec(), None));
    }

    #[test]
    fn source_collection_precedence_matches_encoding_base() {
        let mut policy = TransformPolicy::new(
            1,
            TransformOp::COLLECT_FROM | TransformOp::COLLECT_TO,
            |bytes| bytes.to_vec(),
        );
        assert!(policy.push(b"from", b"to", true));
        let result = policy.finish();
        assert_eq!(result.bytes(), b"from");
    }
}

#[cfg(test)]
#[test]
fn shared_encoding_foundation_preserves_flags_groups_and_leaf_fast_paths() {
    use crate::ascii_encoding::ASCII_ENCODING;
    use crate::multibyte_encoding::{count_valid_bytes, count_valid_bytes_decode, Encoding};
    use crate::utf8_encoding::{UTF8_ENCODING, UTF8_MB3_STRICT_ENCODING};
    use std::cell::Cell;
    let flags = [
        TransformOp::FROM_UTF8,
        TransformOp::TO_UTF8,
        TransformOp::TRUNCATE_TRIM,
        TransformOp::TRUNCATE_REPLACE,
        TransformOp::COLLECT_FROM,
        TransformOp::COLLECT_TO,
        TransformOp::SKIP_ERROR,
    ];
    for bits in 0u16..128 {
        let mut op = TransformOp::default();
        for (index, flag) in flags.iter().enumerate() {
            if bits & (1 << index) != 0 {
                op |= *flag;
            }
        }
        assert_eq!(format!("{op:?}"), format!("TransformOp({bits})"));
        let calls = Cell::new(0usize);
        let mut policy = TransformPolicy::new(0, op, |bytes: &[u8]| {
            calls.set(calls.get() + 1);
            bytes.to_vec()
        });
        for (from, to, valid) in [
            (b"a", b"A", true),
            (b"!", b"_", false),
            (b"b", b"B", true),
            (b"~", b"-", false),
        ] {
            if !policy.push(from, to, valid) {
                break;
            }
        }
        let collected: &[u8] = if bits & 16 != 0 {
            b"a!b~".as_slice()
        } else if bits & 32 != 0 {
            b"A_B-"
        } else {
            b""
        };
        let expected = if bits & 4 != 0 {
            collected.get(..1).unwrap_or_default().to_vec()
        } else if bits & 8 != 0 {
            if bits & 16 != 0 {
                b"a?b?".to_vec()
            } else if bits & 32 != 0 {
                b"A?B?".to_vec()
            } else {
                b"??".to_vec()
            }
        } else {
            collected.to_vec()
        };
        let (bytes, error) = policy.finish().into_parts();
        assert_eq!(bytes, expected, "flags {bits}");
        assert_eq!(error, (bits & 64 == 0).then(|| b"!".to_vec()));
        assert_eq!(calls.get(), usize::from(bits & 64 == 0));
        assert_eq!(ASCII_ENCODING.transform(b"a", op).bytes(), b"a");
        assert_eq!(UTF8_ENCODING.transform(b"a", op).bytes(), b"a");
        assert_eq!(UTF8_MB3_STRICT_ENCODING.transform(b"a", op).bytes(), b"a");
        for encoding in [
            Encoding::Ascii,
            Encoding::Utf8,
            Encoding::Utf8Mb3Strict,
            Encoding::Gbk,
            Encoding::Gb18030,
        ] {
            assert_eq!(
                encoding.transform(b"a", op).bytes(),
                if bits & 48 != 0 { b"a".as_slice() } else { b"" }
            );
        }
        for encoding in [Encoding::Latin1, Encoding::Binary] {
            assert_eq!(
                encoding.transform(b"\xff", op).into_parts(),
                (vec![255], None)
            );
        }
    }
    let source = b"\xffabcZ";
    let ascii = ASCII_ENCODING.transform(source, TransformOp::REPLACE);
    let utf8 = UTF8_ENCODING.transform(source, TransformOp::REPLACE);
    assert_eq!(ascii.bytes(), b"?Z");
    assert_eq!(
        ascii.error().unwrap().to_string(),
        "Invalid ascii character string: 'FF616263'"
    );
    assert_eq!(
        format!("{:?}", ascii.error().unwrap()),
        "AsciiTransformError { invalid: [255, 97, 98, 99] }"
    );
    assert_eq!(utf8.bytes(), b"?abcZ");
    assert_eq!(
        utf8.error().unwrap().to_string(),
        "Invalid utf8 character string: 'FF'"
    );
    assert_eq!(
        Encoding::Utf8
            .transform(source, TransformOp::REPLACE)
            .error()
            .unwrap()
            .to_string(),
        "Invalid utf8mb4 character string: 'FF'"
    );
    assert_eq!(UTF8_MB3_STRICT_ENCODING.mb_len("😂".as_bytes()), 4);
    assert_eq!(
        count_valid_bytes(Encoding::Utf8Mb3Strict, "a😂b".as_bytes()),
        1
    );
    assert_eq!(count_valid_bytes(Encoding::Gbk, "一€".as_bytes()), 3);
    assert_eq!(count_valid_bytes_decode(Encoding::Gbk, b"\xd2\xbb\xff"), 2);
    for encoding in [
        Encoding::Ascii,
        Encoding::Utf8,
        Encoding::Utf8Mb3Strict,
        Encoding::Latin1,
        Encoding::Binary,
        Encoding::Gbk,
        Encoding::Gb18030,
    ] {
        assert_eq!(encoding.peek(b"ab"), b"a");
        assert_eq!(encoding.mb_len(b"a"), 0);
        assert!(encoding.is_valid(b"ab"));
        let mut calls = 0;
        encoding.foreach(b"ab", TransformOp::FROM_UTF8, |from, to, valid| {
            calls += 1;
            assert_eq!(from, b"a");
            assert_eq!(to, b"a");
            assert!(valid);
            false
        });
        assert_eq!(calls, 1);
    }
}
