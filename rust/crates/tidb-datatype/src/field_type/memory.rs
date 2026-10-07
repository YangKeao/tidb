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

//! Source storage-width and allocation accounting for [`FieldType`].

use super::{FieldType, FieldTypeCode, VAR_STORAGE_LEN};
use crate::GoString;
use tidb_query_datatype::codec::native_type_name;

impl FieldType {
    /// Returns the source storage-width estimate.
    pub fn storage_length(&self) -> i64 {
        native_type_name::native_field_storage_length(
            self.code().as_shared_type_name_code(),
            self.flen,
            self.decimal,
            VAR_STORAGE_LEN,
        )
    }

    /// Observes the logical payload charged for a detached metadata snapshot.
    ///
    /// Counts `size_of::<FieldType>()`, exact charset/collation byte lengths,
    /// visible element headers (`len * size_of::<GoString>()`) and byte lengths,
    /// and the independent visible binary-marker length (`len * size_of::<bool>()`).
    /// All additions and multiplications are checked; overflow returns `None`.
    /// Shared immutable string contents are charged per visible occurrence.
    ///
    /// This borrows the visible elements without allocating, cloning, comparing,
    /// hashing or normalizing metadata. It counts neither spare capacity nor
    /// allocator overhead, original ownership, or process/peak memory. Nil and
    /// allocated-empty slices have the same visible payload charge.
    ///
    /// The observation does not freeze aliases: callers requiring a bound on a
    /// later equality/copy/projection must prevent GoSharedSlice alias mutation
    /// throughout that interval. This is not an atomic snapshot or byte identity.
    #[must_use]
    pub fn checked_snapshot_payload_bytes(&self) -> Option<usize> {
        self.elems.with_visible(|elements| {
            snapshot_payload_bytes(
                self.charset_name.len(),
                self.collation_name.len(),
                elements.len(),
                self.elems_is_binary_literal.len(),
                elements.iter().map(GoString::len),
            )
        })
    }

    /// Mirrors Go `FieldType.MemoryUsage` on supported 64-bit targets.
    pub fn memory_usage(&self) -> usize {
        const GO_EMPTY_FIELD_TYPE_SIZE: usize = 120;
        GO_EMPTY_FIELD_TYPE_SIZE
            + self.charset_name.len()
            + self.collation_name.len()
            + self.elems.capacity() * 16
            + self
                .elems
                .with_visible(|elements| elements.iter().map(GoString::len).sum::<usize>())
            + self.elems_is_binary_literal.capacity() * std::mem::size_of::<bool>()
    }
}

// Keep the arithmetic separately testable without constructing impossibly large
// allocations or invalid slice headers. The iterator is borrowed/lazy in the
// observer; no element payload or private marker slice is copied to count it.
fn snapshot_payload_bytes(
    charset_bytes: usize,
    collation_bytes: usize,
    element_count: usize,
    marker_count: usize,
    mut element_lengths: impl Iterator<Item = usize>,
) -> Option<usize> {
    let headers = element_count.checked_mul(std::mem::size_of::<GoString>())?;
    let markers = marker_count.checked_mul(std::mem::size_of::<bool>())?;
    let base = std::mem::size_of::<FieldType>()
        .checked_add(charset_bytes)?
        .checked_add(collation_bytes)?
        .checked_add(headers)?
        .checked_add(markers)?;
    element_lengths.try_fold(base, usize::checked_add)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::go_runtime::GoSharedSlice;

    fn base(field: &FieldType) -> usize {
        std::mem::size_of::<FieldType>() + field.charset_name().len() + field.collation_name().len()
    }

    #[test]
    fn snapshot_payload_base_and_exact_name_bytes() {
        let field = FieldType::new(FieldTypeCode::LongLong)
            .with_charset_name("uTf8Mb4")
            .with_collation_name("source_列");
        assert_eq!(field.charset_name(), "uTf8Mb4");
        assert_eq!(field.collation_name(), "source_列");
        assert_eq!(field.checked_snapshot_payload_bytes(), Some(base(&field)));
        assert_eq!(
            snapshot_payload_bytes(0, 0, 0, 0, std::iter::empty()),
            Some(std::mem::size_of::<FieldType>())
        );
    }

    #[test]
    fn snapshot_payload_counts_non_utf8_elements_verbatim() {
        let field = FieldType::new(FieldTypeCode::LongLong).with_elems([
            GoString::from_bytes(vec![0xff, 0x00, 0x80]),
            GoString::from("ab"),
        ]);
        assert_eq!(
            field.checked_snapshot_payload_bytes(),
            Some(base(&field) + 2 * std::mem::size_of::<GoString>() + 5)
        );
        field.elems.with_visible(|elements| {
            assert_eq!(elements[0].as_bytes(), &[0xff, 0x00, 0x80]);
            assert_eq!(elements[1].as_bytes(), b"ab");
        });
    }

    #[test]
    fn snapshot_payload_counts_independent_marker_lengths() {
        // Empty elems may retain markers; marker length can also be shorter or
        // longer than elems. Counting must never index markers by elem count.
        for (elements, markers) in [(0, 3), (3, 0), (3, 1), (1, 5), (3, 3)] {
            let mut field = FieldType::new(FieldTypeCode::LongLong);
            field.elems = GoSharedSlice::from_vec(vec![GoString::from("x"); elements]);
            field.elems_is_binary_literal = GoSharedSlice::from_vec(vec![true; markers]);
            assert_eq!(
                field.checked_snapshot_payload_bytes(),
                Some(
                    base(&field)
                        + elements * std::mem::size_of::<GoString>()
                        + elements
                        + markers * std::mem::size_of::<bool>()
                )
            );
            assert_eq!(field.elems_is_binary_literal.len(), markers);
        }
    }

    #[test]
    fn snapshot_payload_nil_and_allocated_empty_are_not_capacity() {
        let nil = FieldType::new(FieldTypeCode::LongLong);
        let mut empty = nil.clone();
        empty.elems = GoSharedSlice::from_vec_with_capacity(Vec::new(), 7);
        empty.elems_is_binary_literal = GoSharedSlice::from_vec_with_capacity(Vec::new(), 9);
        assert!(!nil.elems.is_allocated());
        assert!(!nil.elems_is_binary_literal.is_allocated());
        assert!(empty.elems.is_allocated());
        assert!(empty.elems_is_binary_literal.is_allocated());
        assert_eq!(nil.checked_snapshot_payload_bytes(), Some(base(&nil)));
        assert_eq!(empty.checked_snapshot_payload_bytes(), Some(base(&empty)));
        assert_eq!(empty.memory_usage() - nil.memory_usage(), 7 * 16 + 9);
    }

    #[test]
    fn snapshot_payload_leaves_legacy_counters_unchanged() {
        let mut field = FieldType::new(FieldTypeCode::LongLong);
        field.elems = GoSharedSlice::from_vec_with_capacity(vec![GoString::from("abc")], 8);
        field.elems_is_binary_literal =
            GoSharedSlice::from_vec_with_capacity(vec![true, false], 11);
        let legacy = 120
            + field.charset_name().len()
            + field.collation_name().len()
            + 8 * 16
            + 3
            + 11 * std::mem::size_of::<bool>();
        assert_eq!(field.memory_usage(), legacy);
        assert_eq!(field.storage_length(), 8);
        for _ in 0..3 {
            assert_eq!(
                field.checked_snapshot_payload_bytes(),
                Some(
                    base(&field)
                        + std::mem::size_of::<GoString>()
                        + 3
                        + 2 * std::mem::size_of::<bool>()
                )
            );
            assert_eq!(field.memory_usage(), legacy);
            assert_eq!(field.elems.capacity(), 8);
            assert_eq!(field.elems_is_binary_literal.capacity(), 11);
        }
        assert_eq!(
            FieldType::new(FieldTypeCode::Varchar).storage_length(),
            VAR_STORAGE_LEN
        );
    }

    #[test]
    fn snapshot_payload_borrows_shared_headers_without_mutation() {
        let mut field = FieldType::new(FieldTypeCode::LongLong).with_elems(["one"]);
        field.set_elem_with_binary_literal(0, "one", true);
        let alias = field.clone();
        let expected =
            base(&field) + std::mem::size_of::<GoString>() + 3 + std::mem::size_of::<bool>();
        for _ in 0..3 {
            assert_eq!(field.checked_snapshot_payload_bytes(), Some(expected));
            assert_eq!(alias.checked_snapshot_payload_bytes(), Some(expected));
            assert!(field.elems.backing_ptr_eq(&alias.elems));
            assert!(field
                .elems_is_binary_literal
                .backing_ptr_eq(&alias.elems_is_binary_literal));
            assert!(field.elem_is_binary_literal(0));
            field
                .elems
                .with_visible(|elements| assert_eq!(elements[0].as_bytes(), b"one"));
        }
    }

    #[test]
    fn snapshot_payload_is_observation_not_an_atomic_snapshot() {
        let field = FieldType::new(FieldTypeCode::LongLong).with_elems(["x"]);
        let before = field.checked_snapshot_payload_bytes().unwrap();
        let mut alias = field.clone();
        // Deliberately sequenced, not a claimed concurrent-mutation test. The
        // observer retains no snapshot or guard across a subsequent copy.
        alias.set_elem(0, "longer");
        assert_eq!(field.checked_snapshot_payload_bytes(), Some(before + 5));
        assert!(field.elems.backing_ptr_eq(&alias.elems));
    }

    #[test]
    fn snapshot_payload_checked_arithmetic_refuses_every_overflow() {
        let empty = || std::iter::empty();
        assert_eq!(snapshot_payload_bytes(usize::MAX, 0, 0, 0, empty()), None);
        assert_eq!(snapshot_payload_bytes(0, usize::MAX, 0, 0, empty()), None);
        assert_eq!(snapshot_payload_bytes(0, 0, usize::MAX, 0, empty()), None);
        assert_eq!(snapshot_payload_bytes(0, 0, 0, usize::MAX, empty()), None);
        assert_eq!(
            snapshot_payload_bytes(0, 0, 1, 0, [usize::MAX].into_iter()),
            None
        );
        let base = std::mem::size_of::<FieldType>() + 2 * std::mem::size_of::<GoString>();
        assert_eq!(
            snapshot_payload_bytes(0, 0, 2, 0, [usize::MAX - base, 0].into_iter()),
            Some(usize::MAX)
        );
        assert_eq!(
            snapshot_payload_bytes(0, 0, 2, 0, [usize::MAX - base, 1].into_iter()),
            None
        );
        assert_eq!(
            snapshot_payload_bytes(2, 3, 2, 4, [5, 7].into_iter()),
            Some(
                std::mem::size_of::<FieldType>()
                    + 2
                    + 3
                    + 2 * std::mem::size_of::<GoString>()
                    + 4 * std::mem::size_of::<bool>()
                    + 5
                    + 7
            )
        );
    }

    #[test]
    fn shared_field_storage_policy_keeps_fixed_decimal_variable_and_unknown_shapes() {
        for code in [
            FieldTypeCode::Tiny,
            FieldTypeCode::LongLong,
            FieldTypeCode::Date,
            FieldTypeCode::Timestamp,
            FieldTypeCode::Enum,
            FieldTypeCode::Bit,
        ] {
            assert_eq!(FieldType::parser(code).storage_length(), 8, "{code:?}");
        }
        assert_eq!(
            FieldType::parser(FieldTypeCode::NewDecimal)
                .with_flen(10)
                .with_decimal(2)
                .storage_length(),
            5
        );
        assert_eq!(
            FieldType::parser(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(10)
                .storage_length(),
            10
        );
        assert_eq!(
            FieldType::parser(FieldTypeCode::Varchar).storage_length(),
            VAR_STORAGE_LEN
        );
        assert_eq!(
            FieldType::parser(FieldTypeCode::Unknown(246))
                .with_flen(10)
                .with_decimal(2)
                .storage_length(),
            VAR_STORAGE_LEN
        );
        assert!(std::panic::catch_unwind(|| {
            FieldType::parser(FieldTypeCode::NewDecimal)
                .with_flen(0)
                .with_decimal(1)
                .storage_length()
        })
        .is_err());
    }
}
