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

use super::Column;
use crate::shared_bytes::SharedBytes;

#[test]
fn owned_read_view_borrows_payload_offsets_and_validity() {
    let mut column = Column::new_var_len(9);
    let values: [Option<&[u8]>; 9] = [
        Some(&[0, 0xff]),
        None,
        Some(b""),
        Some(b"abc"),
        None,
        Some("中文".as_bytes()),
        Some(b"x"),
        None,
        Some(b"last"),
    ];
    for value in values {
        match value {
            Some(value) => column.append_bytes(value),
            None => column.append_null(),
        }
    }
    assert!(!column.has_shared_mutable_storage());
    let view = column.read_view();
    assert_eq!(view.rows(), 9);
    assert_eq!(view.fixed_len(), None);
    assert_eq!(view.null_bitmap(), &[0b0110_1101, 1]);
    assert_eq!(view.offsets(), &[0, 2, 2, 2, 5, 5, 11, 12, 12, 16]);
    assert_eq!(view.data().as_ptr(), column.get_bytes(0).as_ptr());
    assert_eq!(view.null_bitmap().as_ptr(), column.null_bitmap.as_ptr());
    assert_eq!(view.offsets().as_ptr(), column.offsets.as_ptr());
    for (row, value) in values.iter().enumerate() {
        assert_eq!(
            view.null_bitmap()[row >> 3] & (1 << (row & 7)) != 0,
            value.is_some()
        );
        if let Some(value) = value {
            let start = view.offsets()[row] as usize;
            let end = view.offsets()[row + 1] as usize;
            assert_eq!(&view.data()[start..end], *value);
        }
    }
    drop(view);
    assert!(
        !column.has_shared_mutable_storage(),
        "reading must not promote storage"
    );
}

#[test]
fn fixed_read_view_exposes_native_endian_bytes_without_typed_casts() {
    let mut column = Column::new_fixed_len(8, 3);
    column.append_int64(-17);
    column.append_null();
    column.append_int64(i64::MAX);
    let view = column.read_view();
    assert_eq!(view.rows(), 3);
    assert_eq!(view.fixed_len(), Some(8));
    assert_eq!(view.null_bitmap(), &[0b101]);
    assert_eq!(view.data().len(), 24);
    assert!(view.offsets().is_empty());
    assert_eq!(view.data().as_ptr(), column.get_raw(0).as_ptr());
    assert_eq!(
        i64::from_ne_bytes(view.data()[0..8].try_into().unwrap()),
        -17
    );
    assert_eq!(
        i64::from_ne_bytes(view.data()[16..24].try_into().unwrap()),
        i64::MAX
    );
}

#[test]
fn frozen_unaligned_read_view_keeps_original_payload_pointer() {
    let mut payload = vec![0; 17];
    // Deliberately select an unaligned visible start, regardless of the byte
    // vector allocator's stronger-than-required incidental alignment.
    let start = usize::from((payload.as_ptr() as usize).is_multiple_of(8));
    payload[start..start + 8].copy_from_slice(&(-42_i64).to_ne_bytes());
    payload[start + 8..start + 16].copy_from_slice(&7_i64.to_ne_bytes());
    let payload = bytes::Bytes::from(payload);
    let mut column = Column::new_fixed_len(8, 0);
    column.length = 2;
    column.null_bitmap = vec![0b01];
    column.data = SharedBytes::from_bytes(payload.slice(start..start + 16));
    assert!(!column.has_shared_mutable_storage());
    let view = column.read_view();
    assert_eq!(view.data().as_ptr(), payload[start..].as_ptr());
    assert!(!(view.data().as_ptr() as usize).is_multiple_of(8));
    assert_eq!(view.rows(), 2);
    assert_eq!(view.fixed_len(), Some(8));
    assert_eq!(view.null_bitmap(), &[1]);
    assert_eq!(
        i64::from_ne_bytes(view.data()[..8].try_into().unwrap()),
        -42
    );
    assert_eq!(view.data().as_ptr(), column.get_raw(0).as_ptr());
}

#[test]
fn shared_read_view_keeps_guard_and_does_not_force_a_snapshot() {
    let mut column = Column::new_var_len(1);
    column.append_bytes(b"old\0\xff");
    let mut alias = column.data.share_range(0, 5);
    assert!(column.has_shared_mutable_storage());
    let pointer = alias.read().as_ptr();
    let view = column.read_view();
    assert_eq!(view.data().as_ptr(), pointer);
    assert_eq!(view.data(), b"old\0\xff");
    assert_eq!(view.offsets().as_ptr(), column.offsets.as_ptr());
    assert_eq!(view.null_bitmap().as_ptr(), column.null_bitmap.as_ptr());
    // A distinct mutable header cannot invalidate a live view: the existing
    // shared-bytes policy detaches that mutator on read-lock contention.
    alias.copy_from_slice(0..3, b"new");
    assert_eq!(view.data().as_ptr(), pointer);
    assert_eq!(view.data(), b"old\0\xff");
    assert_eq!(alias.read().as_ref(), b"new\0\xff");
    drop(view);
    assert_eq!(column.get_bytes(0).as_ref(), b"old\0\xff");
    assert!(column.has_shared_mutable_storage());
    assert!(!column.data.backing_ptr_eq(&alias));
}

#[test]
fn read_view_preserves_nonzero_offset_base_and_empty_layouts() {
    let mut column = Column::new_var_len(0);
    column.length = 2;
    column.null_bitmap = vec![0b11];
    column.offsets = vec![6, 8, 10];
    column.data = SharedBytes::from_vec(b"prefixaabb".to_vec());
    let view = column.read_view();
    assert_eq!(view.data(), b"prefixaabb");
    assert_eq!(view.offsets(), &[6, 8, 10]);
    assert_eq!(view.offsets().as_ptr(), column.offsets.as_ptr());
    assert_eq!(column.get_bytes(0).as_ref(), b"aa");
    assert_eq!(column.get_bytes(1).as_ref(), b"bb");
    drop(view);

    for (column, width, offsets) in [
        (Column::default(), None, Vec::new()),
        (Column::new_var_len(0), None, vec![0]),
        (Column::new_fixed_len(8, 0), Some(8), Vec::new()),
        (Column::new_fixed_len(0, 0), Some(0), Vec::new()),
    ] {
        let view = column.read_view();
        assert_eq!(view.rows(), 0);
        assert!(view.data().is_empty());
        assert!(view.null_bitmap().is_empty());
        assert_eq!(view.offsets(), offsets);
        assert_eq!(view.fixed_len(), width);
    }
}
