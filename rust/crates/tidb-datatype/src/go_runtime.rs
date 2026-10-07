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

//! Dependency-leaf representations of Go runtime slice state.
//!
//! A Go slice copies its `(data,len,cap)` header while retaining mutable
//! backing-array identity. Metadata types in both `tidb-datatype` and
//! higher-level crates need that representation, so it lives here rather than
//! in a model-specific crate. Persisted receiver codecs remain with the owning
//! metadata type and use the explicitly documented decoder plumbing below.

use std::sync::{Arc, RwLock};

use tidb_query_datatype::codec::native_field_value::{
    native_go_64_next_slice_capacity, native_go_64_slice_decode_capacity,
    NativeGoSliceElementLayout,
};

/// Whether a Go slice's element type contains pointers and therefore uses a
/// scanned allocation. Above the malloc-header threshold, scanned and noscan
/// slices with the same element width can have different observable caps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoSliceElementLayout {
    /// The element type contains no pointers.
    NoPointers,
    /// The element type contains at least one pointer.
    PointerBearing,
}

const fn native_element_layout(layout: GoSliceElementLayout) -> NativeGoSliceElementLayout {
    match layout {
        GoSliceElementLayout::NoPointers => NativeGoSliceElementLayout::NoPointers,
        GoSliceElementLayout::PointerBearing => NativeGoSliceElementLayout::PointerBearing,
    }
}

/// Computes Go 1.25's next 64-bit slice capacity for a concrete element
/// width/layout. This is public so higher-level source-shaped codecs can use
/// the same allocator rule rather than duplicating it.
#[doc(hidden)]
pub fn go_64_next_slice_capacity_for_element(
    new_len: usize,
    old_capacity: usize,
    element_size: usize,
    layout: GoSliceElementLayout,
) -> usize {
    native_go_64_next_slice_capacity(
        new_len,
        old_capacity,
        element_size,
        native_element_layout(layout),
    )
}

/// Computes the capacity reached while Go's array decoder exposes elements
/// one at a time.
#[doc(hidden)]
pub fn go_64_slice_decode_capacity(
    capacity: usize,
    decoded_len: usize,
    element_size: usize,
    layout: GoSliceElementLayout,
) -> usize {
    native_go_64_slice_decode_capacity(
        capacity,
        decoded_len,
        element_size,
        native_element_layout(layout),
    )
}

/// A Go slice header with a shared mutable backing array.
///
/// Cloning copies only `(data,len,cap)`, so element replacement is visible
/// through sibling headers while changing one header's length is not.
pub struct GoSharedSlice<T> {
    backing: Option<Arc<RwLock<Vec<T>>>>,
    start: usize,
    len: usize,
    capacity: usize,
}

impl<T> GoSharedSlice<T> {
    /// Constructs an allocated slice, retaining only initialized capacity.
    #[must_use]
    pub fn from_vec(values: Vec<T>) -> Self {
        let len = values.len();
        Self {
            backing: Some(Arc::new(RwLock::new(values))),
            start: 0,
            len,
            // Rust Vec spare slots are uninitialized. A Go slice's capacity
            // is fully initialized and resliceable, so the ordinary
            // conversion exposes only the initialized range.
            capacity: len,
        }
    }

    /// Constructs a source slice with a larger, fully initialized capacity.
    #[must_use]
    pub fn from_vec_with_capacity(mut values: Vec<T>, capacity: usize) -> Self
    where
        T: Default,
    {
        assert!(capacity >= values.len(), "Go slice cap is smaller than len");
        let len = values.len();
        values.resize_with(capacity, T::default);
        Self {
            backing: Some(Arc::new(RwLock::new(values))),
            start: 0,
            len,
            capacity,
        }
    }

    /// Returns whether the Go slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.backing.is_some()
    }

    /// Returns the source slice length (nil and allocated-empty are both 0).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the source length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the source slice capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reports whether two headers point into the same backing array.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// Copies the visible elements into a fresh outer backing array.
    #[must_use]
    pub fn copy_outer(&self) -> Self
    where
        T: Clone,
    {
        Self::from_vec(self.snapshot())
    }

    /// Go `slices.Clone` for a source element size on the supported 64-bit
    /// runtime. Nil remains nil; allocated-empty gets a fresh allocated cap-0
    /// backing; non-empty inputs allocate through `append`/`growslice`.
    #[must_use]
    pub fn slices_clone(&self, element_size: usize, layout: GoSliceElementLayout) -> Self
    where
        T: Clone + Default,
    {
        if !self.is_allocated() {
            return Self::default();
        }
        let values = self.snapshot();
        if values.is_empty() {
            // Go 1.25 uses append(S{}, s...), not s[:0:0], specifically so a
            // zero-length Clone does not keep the source backing alive.
            return Self::from_vec(Vec::new());
        }
        let capacity = go_64_next_slice_capacity_for_element(values.len(), 0, element_size, layout);
        Self::from_vec_with_capacity(values, capacity)
    }

    /// Appends one element using Go 1.25's 64-bit growslice capacity rule.
    /// Writes within capacity stay visible through sibling headers; growth
    /// detaches only this header.
    pub fn push_go(&mut self, value: T, element_size: usize, layout: GoSliceElementLayout)
    where
        T: Clone + Default,
    {
        let index = self.len;
        let grown_capacity = if index < self.capacity {
            self.capacity
        } else {
            go_64_next_slice_capacity_for_element(index + 1, self.capacity, element_size, layout)
        };
        self.prepare_decode_slot(index, grown_capacity);
        self.set(index, value);
    }

    /// Go 1.25 `slices.Delete`: shift the retained tail in the same backing,
    /// clear the vacated visible slots to their zero value, and shorten only
    /// this header. Sibling headers therefore observe both the shift and clear
    /// while retaining their own length and capacity.
    pub fn delete_go(&mut self, start: usize, end: usize)
    where
        T: Clone + Default,
    {
        assert!(start <= end, "invalid Go slice delete range");
        assert!(end <= self.len, "slice bounds out of range in Go delete");
        if start == end {
            return;
        }

        let old_len = self.len;
        let removed = end - start;
        let new_len = old_len - removed;
        let mut values = self
            .backing
            .as_ref()
            .expect("delete from nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for destination in start..new_len {
            values[self.start + destination] = values[self.start + destination + removed].clone();
        }
        for cleared in new_len..old_len {
            values[self.start + cleared] = T::default();
        }
        self.len = new_len;
    }

    /// Clones the visible element values. Pointer handles contained in `T`
    /// retain their own pointee identity.
    #[must_use]
    pub fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        let Some(backing) = &self.backing else {
            return Vec::new();
        };
        let values = backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start..self.start + self.len].to_vec()
    }

    /// Maps the visible elements while holding one read guard. The callback
    /// must not re-enter this same backing.
    #[must_use]
    pub fn map_visible<U>(&self, mut map: impl FnMut(&T) -> U) -> Vec<U> {
        let Some(backing) = &self.backing else {
            return Vec::new();
        };
        let values = backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start..self.start + self.len]
            .iter()
            .map(&mut map)
            .collect()
    }

    /// Borrows the visible elements without allocating or cloning them.
    ///
    /// The callback runs while the backing read lock is held and therefore
    /// must not re-enter this same backing through another shallow header.
    /// Nil and allocated-empty slices both pass an empty slice; callers that
    /// need allocation identity inspect [`Self::is_allocated`] separately.
    pub fn with_visible<R>(&self, read: impl FnOnce(&[T]) -> R) -> R {
        let Some(backing) = &self.backing else {
            return read(&[]);
        };
        let values = backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        read(&values[self.start..self.start + self.len])
    }

    /// Mutates one visible element in place through every shared header. The
    /// callback must not re-enter this same backing.
    pub fn update(&self, index: usize, update: impl FnOnce(&mut T)) {
        assert!(index < self.len, "index out of Go slice bounds");
        let mut values = self
            .backing
            .as_ref()
            .expect("index of nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        update(&mut values[self.start + index]);
    }

    /// Clones one visible element.
    #[must_use]
    pub fn get(&self, index: usize) -> T
    where
        T: Clone,
    {
        assert!(index < self.len, "index out of Go slice bounds");
        let values = self
            .backing
            .as_ref()
            .expect("index of nil Go slice")
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index].clone()
    }

    /// Replaces one visible element in the shared backing array.
    pub fn set(&self, index: usize, value: T) {
        assert!(index < self.len, "index out of Go slice bounds");
        let mut values = self
            .backing
            .as_ref()
            .expect("index of nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index] = value;
    }

    /// Reslices this header to zero length without changing sibling headers
    /// or destroying the backing array.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Replaces this header as `encoding/json` replaces a decoded Go slice.
    ///
    /// This is public only for source-shaped receiver codecs in dependent
    /// crates; ordinary model APIs should use the semantic slice operations.
    #[doc(hidden)]
    pub fn replace_decoded(&mut self, values: Vec<T>, grown_capacity: usize)
    where
        T: Default,
    {
        if values.is_empty() {
            *self = Self::from_vec(Vec::new());
            return;
        }
        let new_len = values.len();
        if let Some(backing) = &self.backing {
            if new_len <= self.capacity {
                let mut destination = backing
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for (offset, value) in values.into_iter().enumerate() {
                    destination[self.start + offset] = value;
                }
                self.len = new_len;
                return;
            }
        }
        *self = Self::from_vec_with_capacity(values, grown_capacity);
    }

    /// Makes a sequential decoder slot visible, growing exactly when Go's
    /// decoder detaches the destination header from its old backing.
    #[doc(hidden)]
    pub fn prepare_decode_slot(&mut self, index: usize, grown_capacity: usize)
    where
        T: Clone + Default,
    {
        if index >= self.capacity {
            assert!(
                grown_capacity > self.capacity && grown_capacity > index,
                "Go slice decoder did not grow enough for its next slot"
            );
            let mut values = if let Some(backing) = &self.backing {
                let values = backing
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                values[self.start..self.start + self.len].to_vec()
            } else {
                Vec::new()
            };
            values.resize_with(grown_capacity, T::default);
            self.backing = Some(Arc::new(RwLock::new(values)));
            self.start = 0;
            self.capacity = grown_capacity;
        }
        if index >= self.len {
            self.len = index + 1;
        }
    }

    /// Clones an initialized slot anywhere through the header's capacity.
    #[doc(hidden)]
    pub fn decode_slot(&self, index: usize) -> T
    where
        T: Clone,
    {
        assert!(index < self.capacity, "decode slot exceeds Go slice cap");
        let values = self
            .backing
            .as_ref()
            .expect("decode slot of nil Go slice")
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index].clone()
    }

    /// Replaces a decoder-visible slot after `prepare_decode_slot` exposes it.
    #[doc(hidden)]
    pub fn set_decode_slot(&self, index: usize, value: T) {
        assert!(index < self.len, "decode slot is not visible");
        let mut values = self
            .backing
            .as_ref()
            .expect("decode slot of nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index] = value;
    }

    /// Applies Go's successful end-of-array truncation.
    #[doc(hidden)]
    pub fn finish_decode(&mut self, decoded_len: usize) {
        if decoded_len == 0 {
            *self = Self::from_vec(Vec::new());
        } else {
            assert!(decoded_len <= self.len, "decoder length was not exposed");
            self.len = decoded_len;
        }
    }
}

impl<T> Clone for GoSharedSlice<T> {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.as_ref().map(Arc::clone),
            start: self.start,
            len: self.len,
            capacity: self.capacity,
        }
    }
}

impl<T> From<Vec<T>> for GoSharedSlice<T> {
    fn from(values: Vec<T>) -> Self {
        Self::from_vec(values)
    }
}

impl<T> From<Option<Vec<T>>> for GoSharedSlice<T> {
    fn from(values: Option<Vec<T>>) -> Self {
        values.map_or_else(Self::default, Self::from_vec)
    }
}

impl<T> Default for GoSharedSlice<T> {
    fn default() -> Self {
        Self {
            backing: None,
            start: 0,
            len: 0,
            capacity: 0,
        }
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for GoSharedSlice<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_allocated() {
            formatter
                .debug_struct("GoSharedSlice")
                .field("values", &self.snapshot())
                .field("capacity", &self.capacity)
                .finish()
        } else {
            formatter.write_str("GoSharedSlice(nil)")
        }
    }
}

impl<T: Clone + serde::Serialize> serde::Serialize for GoSharedSlice<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(backing) = &self.backing else {
            return serializer.serialize_none();
        };
        let snapshot = {
            let values = backing
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            values[self.start..self.start + self.len].to_vec()
        };
        serde::Serialize::serialize(&snapshot, serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for GoSharedSlice<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Option<Vec<T>> as serde::Deserialize>::deserialize(deserializer)
            .map(|values| values.map_or_else(Self::default, Self::from_vec))
    }
}

#[cfg(test)]
#[test]
fn shared_go_slice_growth_wrappers_keep_noscan_scanned_and_decode_capacities() {
    assert_eq!(
        go_64_slice_decode_capacity(0, 9, 1, GoSliceElementLayout::NoPointers),
        16
    );
    assert_eq!(
        go_64_slice_decode_capacity(0, 5, 16, GoSliceElementLayout::PointerBearing),
        8
    );
    assert_eq!(
        go_64_next_slice_capacity_for_element(257, 256, 16, GoSliceElementLayout::NoPointers),
        512
    );
    assert_eq!(
        go_64_next_slice_capacity_for_element(257, 256, 16, GoSliceElementLayout::PointerBearing),
        591
    );
}
