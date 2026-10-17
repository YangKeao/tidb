// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native values are owners, never expressions or evaluators. The private
//! frame ABI carries checked handles, not serialized/normalized SQL values.
//! Charges cover record capacity and a conservative logical payload allowance;
//! external input/source owners, SDK transients and allocator peaks are outside
//! this observer. It is not a statement memory tracker.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(1);
const MAGIC: u64 = 0x31564d5544414e; // private native-Datum value ABI version 1
const HANDLE_BYTES: usize = 5 * size_of::<u64>();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NativeValueRole {
    CharInput = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NativeValueOrigin {
    pub node: usize,
    pub row: InputRow,
}
struct Entry {
    value: Datum,
    role: NativeValueRole,
    origin: NativeValueOrigin,
    charge: usize,
}

/// Allocation-free actual storage inspection; no SQL stringification, type
/// conversion, UTF-8 validation or canonicalization occurs here.
pub(super) fn native_payload_bytes(value: &Datum) -> LocalResult<usize> {
    match value {
        Datum::Decimal(value) => Ok(value.coefficient_bytes().len()),
        Datum::Json(value) => value
            .value()
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("native JSON payload size overflow")),
        Datum::VectorFloat32(value) => value
            .len()
            .checked_mul(size_of::<f32>())
            .ok_or_else(|| resource("native vector payload size overflow")),
        _ => Ok(value.go_bytes().len()),
    }
}
fn payload_charge(value: &Datum) -> LocalResult<usize> {
    // The allowance is deliberately logical/conservative; shared immutable
    // Go-string backing remains externally owned, and SmallVec may be inline.
    native_payload_bytes(value)?
        .checked_mul(2)
        .ok_or_else(|| resource("native value payload allowance overflow"))
}
fn allocate_namespace(counter: &AtomicU64) -> LocalResult<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| resource("native value namespace exhausted"))
}
fn invalid_handle() -> LocalError {
    binding("native value handle has invalid shape or provenance")
}

pub(super) struct NativeValueStore {
    namespace: u64,
    generation: u64,
    entries: Vec<Entry>,
    payload_charge: usize,
    max_logical_bytes: usize,
    poisoned: bool,
}
impl NativeValueStore {
    pub(super) fn new(max_logical_bytes: usize) -> LocalResult<Self> {
        Ok(Self {
            namespace: allocate_namespace(&NEXT_NAMESPACE)?,
            generation: 0,
            entries: Vec::new(),
            payload_charge: 0,
            max_logical_bytes,
            poisoned: false,
        })
    }
    fn logical_bytes(&self) -> LocalResult<usize> {
        array_bytes::<Entry>(self.entries.capacity())?
            .checked_add(self.payload_charge)
            .ok_or_else(|| resource("native value store size overflow"))
    }
    fn check(&self, bytes: usize) -> LocalResult<()> {
        if self.poisoned || bytes > self.max_logical_bytes {
            Err(resource("native value store policy exceeded"))
        } else {
            Ok(())
        }
    }
    /// Only call after every previous frame/task and output consumer finishes.
    pub(super) fn begin_window(&mut self) -> LocalResult<()> {
        self.check(self.logical_bytes()?)?;
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| resource("native value generation exhausted"))?;
        self.entries.clear();
        self.payload_charge = 0;
        self.generation = generation;
        Ok(())
    }
    pub(super) fn insert_clone(
        &mut self,
        value: &Datum,
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<Vec<u8>> {
        if self.generation == 0 {
            return Err(invalid_handle());
        }
        let charge = payload_charge(value)?;
        let growth = usize::from(self.entries.len() == self.entries.capacity());
        self.check(
            self.logical_bytes()?
                .checked_add(array_bytes::<Entry>(growth)?)
                .and_then(|bytes| bytes.checked_add(charge))
                .and_then(|bytes| bytes.checked_add(HANDLE_BYTES))
                .ok_or_else(|| resource("native value insertion size overflow"))?,
        )?;
        let index = u64::try_from(self.entries.len())
            .map_err(|_| resource("native value index exceeds ABI width"))?;
        self.entries
            .try_reserve_exact(1)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        let accepted = self
            .logical_bytes()?
            .checked_add(charge)
            .and_then(|bytes| bytes.checked_add(HANDLE_BYTES))
            .ok_or_else(|| resource("native value reserved size overflow"))?;
        if let Err(error) = self.check(accepted) {
            self.poisoned = true;
            return Err(error);
        }
        let mut handle = Vec::new();
        handle
            .try_reserve_exact(HANDLE_BYTES)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        for word in [MAGIC, self.namespace, self.generation, index, role as u64] {
            handle.extend_from_slice(&word.to_le_bytes());
        }
        self.check(
            self.logical_bytes()?
                .checked_add(charge)
                .and_then(|bytes| bytes.checked_add(handle.capacity()))
                .ok_or_else(|| resource("native value handle capacity overflow"))?,
        )?;
        let owned = value.clone();
        self.payload_charge = self
            .payload_charge
            .checked_add(charge)
            .ok_or_else(|| resource("native value payload total overflow"))?;
        self.entries.push(Entry {
            value: owned,
            role,
            origin,
            charge,
        });
        Ok(handle)
    }
    pub(super) fn get(
        &self,
        handle: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<&Datum> {
        Ok(&self.entry(handle, role, origin)?.value)
    }
    fn entry(
        &self,
        handle: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<&Entry> {
        self.check(self.logical_bytes()?)?;
        if handle.len() != HANDLE_BYTES {
            return Err(invalid_handle());
        }
        let word = |index: usize| {
            u64::from_le_bytes(
                handle[index * 8..(index + 1) * 8]
                    .try_into()
                    .expect("fixed ABI word"),
            )
        };
        if word(0) != MAGIC
            || word(1) != self.namespace
            || word(2) != self.generation
            || self.generation == 0
            || word(4) != role as u64
        {
            return Err(invalid_handle());
        }
        let entry = self
            .entries
            .get(usize::try_from(word(3)).map_err(|_| invalid_handle())?)
            .ok_or_else(invalid_handle)?;
        if entry.role != role || entry.origin != origin {
            return Err(invalid_handle());
        }
        Ok(entry)
    }
    /// The borrow ends before the original native context receives any call.
    pub(super) fn clone_for_kernel(
        &self,
        handle: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<Datum> {
        let entry = self.entry(handle, role, origin)?;
        self.check(
            self.logical_bytes()?
                .checked_add(entry.charge)
                .ok_or_else(|| resource("native value kernel copy size overflow"))?,
        )?;
        Ok(entry.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{BinaryLiteral, Collation, Decimal, MysqlEnum, MysqlSet, TimeType};
    fn origin() -> NativeValueOrigin {
        NativeValueOrigin {
            node: 3,
            row: InputRow {
                occurrence: 1,
                input_row: 7,
            },
        }
    }
    #[test]
    fn store_preserves_all_nineteen_native_kinds_and_raw_representations() {
        let mut time = tidb_datatype::parse_datetime(
            "2026-10-17 12:34:56.123456",
            &chrono_tz::UTC,
            true,
            false,
        )
        .unwrap()
        .time;
        time.set_kind(TimeType::Timestamp);
        let mut vector = VectorFloat32::must_create(vec![1.0, -0.0]);
        vector.elements_mut()[0] = f32::from_bits(0x7fa00001);
        let fixtures = [
            Datum::Null,
            Datum::MinNotNull,
            Datum::MaxValue,
            Datum::Int(i64::MIN),
            Datum::UInt(u64::MAX),
            Datum::Decimal(
                Decimal::from_raw_parts(true, vec![0xff, b'0', 0], 2, 8)
                    .with_declared_shape(228, -1),
            ),
            Datum::Real(f64::from_bits(0x7ff8000000000042)),
            Datum::Float32(f64::from_bits(0xfff8000000000033)),
            Datum::new_collation_string(vec![0xff, 0], Collation::GbkBin),
            Datum::Bytes(vec![0xff, 0]),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0, 0xff])),
            Datum::Duration(MySqlDuration::from_nanoseconds(-123456789012, 4).unwrap()),
            Datum::Enum(MysqlEnum::new(vec![0xff, 0], 7), Collation::GbkBin),
            Datum::Bit(BinaryLiteral::from(vec![0, 0xff])),
            Datum::Set(MysqlSet::new(vec![0xff, 0], 9), Collation::GbkBin),
            Datum::Time(time),
            Datum::Json(BinaryJSON::from_encoded_parts(0xff, [0x8c, 0, 0xff])),
            Datum::Raw(vec![0xff, 0]),
            Datum::VectorFloat32(vector),
        ];
        let mut store = NativeValueStore::new(1024 * 1024).unwrap();
        store.begin_window().unwrap();
        for expected in &fixtures {
            let handle = store
                .insert_clone(expected, NativeValueRole::CharInput, origin())
                .unwrap();
            let actual = store
                .clone_for_kernel(&handle, NativeValueRole::CharInput, origin())
                .unwrap();
            assert_eq!(actual.kind(), expected.kind());
            match (&actual, expected) {
                (Datum::Real(a), Datum::Real(b)) | (Datum::Float32(a), Datum::Float32(b)) => {
                    assert_eq!(a.to_bits(), b.to_bits())
                }
                (Datum::Decimal(a), Datum::Decimal(b)) => {
                    assert_eq!(a.coefficient_bytes(), b.coefficient_bytes());
                    assert_eq!(
                        (
                            a.scale(),
                            a.storage_scale(),
                            a.is_negative(),
                            a.declared_shape()
                        ),
                        (
                            b.scale(),
                            b.storage_scale(),
                            b.is_negative(),
                            b.declared_shape()
                        )
                    );
                }
                (Datum::VectorFloat32(a), Datum::VectorFloat32(b)) => assert_eq!(
                    a.elements().iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                    b.elements().iter().map(|x| x.to_bits()).collect::<Vec<_>>()
                ),
                _ => assert_eq!(&actual, expected),
            }
        }
    }
    #[test]
    fn handles_reject_stale_cross_store_role_source_and_occurrence_drift() {
        let mut store = NativeValueStore::new(4096).unwrap();
        store.begin_window().unwrap();
        let token = store
            .insert_clone(&Datum::Int(7), NativeValueRole::CharInput, origin())
            .unwrap();
        let mut other = NativeValueStore::new(4096).unwrap();
        other.begin_window().unwrap();
        other
            .insert_clone(&Datum::Int(8), NativeValueRole::CharInput, origin())
            .unwrap();
        assert!(other
            .get(&token, NativeValueRole::CharInput, origin())
            .is_err());
        for len in 0..HANDLE_BYTES {
            assert!(store
                .get(&token[..len], NativeValueRole::CharInput, origin())
                .is_err());
        }
        let mut extended = token.clone();
        extended.push(0);
        assert!(store
            .get(&extended, NativeValueRole::CharInput, origin())
            .is_err());
        let mut bytes_origin = origin();
        bytes_origin.node += 2;
        let bytes_value = Datum::Bytes(token.clone());
        let bytes_token = store
            .insert_clone(&bytes_value, NativeValueRole::CharInput, bytes_origin)
            .unwrap();
        assert_eq!(
            store
                .get(&bytes_token, NativeValueRole::CharInput, bytes_origin)
                .unwrap(),
            &bytes_value
        );
        for offset in [0, 8, 16, 24, 32] {
            let mut forged = token.clone();
            forged[offset] ^= 1;
            assert!(store
                .get(&forged, NativeValueRole::CharInput, origin())
                .is_err());
        }
        let mut wrong = origin();
        wrong.node += 1;
        assert!(store
            .get(&token, NativeValueRole::CharInput, wrong)
            .is_err());
        let mut wrong = origin();
        wrong.row.occurrence += 1;
        assert!(store
            .get(&token, NativeValueRole::CharInput, wrong)
            .is_err());
        let mut wrong = origin();
        wrong.row.input_row += 1;
        assert!(store
            .get(&token, NativeValueRole::CharInput, wrong)
            .is_err());
        store.begin_window().unwrap();
        let fresh = store
            .insert_clone(&Datum::Int(7), NativeValueRole::CharInput, origin())
            .unwrap();
        assert_ne!(token, fresh);
        assert!(store
            .get(&token, NativeValueRole::CharInput, origin())
            .is_err());
        assert_eq!(
            store
                .get(&fresh, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Int(7)
        );
    }
    #[test]
    fn resource_and_identity_exhaustion_never_publish_a_new_handle() {
        let mut store = NativeValueStore::new(512).unwrap();
        store.begin_window().unwrap();
        let token = store
            .insert_clone(&Datum::Int(7), NativeValueRole::CharInput, origin())
            .unwrap();
        let entries = store.entries.len();
        assert!(matches!(
            store.insert_clone(
                &Datum::Raw(vec![0; 1000]),
                NativeValueRole::CharInput,
                origin()
            ),
            Err(LocalError::ResourceLimit(_))
        ));
        assert_eq!(store.entries.len(), entries);
        assert_eq!(
            store
                .get(&token, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Int(7)
        );
        store.generation = u64::MAX;
        assert!(matches!(
            store.begin_window(),
            Err(LocalError::ResourceLimit(_))
        ));
        assert!(matches!(
            allocate_namespace(&AtomicU64::new(u64::MAX)),
            Err(LocalError::ResourceLimit(_))
        ));
    }
}
