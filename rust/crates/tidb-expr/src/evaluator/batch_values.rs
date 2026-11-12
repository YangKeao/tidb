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
//! Handles are inline identities. Value ownership and generations are checked;
//! the former Demo payload/lease accounting and byte caps are removed.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(1);
const MAGIC: u64 = 0x32564d5544414e; // private native-Datum value ABI version 2
pub(super) const HANDLE_BYTES: usize = 5 * size_of::<u64>();
pub(super) type NativeValueHandle = [u8; HANDLE_BYTES];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NativeValueRole {
    CharInput = 1,
    Output = 2,
    DecimalFast = 3,
    FunctionOutput = 4,
}
impl NativeValueRole {
    pub(super) fn sql_value(self) -> bool {
        matches!(self, Self::CharInput | Self::Output | Self::FunctionOutput)
    }
    pub(super) fn expression_view(self) -> bool {
        self != Self::FunctionOutput
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NativeValueOrigin {
    pub node: usize,
    pub row: InputRow,
}
enum StoredValue {
    // An actual SQL owner is on the kernel stack, never a SQL NULL.
    Leased,
    Sql(Datum),
    DiagnosticUnavailable,
    DiagnosticText(Box<str>),
    DecimalFast(tidb_query_expr::NativeDecimalFastOutcome),
}
struct Entry {
    value: Option<StoredValue>,
    version: u32,
    role: NativeValueRole,
    origin: NativeValueOrigin,
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
    free: Vec<usize>,
    poisoned: bool,
}
impl NativeValueStore {
    /// A finished fragment releases its temporary owners before the next
    /// sibling demand. No allocation or namespace reservation occurs during
    /// cleanup. Generation exhaustion makes subsequent use refuse safely.
    pub(super) fn release_window(&mut self) {
        self.entries.clear();
        self.free.clear();
        match self.generation.checked_add(1) {
            Some(next) => self.generation = next,
            None => self.poisoned = true,
        }
    }

    pub(super) fn new() -> LocalResult<Self> {
        Ok(Self {
            namespace: allocate_namespace(&NEXT_NAMESPACE)?,
            generation: 0,
            entries: Vec::new(),
            free: Vec::new(),
            poisoned: false,
        })
    }

    /// Reject a poisoned store before materializing new native input.
    pub(super) fn check_live(&self) -> LocalResult<()> {
        if self.poisoned {
            Err(invalid_handle())
        } else {
            Ok(())
        }
    }
    /// Only call after every previous frame/task and output consumer finishes.
    pub(super) fn begin_window(&mut self) -> LocalResult<()> {
        if self.poisoned {
            return Err(invalid_handle());
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| resource("native value generation exhausted"))?;
        self.entries.clear();
        self.free.clear();
        Ok(())
    }
    pub(super) fn insert_clone(
        &mut self,
        value: &Datum,
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<NativeValueHandle> {
        if self.generation == 0 || role == NativeValueRole::DecimalFast {
            return Err(invalid_handle());
        }
        self.insert_with(role, origin, || StoredValue::Sql(value.clone()))
    }
    pub(super) fn insert_owned(
        &mut self,
        value: Datum,
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<NativeValueHandle> {
        if self.generation == 0 || role == NativeValueRole::DecimalFast {
            return Err(invalid_handle());
        }
        self.insert_with(role, origin, || StoredValue::Sql(value))
    }
    /// Fast representation refusal is a typed, non-SQL outcome. It cannot
    /// enter a SQL kernel or output consumer through the ordinary value API.
    pub(super) fn insert_decimal_fast(
        &mut self,
        value: tidb_query_expr::NativeDecimalFastOutcome,
        origin: NativeValueOrigin,
    ) -> LocalResult<NativeValueHandle> {
        self.insert_with(NativeValueRole::DecimalFast, origin, || {
            StoredValue::DecimalFast(value)
        })
    }
    pub(super) fn decimal_fast(
        &self,
        handle: &[u8],
        origin: NativeValueOrigin,
    ) -> LocalResult<&tidb_query_expr::NativeDecimalFastOutcome> {
        match self
            .entry(handle, NativeValueRole::DecimalFast, origin)?
            .value
            .as_ref()
            .expect("authenticated live value")
        {
            StoredValue::DecimalFast(value) => Ok(value),
            StoredValue::Leased
            | StoredValue::Sql(_)
            | StoredValue::DiagnosticUnavailable
            | StoredValue::DiagnosticText(_) => Err(invalid_handle()),
        }
    }
    /// A real suppressed diagnostic failure, distinct from every SQL Datum.
    /// It may only travel inside an explicitly annotated diagnostic subtree.
    pub(super) fn insert_diagnostic_unavailable(
        &mut self,
        origin: NativeValueOrigin,
    ) -> LocalResult<NativeValueHandle> {
        self.insert_with(NativeValueRole::CharInput, origin, || {
            StoredValue::DiagnosticUnavailable
        })
    }
    pub(super) fn diagnostic_unavailable(
        &self,
        token: &[u8],
        origin: NativeValueOrigin,
    ) -> LocalResult<bool> {
        match self
            .entry(token, NativeValueRole::CharInput, origin)?
            .value
            .as_ref()
            .expect("authenticated live value")
        {
            StoredValue::DiagnosticUnavailable => Ok(true),
            StoredValue::Sql(_) => Ok(false),
            StoredValue::Leased | StoredValue::DecimalFast(_) | StoredValue::DiagnosticText(_) => {
                Err(invalid_handle())
            }
        }
    }
    pub(super) fn insert_diagnostic_text(
        &mut self,
        text: String,
        origin: NativeValueOrigin,
    ) -> LocalResult<NativeValueHandle> {
        self.insert_with(NativeValueRole::CharInput, origin, || {
            StoredValue::DiagnosticText(text.into_boxed_str())
        })
    }
    pub(super) fn diagnostic_text(
        &self,
        token: &[u8],
        origin: NativeValueOrigin,
    ) -> LocalResult<Option<&str>> {
        match self
            .entry(token, NativeValueRole::CharInput, origin)?
            .value
            .as_ref()
            .expect("authenticated live value")
        {
            StoredValue::DiagnosticUnavailable => Ok(None),
            StoredValue::DiagnosticText(text) => Ok(Some(text)),
            _ => Err(invalid_handle()),
        }
    }
    fn insert_with(
        &mut self,
        role: NativeValueRole,
        origin: NativeValueOrigin,
        make_value: impl FnOnce() -> StoredValue,
    ) -> LocalResult<NativeValueHandle> {
        if self.poisoned || self.generation == 0 {
            return Err(invalid_handle());
        }
        let reuse = self.free.last().copied();
        let slot = reuse.unwrap_or(self.entries.len());
        let slot_word =
            u32::try_from(slot).map_err(|_| resource("native value slot space exhausted"))?;
        let version = if let Some(slot) = reuse {
            self.entries[slot]
                .version
                .checked_add(1)
                .ok_or_else(|| resource("native value slot version exhausted"))?
        } else {
            1
        };
        if reuse.is_none() {
            self.entries
                .try_reserve(1)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        }
        let entry = Entry {
            value: Some(make_value()),
            version,
            role,
            origin,
        };
        if let Some(slot) = reuse {
            self.free.pop();
            self.entries[slot] = entry;
        } else {
            self.entries.push(entry);
        }
        let mut handle = [0; HANDLE_BYTES];
        let index = (u64::from(version) << 32) | u64::from(slot_word);
        for (word, bytes) in [MAGIC, self.namespace, self.generation, index, role as u64]
            .into_iter()
            .zip(handle.chunks_exact_mut(8))
        {
            bytes.copy_from_slice(&word.to_le_bytes());
        }
        Ok(handle)
    }
    pub(super) fn get(
        &self,
        handle: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<&Datum> {
        match self
            .entry(handle, role, origin)?
            .value
            .as_ref()
            .expect("authenticated live value")
        {
            StoredValue::Sql(value) => Ok(value),
            StoredValue::Leased
            | StoredValue::DecimalFast(_)
            | StoredValue::DiagnosticUnavailable
            | StoredValue::DiagnosticText(_) => Err(invalid_handle()),
        }
    }
    /// Metadata authentication also works while the genuine SQL value is owned
    /// by a live lease. Ordinary SQL readers always reject that private marker.
    pub(super) fn authenticate(
        &self,
        token: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<()> {
        self.entry(token, role, origin).map(|_| ())
    }
    /// Transfer ownership without cloning the payload or retaining a RefCell
    /// borrow across the original context's callbacks. The lease restores the
    /// value only if the exact generation and slot are still live.
    pub(super) fn lease_for_kernel<'a>(
        arena: &'a RefCell<Self>,
        token: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<NativeValueLease<'a>> {
        let mut store = arena.borrow_mut();
        let entry = store.entry(token, role, origin)?;
        if !matches!(entry.value, Some(StoredValue::Sql(_))) {
            return Err(invalid_handle());
        }
        let version = entry.version;
        let slot = (u64::from_le_bytes(token[24..32].try_into().expect("authenticated ABI"))
            & u64::from(u32::MAX)) as usize;
        let Some(StoredValue::Sql(value)) = store.entries[slot].value.replace(StoredValue::Leased)
        else {
            unreachable!("authenticated SQL owner")
        };
        Ok(NativeValueLease {
            arena,
            value: Some(value),
            namespace: store.namespace,
            generation: store.generation,
            slot,
            version,
            role,
            origin,
        })
    }
    fn entry(
        &self,
        handle: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<&Entry> {
        if self.poisoned {
            return Err(invalid_handle());
        }
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
            .get(usize::try_from(word(3) & u64::from(u32::MAX)).map_err(|_| invalid_handle())?)
            .ok_or_else(invalid_handle)?;
        if entry.role != role
            || entry.origin != origin
            || entry.value.is_none()
            || u64::from(entry.version) != word(3) >> 32
        {
            return Err(invalid_handle());
        }
        Ok(entry)
    }
    /// Consume a completed Fresh subtree after its last host use. Ancestors,
    /// other occurrences and other producers remain live. Reused slots mint a
    /// new version, so none of the consumed capabilities can name a new value.
    pub(super) fn discard_subtree(
        &mut self,
        row: InputRow,
        nodes: std::ops::Range<usize>,
    ) -> LocalResult<()> {
        for index in 0..self.entries.len() {
            let entry = &mut self.entries[index];
            if entry.value.is_none()
                || entry.origin.row != row
                || !nodes.contains(&entry.origin.node)
            {
                continue;
            }
            self.free
                .try_reserve(1)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            entry.value.take();
            self.free.push(index);
        }
        Ok(())
    }
    /// The borrow ends before the original native context receives any call.
    pub(super) fn clone_for_kernel(
        &self,
        handle: &[u8],
        role: NativeValueRole,
        origin: NativeValueOrigin,
    ) -> LocalResult<Datum> {
        self.get(handle, role, origin).cloned()
    }
}

/// A stack owner, not an expression, evaluator or callback. Active payloads
/// remain in the same arena's accounting even if a callback resets its window.
pub(super) struct NativeValueLease<'a> {
    arena: &'a RefCell<NativeValueStore>,
    value: Option<Datum>,
    namespace: u64,
    generation: u64,
    slot: usize,
    version: u32,
    role: NativeValueRole,
    origin: NativeValueOrigin,
}
impl NativeValueLease<'_> {
    pub(super) fn datum(&self) -> &Datum {
        self.value.as_ref().expect("live owned SQL lease")
    }
}
impl Drop for NativeValueLease<'_> {
    fn drop(&mut self) {
        // A callback may reset the arena. Restore only the same live identity;
        // never retain a RefCell borrow or panic while unwinding.
        if let Ok(mut store) = self.arena.try_borrow_mut() {
            if store.namespace == self.namespace && store.generation == self.generation {
                if let Some(entry) = store.entries.get_mut(self.slot) {
                    if entry.version == self.version
                        && entry.role == self.role
                        && entry.origin == self.origin
                        && matches!(entry.value, Some(StoredValue::Leased))
                    {
                        entry.value = self.value.take().map(StoredValue::Sql);
                    }
                }
            }
        }
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
    fn diagnostic_failures_are_authenticated_completions_and_never_sql_null() {
        let mut store = NativeValueStore::new().unwrap();
        assert!(store.insert_diagnostic_unavailable(origin()).is_err());
        store.begin_window().unwrap();
        let failed = store.insert_diagnostic_unavailable(origin()).unwrap();
        assert!(store.diagnostic_unavailable(&failed, origin()).unwrap());
        for role in [
            NativeValueRole::CharInput,
            NativeValueRole::Output,
            NativeValueRole::DecimalFast,
        ] {
            assert!(store.get(&failed, role, origin()).is_err());
            assert!(store.clone_for_kernel(&failed, role, origin()).is_err());
            let mut forged = failed.clone();
            forged[32..40].copy_from_slice(&(role as u64).to_le_bytes());
            assert!(store.get(&forged, role, origin()).is_err());
        }
        let sql = store
            .insert_clone(&Datum::Null, NativeValueRole::CharInput, origin())
            .unwrap();
        assert!(!store.diagnostic_unavailable(&sql, origin()).unwrap());
        assert_eq!(
            store
                .get(&sql, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Null
        );
        let mut wrong = origin();
        wrong.node += 1;
        assert!(store.diagnostic_unavailable(&failed, wrong).is_err());
        wrong = origin();
        wrong.row.occurrence += 1;
        assert!(store.diagnostic_unavailable(&failed, wrong).is_err());
        wrong = origin();
        wrong.row.input_row += 1;
        assert!(store.diagnostic_unavailable(&failed, wrong).is_err());
        let mut foreign = NativeValueStore::new().unwrap();
        foreign.begin_window().unwrap();
        assert!(foreign.diagnostic_unavailable(&failed, origin()).is_err());
        store.begin_window().unwrap();
        assert!(store.diagnostic_unavailable(&failed, origin()).is_err());
        let token = store.insert_diagnostic_unavailable(origin()).unwrap();
        assert!(store.diagnostic_unavailable(&token, origin()).unwrap());
    }
    #[test]
    fn decimal_fast_outcomes_are_distinct_from_sql_values_and_keep_provenance() {
        use tidb_query_expr::{NativeDecimalFastOutcome as Outcome, NativeDecimalFastValue};
        let mut store = NativeValueStore::new().unwrap();
        store.begin_window().unwrap();
        let outcomes = [
            Outcome::Unsupported,
            Outcome::Value(None),
            Outcome::Value(Some(NativeDecimalFastValue {
                coefficient: -125,
                storage_scale: 2,
                scale: 2,
            })),
        ];
        for expected in outcomes {
            let token = store
                .insert_decimal_fast(expected.clone(), origin())
                .unwrap();
            assert_eq!(store.decimal_fast(&token, origin()).unwrap(), &expected);
            assert!(store
                .get(&token, NativeValueRole::DecimalFast, origin())
                .is_err());
            assert!(store
                .clone_for_kernel(&token, NativeValueRole::DecimalFast, origin())
                .is_err());
            for role in [NativeValueRole::CharInput, NativeValueRole::Output] {
                assert!(store.get(&token, role, origin()).is_err());
                let mut forged = token.clone();
                forged[32..40].copy_from_slice(&(role as u64).to_le_bytes());
                assert!(store.get(&forged, role, origin()).is_err());
            }
            let mut wrong = origin();
            wrong.row.occurrence += 1;
            assert!(store.decimal_fast(&token, wrong).is_err());
            let mut foreign = NativeValueStore::new().unwrap();
            foreign.begin_window().unwrap();
            assert!(foreign.decimal_fast(&token, origin()).is_err());
            store.begin_window().unwrap();
            assert!(store.decimal_fast(&token, origin()).is_err());
        }
        assert!(store
            .insert_clone(&Datum::Null, NativeValueRole::DecimalFast, origin())
            .is_err());
        let sql = store
            .insert_clone(&Datum::Null, NativeValueRole::Output, origin())
            .unwrap();
        assert!(store.decimal_fast(&sql, origin()).is_err());
        assert_eq!(
            store.get(&sql, NativeValueRole::Output, origin()).unwrap(),
            &Datum::Null
        );
    }
    #[test]
    fn decimal_fast_outcome_insertion_checks_generation_before_ownership() {
        use tidb_query_expr::NativeDecimalFastOutcome as Outcome;
        let mut store = NativeValueStore::new().unwrap();
        assert!(store
            .insert_decimal_fast(Outcome::Unsupported, origin())
            .is_err());
        store.begin_window().unwrap();
        let token = store
            .insert_decimal_fast(Outcome::Value(None), origin())
            .unwrap();
        assert_eq!(
            store.decimal_fast(&token, origin()).unwrap(),
            &Outcome::Value(None)
        );
        let unsupported = store
            .insert_decimal_fast(Outcome::Unsupported, origin())
            .unwrap();
        assert_eq!(
            store.decimal_fast(&unsupported, origin()).unwrap(),
            &Outcome::Unsupported
        );
        assert!(store
            .get(&unsupported, NativeValueRole::DecimalFast, origin())
            .is_err());
        assert_eq!(
            store.decimal_fast(&token, origin()).unwrap(),
            &Outcome::Value(None)
        );
        assert_eq!(store.entries.len(), 2);
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
        let mut store = NativeValueStore::new().unwrap();
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
        let mut store = NativeValueStore::new().unwrap();
        store.begin_window().unwrap();
        let token = store
            .insert_clone(&Datum::Int(7), NativeValueRole::CharInput, origin())
            .unwrap();
        let mut other = NativeValueStore::new().unwrap();
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
        let mut extended = token.to_vec();
        extended.push(0);
        assert!(store
            .get(&extended, NativeValueRole::CharInput, origin())
            .is_err());
        let mut bytes_origin = origin();
        bytes_origin.node += 2;
        let bytes_value = Datum::Bytes(token.to_vec());
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
        let mut store = NativeValueStore::new().unwrap();
        store.begin_window().unwrap();
        let token = store
            .insert_clone(&Datum::Int(7), NativeValueRole::CharInput, origin())
            .unwrap();
        let entries = store.entries.len();
        let raw = store
            .insert_clone(
                &Datum::Raw(vec![0; 1000]),
                NativeValueRole::CharInput,
                origin(),
            )
            .unwrap();
        assert_eq!(
            store
                .get(&raw, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Raw(vec![0; 1000])
        );
        assert_eq!(store.entries.len(), entries + 1);
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

// The executed append-only owner remains independent for read/authentication
// comparisons; it is never a production fallback or a release implementation.
#[cfg(test)]
#[allow(dead_code, deprecated)]
#[path = "batch_values_original.rs"]
mod original;

#[cfg(test)]
mod pool_tests {
    use super::*;
    #[test]
    fn pool_values_and_authentication_match_independent_append_only_owner() {
        use crate::tikv::dynamic_leaf_tests::frame;
        let mut old = original::NativeValueStore::new(64 << 20).unwrap();
        let mut new = NativeValueStore::new().unwrap();
        let values = crate::tikv::byte_unary_value_tests::values();
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for epoch in 0..12 {
            old.begin_window().unwrap();
            new.begin_window().unwrap();
            for (i, value) in values.iter().enumerate() {
                for output in [false, true] {
                    let row = InputRow {
                        occurrence: epoch,
                        input_row: 7,
                    };
                    let role = if output {
                        NativeValueRole::Output
                    } else {
                        NativeValueRole::CharInput
                    };
                    let old_role = if output {
                        original::NativeValueRole::Output
                    } else {
                        original::NativeValueRole::CharInput
                    };
                    let origin = NativeValueOrigin { node: i, row };
                    let old_origin = original::NativeValueOrigin { node: i, row };
                    let a = old.insert_clone(value, old_role, old_origin).unwrap();
                    let b = new.insert_clone(value, role, origin).unwrap();
                    let expected = frame(old.get(&a, old_role, old_origin).unwrap());
                    assert_eq!(frame(new.get(&b, role, origin).unwrap()), expected);
                    assert!(old
                        .get(
                            &a,
                            old_role,
                            original::NativeValueOrigin { node: i + 999, row }
                        )
                        .is_err());
                    assert!(new
                        .get(&b, role, NativeValueOrigin { node: i + 999, row })
                        .is_err());
                    for byte in format!("{epoch}/{i}/{output}|{expected}").bytes() {
                        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                    }
                    count += 1;
                }
            }
        }
        eprintln!("NATIVE POOL INDEPENDENT ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (3576, 0x9F323E997721426B));
    }
    #[test]
    fn consumed_subtree_slots_preserve_ancestors_and_reject_old_versions() {
        let mut store = NativeValueStore::new().unwrap();
        store.begin_window().unwrap();
        let row = InputRow {
            occurrence: 2,
            input_row: 7,
        };
        let ancestor = NativeValueOrigin { node: 1, row };
        let body = NativeValueOrigin { node: 4, row };
        let other = NativeValueOrigin {
            node: 4,
            row: InputRow {
                occurrence: 3,
                ..row
            },
        };
        let keep = store
            .insert_clone(&Datum::Int(11), NativeValueRole::CharInput, ancestor)
            .unwrap();
        let keep_other = store
            .insert_clone(&Datum::Int(12), NativeValueRole::CharInput, other)
            .unwrap();
        let value = Datum::Bytes(vec![b'x'; 32 << 10]);
        let mut old: Option<Vec<u8>> = None;
        for _ in 0..1024 {
            let token = store
                .insert_clone(&value, NativeValueRole::CharInput, body)
                .unwrap();
            if let Some(old) = old.take() {
                assert!(store.get(&old, NativeValueRole::CharInput, body).is_err());
            }
            assert_eq!(
                store.get(&token, NativeValueRole::CharInput, body).unwrap(),
                &value
            );
            store.discard_subtree(row, 4..5).unwrap();
            assert!(store.get(&token, NativeValueRole::CharInput, body).is_err());
            assert_eq!(
                store
                    .get(&keep, NativeValueRole::CharInput, ancestor)
                    .unwrap(),
                &Datum::Int(11)
            );
            assert_eq!(
                store
                    .get(&keep_other, NativeValueRole::CharInput, other)
                    .unwrap(),
                &Datum::Int(12)
            );
            assert!(store.entries.len() < 64);
            old = Some(token.to_vec());
        }
        assert_eq!(store.entries.len(), 3);
        store.entries[2].version = u32::MAX;
        assert!(matches!(
            store.insert_clone(&Datum::Null, NativeValueRole::CharInput, body),
            Err(LocalError::ResourceLimit(_))
        ));
        store.begin_window().unwrap();
        store
            .insert_clone(&Datum::Null, NativeValueRole::CharInput, body)
            .unwrap();
        assert!(store
            .get(&keep, NativeValueRole::CharInput, ancestor)
            .is_err());
    }
}

#[cfg(test)]
mod loan_tests {
    use super::*;
    fn origin() -> NativeValueOrigin {
        NativeValueOrigin {
            node: 4,
            row: InputRow {
                occurrence: 2,
                input_row: 7,
            },
        }
    }
    #[test]
    fn loan_preserves_actual_values_callbacks_and_unwind_without_a_payload_copy() {
        use crate::tikv::dynamic_leaf_tests::frame;
        let arena = RefCell::new(NativeValueStore::new().unwrap());
        for value in crate::tikv::byte_unary_value_tests::values() {
            arena.borrow_mut().begin_window().unwrap();
            let token = arena
                .borrow_mut()
                .insert_clone(&value, NativeValueRole::CharInput, origin())
                .unwrap();
            for panic in [false, true] {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let loan = NativeValueStore::lease_for_kernel(
                        &arena,
                        &token,
                        NativeValueRole::CharInput,
                        origin(),
                    )
                    .unwrap();
                    assert_eq!(frame(loan.datum()), frame(&value));
                    // A real callback can exclusively borrow the same arena.
                    let store = arena.borrow_mut();
                    store
                        .authenticate(&token, NativeValueRole::CharInput, origin())
                        .unwrap();
                    assert!(store
                        .get(&token, NativeValueRole::CharInput, origin())
                        .is_err());
                    assert!(store
                        .clone_for_kernel(&token, NativeValueRole::CharInput, origin())
                        .is_err());

                    if panic {
                        panic!("real callback unwind");
                    }
                    drop(store);
                }));
                assert_eq!(outcome.is_err(), panic);
                assert_eq!(
                    frame(
                        arena
                            .borrow()
                            .get(&token, NativeValueRole::CharInput, origin())
                            .unwrap()
                    ),
                    frame(&value)
                );
            }
        }
    }
    #[test]
    fn loan_retains_owned_payload_through_window_reset_and_cannot_resurrect_stale_slots() {
        let arena = RefCell::new(NativeValueStore::new().unwrap());
        let value = Datum::Bytes(vec![0xff; 128 << 10]);
        arena.borrow_mut().begin_window().unwrap();
        let token = arena
            .borrow_mut()
            .insert_clone(&value, NativeValueRole::CharInput, origin())
            .unwrap();
        let loan = NativeValueStore::lease_for_kernel(
            &arena,
            &token,
            NativeValueRole::CharInput,
            origin(),
        )
        .unwrap();
        arena.borrow_mut().begin_window().unwrap();
        assert!(arena
            .borrow()
            .get(&token, NativeValueRole::CharInput, origin())
            .is_err());
        let fresh;
        {
            let mut store = arena.borrow_mut();
            fresh = store
                .insert_clone(&Datum::Null, NativeValueRole::CharInput, origin())
                .unwrap();
        }
        assert_eq!(loan.datum(), &value);
        drop(loan);
        assert_eq!(
            arena
                .borrow()
                .get(&fresh, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Null
        );
        assert!(arena
            .borrow()
            .get(&token, NativeValueRole::CharInput, origin())
            .is_err());
        let lease = NativeValueStore::lease_for_kernel(
            &arena,
            &fresh,
            NativeValueRole::CharInput,
            origin(),
        )
        .unwrap();
        arena
            .borrow_mut()
            .discard_subtree(origin().row, 4..5)
            .unwrap();
        let replacement = arena
            .borrow_mut()
            .insert_clone(&Datum::Int(99), NativeValueRole::CharInput, origin())
            .unwrap();
        drop(lease);
        assert!(arena
            .borrow()
            .get(&fresh, NativeValueRole::CharInput, origin())
            .is_err());
        assert_eq!(
            arena
                .borrow()
                .get(&replacement, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Int(99)
        );
        let lease = NativeValueStore::lease_for_kernel(
            &arena,
            &replacement,
            NativeValueRole::CharInput,
            origin(),
        )
        .unwrap();
        let mut other = NativeValueStore::new().unwrap();
        other.begin_window().unwrap();
        let foreign = other
            .insert_clone(&Datum::Int(101), NativeValueRole::CharInput, origin())
            .unwrap();
        *arena.borrow_mut() = other;
        drop(lease);
        assert_eq!(
            arena
                .borrow()
                .get(&foreign, NativeValueRole::CharInput, origin())
                .unwrap(),
            &Datum::Int(101)
        );
    }
    #[test]
    fn loan_rejects_non_sql_or_forged_caps_without_consuming_sql() {
        let arena = RefCell::new(NativeValueStore::new().unwrap());
        arena.borrow_mut().begin_window().unwrap();
        assert!(arena.borrow().entries.is_empty());

        let value = Datum::VectorFloat32(VectorFloat32::init(131_072));
        let token = arena
            .borrow_mut()
            .insert_clone(&value, NativeValueRole::CharInput, origin())
            .unwrap();
        let loan = NativeValueStore::lease_for_kernel(
            &arena,
            &token,
            NativeValueRole::CharInput,
            origin(),
        )
        .unwrap();
        assert_eq!(loan.datum(), &value);
        assert!(NativeValueStore::lease_for_kernel(
            &arena,
            &token,
            NativeValueRole::CharInput,
            origin()
        )
        .is_err());
        drop(loan);
        for byte in [0, 8, 16, 24, 28, 32] {
            let mut forged = token.clone();
            forged[byte] ^= 0xff;
            assert!(NativeValueStore::lease_for_kernel(
                &arena,
                &forged,
                NativeValueRole::CharInput,
                origin()
            )
            .is_err());
        }
        let wrong = NativeValueOrigin {
            node: 5,
            ..origin()
        };
        assert!(NativeValueStore::lease_for_kernel(
            &arena,
            &token,
            NativeValueRole::CharInput,
            wrong
        )
        .is_err());
        assert!(NativeValueStore::lease_for_kernel(
            &arena,
            &token,
            NativeValueRole::Output,
            origin()
        )
        .is_err());

        let diagnostic = arena
            .borrow_mut()
            .insert_diagnostic_unavailable(origin())
            .unwrap();
        assert!(NativeValueStore::lease_for_kernel(
            &arena,
            &diagnostic,
            NativeValueRole::CharInput,
            origin()
        )
        .is_err());
    }
}
