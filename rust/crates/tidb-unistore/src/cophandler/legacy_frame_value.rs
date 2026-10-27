// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Owned values for actual legacy typed consumer ports. This module is a
//! registry preserves consumer profiles throughout the SDK frame driver.
//! Bytes tokens are private capabilities, never SQL values or declarations.
use super::{LegacyEvalError, LegacyResult};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use tidb_datatype::{BinaryJSON, Datum, Decimal, MySqlDuration, Time};

const MAGIC: u64 = 0x31474c56414c5545;
const WORDS: usize = 8;
const TOKEN_BYTES: usize = WORDS * size_of::<u64>();
static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum View {
    Int,
    FoldedInt,
    Decimal,
    Json,
    Real,
    Duration,
    Time,
    Bytes,
    Datum,
}

pub(super) enum Value {
    Int(Option<i128>),
    Decimal(Option<Decimal>),
    Json(Option<BinaryJSON>),
    Real(Option<f64>),
    Duration(Option<MySqlDuration>),
    Time(Option<Time>),
    Bytes(Option<Vec<u8>>),
    Datum(Datum),
}

impl Value {
    pub(super) fn is_null(&self) -> bool {
        match self {
            Self::Int(v) => v.is_none(),
            Self::Decimal(v) => v.is_none(),
            Self::Json(v) => v.is_none(),
            Self::Real(v) => v.is_none(),
            Self::Duration(v) => v.is_none(),
            Self::Time(v) => v.is_none(),
            Self::Bytes(v) => v.is_none(),
            Self::Datum(v) => v.is_null(),
        }
    }
    fn accepts(&self, view: View) -> bool {
        matches!(
            (self, view),
            (Self::Int(_), View::Int | View::FoldedInt)
                | (Self::Decimal(_), View::Decimal)
                | (Self::Json(_), View::Json)
                | (Self::Real(_), View::Real)
                | (Self::Duration(_), View::Duration)
                | (Self::Time(_), View::Time)
                | (Self::Bytes(_), View::Bytes)
                | (Self::Datum(_), View::Datum)
        )
    }

    fn payload(&self) -> usize {
        match self {
            Self::Bytes(Some(v)) => v.capacity(),
            Self::Decimal(Some(v)) => v.coefficient_bytes().len(),
            Self::Json(Some(v)) => v.value().len().saturating_add(1),
            Self::Datum(Datum::Bytes(v)) | Self::Datum(Datum::Raw(v)) => v.capacity(),
            Self::Datum(Datum::String(v)) => v.bytes().len(),
            Self::Datum(Datum::Decimal(v)) => v.coefficient_bytes().len(),
            Self::Datum(Datum::Json(v)) => v.value().len().saturating_add(1),
            Self::Datum(Datum::VectorFloat32(v)) => v.len().saturating_mul(size_of::<f32>()),
            _ => 0,
        }
    }
}

type Outcome = LegacyResult<Value>;

fn payload(outcome: &Outcome) -> usize {
    match outcome {
        Ok(v) => v.payload(),
        Err(LegacyEvalError::Sql(v)) => v.capacity(),
        // Opaque native causes retain their original owner. Its internals and
        // externally shared native byte owners are not a total-heap claim.
        Err(LegacyEvalError::Infrastructure(_) | LegacyEvalError::InvalidResult(_)) => 0,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Origin {
    pub(super) node: usize,
    pub(super) view: View,
    pub(super) occurrence: usize,
    pub(super) physical: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValueError {
    Contract,
    Resource,
}

struct Entry {
    outcome: Option<Outcome>,
    origin: Origin,
    payload: usize,
}

/// One invocation's affine outcome owners, including actual errors. Charges
/// cover actual table capacity and Vec/String capacities plus logical encoded
/// payload for externally shared Decimal/JSON/String/vector owners. Kernel
/// transients, SDK frames and the immutable source are accounted separately.
pub(super) struct Values {
    namespace: u64,
    generation: u64,
    entries: Vec<Entry>,
    payload: usize,
    maximum: usize,
    poisoned: bool,
}

impl Values {
    pub(super) fn new(maximum: usize) -> Result<Self, ValueError> {
        let mut namespace = NEXT_NAMESPACE.load(Ordering::Relaxed);
        loop {
            let next = namespace.checked_add(1).ok_or(ValueError::Resource)?;
            match NEXT_NAMESPACE.compare_exchange_weak(
                namespace,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => namespace = current,
            }
        }
        Ok(Self {
            namespace,
            generation: 1,
            entries: vec![],
            payload: 0,
            maximum,
            poisoned: false,
        })
    }

    fn bytes(&self) -> Result<usize, ValueError> {
        self.entries
            .capacity()
            .checked_mul(size_of::<Entry>())
            .and_then(|n| n.checked_add(self.payload))
            .ok_or(ValueError::Resource)
    }

    fn check(&self, additional: usize) -> Result<(), ValueError> {
        if self.poisoned
            || self
                .bytes()?
                .checked_add(additional)
                .is_none_or(|n| n > self.maximum)
        {
            Err(ValueError::Resource)
        } else {
            Ok(())
        }
    }

    pub(super) fn insert(
        &mut self,
        origin: Origin,
        outcome: Outcome,
    ) -> Result<Vec<u8>, ValueError> {
        if outcome.as_ref().is_ok_and(|v| !v.accepts(origin.view)) {
            return Err(ValueError::Contract);
        }
        let charge = payload(&outcome);
        let additional_header = if self.entries.len() == self.entries.capacity() {
            size_of::<Entry>()
        } else {
            0
        };
        self.check(
            charge
                .checked_add(TOKEN_BYTES)
                .and_then(|n| n.checked_add(additional_header))
                .ok_or(ValueError::Resource)?,
        )?;
        self.entries
            .try_reserve_exact(1)
            .map_err(|_| ValueError::Resource)?;
        if let Err(error) = self.check(
            charge
                .checked_add(TOKEN_BYTES)
                .ok_or(ValueError::Resource)?,
        ) {
            self.poisoned = true;
            return Err(error);
        }
        let index = self.entries.len();
        let fields = [
            MAGIC,
            self.namespace,
            self.generation,
            u64::try_from(index).map_err(|_| ValueError::Resource)?,
            u64::try_from(origin.node).map_err(|_| ValueError::Resource)?,
            origin.view as u64,
            u64::try_from(origin.occurrence).map_err(|_| ValueError::Resource)?,
            u64::try_from(origin.physical).map_err(|_| ValueError::Resource)?,
        ];
        let mut token = Vec::new();
        token
            .try_reserve_exact(TOKEN_BYTES)
            .map_err(|_| ValueError::Resource)?;
        self.check(
            charge
                .checked_add(token.capacity())
                .ok_or(ValueError::Resource)?,
        )?;
        for word in fields {
            token.extend_from_slice(&word.to_le_bytes());
        }
        self.payload = self
            .payload
            .checked_add(charge)
            .ok_or(ValueError::Resource)?;
        self.entries.push(Entry {
            outcome: Some(outcome),
            origin,
            payload: charge,
        });
        Ok(token)
    }

    fn index(&self, token: &[u8], origin: Origin) -> Result<usize, ValueError> {
        if self.poisoned || token.len() != TOKEN_BYTES {
            return Err(ValueError::Contract);
        }
        let mut words = [0; WORDS];
        for (word, bytes) in words.iter_mut().zip(token.chunks_exact(8)) {
            *word = u64::from_le_bytes(bytes.try_into().map_err(|_| ValueError::Contract)?);
        }
        if words[0] != MAGIC
            || words[1] != self.namespace
            || words[2] != self.generation
            || words[4] != origin.node as u64
            || words[5] != origin.view as u64
            || words[6] != origin.occurrence as u64
            || words[7] != origin.physical as u64
        {
            return Err(ValueError::Contract);
        }
        let index = usize::try_from(words[3]).map_err(|_| ValueError::Contract)?;
        self.entries
            .get(index)
            .filter(|e| e.origin == origin && e.outcome.is_some())
            .ok_or(ValueError::Contract)?;
        Ok(index)
    }

    /// Inspect demand state without transferring the charged outcome owner.
    pub(super) fn peek(&self, token: &[u8], origin: Origin) -> Result<&Outcome, ValueError> {
        let index = self.index(token, origin)?;
        self.entries[index]
            .outcome
            .as_ref()
            .ok_or(ValueError::Contract)
    }

    /// Include the adapter's actual task/route/scope capacities in this same
    /// finite allowance while outcomes stay owned here across SDK suspensions.
    pub(super) fn check_external(&self, bytes: usize) -> Result<(), ValueError> {
        self.check(bytes)
    }

    /// Consume once after checking every namespace, generation, actual source
    /// consumer and row field. Original errors move without cloning/rendering.
    pub(super) fn take(&mut self, token: &[u8], origin: Origin) -> Result<Outcome, ValueError> {
        let index = self.index(token, origin)?;
        let entry = &mut self.entries[index];
        let outcome = entry.outcome.take().ok_or(ValueError::Contract)?;
        self.payload = self
            .payload
            .checked_sub(entry.payload)
            .ok_or(ValueError::Contract)?;
        entry.payload = 0;
        Ok(outcome)
    }

    #[cfg(test)]
    pub(super) fn reset(&mut self) -> Result<(), ValueError> {
        self.check(0)?;
        let next = self.generation.checked_add(1).ok_or(ValueError::Resource)?;
        self.entries.clear();
        self.payload = 0;
        self.generation = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(view: View) -> (super::super::SimpleExpr, View) {
        (super::super::SimpleExpr::Int(7), view)
    }
    fn origin(expr: &super::super::SimpleExpr, view: View) -> Origin {
        Origin {
            node: expr as *const _ as usize,
            view,
            occurrence: 2,
            physical: 19,
        }
    }

    #[test]
    fn legacy_outcome_abi_preserves_full_width_null_and_exact_native_error() {
        let (expr, view) = source(View::Int);
        let origin = origin(&expr, view);
        let mut values = Values::new(1024 * 1024).unwrap();
        let token = values
            .insert(origin, Ok(Value::Int(Some(i128::MIN))))
            .unwrap();
        assert!(matches!(
            values.take(&token, origin).unwrap(),
            Ok(Value::Int(Some(i128::MIN)))
        ));
        assert!(matches!(
            values.take(&token, origin),
            Err(ValueError::Contract)
        ));
        let token = values.insert(origin, Ok(Value::Int(None))).unwrap();
        assert!(matches!(
            values.take(&token, origin).unwrap(),
            Ok(Value::Int(None))
        ));

        let cache = tidb_expr::ReadyValueCache::new();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cache.with_columns(&tidb_expr::NoColumns, |_| panic!("genuine scope producer"));
        }))
        .is_err());
        let error = cache
            .with_columns(&tidb_expr::NoColumns, |columns| {
                tidb_expr::eval_pi_in(columns)
            })
            .unwrap_err();
        let exact = error.clone();
        let token = values
            .insert(origin, Err(LegacyEvalError::Infrastructure(error)))
            .unwrap();
        match values.take(&token, origin).unwrap() {
            Err(LegacyEvalError::Infrastructure(error)) => {
                assert_eq!(error, exact, "same opaque cause identity")
            }
            _ => panic!("actual infrastructure error became a value or SQL NULL"),
        }
        let token = values
            .insert(origin, Err(LegacyEvalError::Sql("original SQL".into())))
            .unwrap();
        assert!(
            matches!(values.take(&token, origin).unwrap(), Err(LegacyEvalError::Sql(s)) if s == "original SQL")
        );
    }

    #[test]
    fn legacy_outcome_abi_rejects_foreign_roles_sources_rows_generations_and_resource_growth() {
        let (expr, view) = source(View::Int);
        let origin = origin(&expr, view);
        let mut values = Values::new(1024 * 1024).unwrap();
        let token = values
            .insert(origin, Ok(Value::Int(Some(i128::MAX))))
            .unwrap();
        let mut foreign = Values::new(1024 * 1024).unwrap();
        assert!(matches!(
            foreign.take(&token, origin),
            Err(ValueError::Contract)
        ));
        for changed in [
            Origin {
                node: origin.node + 1,
                ..origin
            },
            Origin {
                view: View::FoldedInt,
                ..origin
            },
            Origin {
                occurrence: 3,
                ..origin
            },
            Origin {
                physical: 20,
                ..origin
            },
        ] {
            assert!(matches!(
                values.take(&token, changed),
                Err(ValueError::Contract)
            ));
        }
        assert!(matches!(
            values.insert(origin, Ok(Value::Bytes(None))),
            Err(ValueError::Contract)
        ));
        assert!(matches!(
            values.take(&token[..TOKEN_BYTES - 1], origin),
            Err(ValueError::Contract)
        ));
        values.reset().unwrap();
        assert!(matches!(
            values.take(&token, origin),
            Err(ValueError::Contract)
        ));
        let mut denied = Values::new(size_of::<Entry>() + TOKEN_BYTES - 1).unwrap();
        assert!(matches!(
            denied.insert(origin, Ok(Value::Int(Some(1)))),
            Err(ValueError::Resource)
        ));
        let bytes_origin = Origin {
            view: View::Bytes,
            ..origin
        };
        let mut large = Vec::with_capacity(4096);
        large.extend_from_slice(b"small logical length, real large capacity");
        let mut denied = Values::new(1024).unwrap();
        assert!(matches!(
            denied.insert(bytes_origin, Ok(Value::Bytes(Some(large)))),
            Err(ValueError::Resource)
        ));

        // Every real consumer role has its own token, including actual NULLs.
        for (view, value) in [
            (View::Decimal, Value::Decimal(None)),
            (View::Json, Value::Json(None)),
            (View::Real, Value::Real(None)),
            (View::Duration, Value::Duration(None)),
            (View::Time, Value::Time(None)),
            (View::Bytes, Value::Bytes(None)),
            (View::Datum, Value::Datum(Datum::Null)),
        ] {
            let origin = Origin { view, ..origin };
            let token = values.insert(origin, Ok(value)).unwrap();
            assert!(values
                .take(&token, origin)
                .unwrap()
                .unwrap_or_else(|_| panic!("actual nullable value"))
                .is_null());
        }
    }
}
