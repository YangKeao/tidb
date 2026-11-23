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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Scalar datum comparison for sort and grouping keys.

use std::cmp::Ordering;

use tidb_ast::BinaryOp;

use crate::ops::eval_binary;
use crate::{Datum, EvalError};

/// Total order over two same-typed scalar datums, matching Go
/// `pkg/util/chunk/compare.go` `GetCompareFunc`.
///
/// Semantics are exactly the ones this crate's own `=`/`<` operators use
/// (the ordering behind `IN`/`BETWEEN`/comparison), plus the sort-side NULL
/// rule: NULL orders below every non-NULL value and equal to NULL (Go
/// `chunk.cmpNull` / `Datum.Compare`'s `KindNull` arm) instead of the
/// operators' three-valued NULL propagation. Strings compare under the
/// session `utf8mb4_bin` PAD SPACE collation; numeric kinds compare
/// cross-kind through MySQL's promotion rules (Int/UInt/Decimal exactly,
/// Real via `f64`); string-vs-numeric compares as MySQL real coercion.
/// Errors surface for operand kinds the evaluator does not order.
pub fn compare_datums(l: &Datum, r: &Datum) -> Result<Ordering, EvalError> {
    compare_datums_with_collation(l, r, crate::ops::DERIVATION_FREE_COLLATION)
}

/// [`compare_datums`] under an explicitly derived collation.
///
/// A sort or grouping key that is a string compares -- and, for grouping,
/// is IDENTIFIED -- under the collation its own expression carries, which is
/// what makes `ORDER BY ci_col` produce `a, A, b, B` and `GROUP BY ci_col`
/// produce two groups instead of four (Go builds `keyCmpFuncs` from the
/// by-item's `RetType`, whose collation the derivation set).
pub fn compare_datums_with_collation(
    l: &Datum,
    r: &Datum,
    collation: tidb_datatype::Collation,
) -> Result<Ordering, EvalError> {
    match (l, r) {
        (Datum::Null, Datum::Null) => return Ok(Ordering::Equal),
        (Datum::Null, _) => return Ok(Ordering::Less),
        (_, Datum::Null) => return Ok(Ordering::Greater),
        _ => {}
    }
    if let (Some(a), Some(b)) = (l.as_raw_bytes(), r.as_raw_bytes()) {
        return Ok(collation.compare(a, b));
    }
    match eval_binary(BinaryOp::Eq, l.clone(), r.clone())? {
        Datum::Int(1) => return Ok(Ordering::Equal),
        Datum::Int(0) => {}
        Datum::Null => return Err(EvalError::Unsupported("unordered scalar comparison")),
        _ => unreachable!("eval_binary(Eq, ...) only ever returns Int or Null"),
    }
    match eval_binary(BinaryOp::Lt, l.clone(), r.clone())? {
        Datum::Int(1) => Ok(Ordering::Less),
        Datum::Int(0) => Ok(Ordering::Greater),
        Datum::Null => Err(EvalError::Unsupported("unordered scalar comparison")),
        _ => unreachable!("eval_binary(Lt, ...) only ever returns Int or Null"),
    }
}

#[cfg(test)]
mod tests {
    use super::compare_datums_with_collation;
    use tidb_datatype::{parse_datetime, Collation, Datum};

    #[test]
    fn invalid_temporal_comparison_returns_an_error_instead_of_panicking() {
        let time = Datum::new_time(
            parse_datetime("2026-08-14 12:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        assert!(compare_datums_with_collation(
            &time,
            &Datum::new_string("not-a-time"),
            Collation::Utf8Mb4Bin,
        )
        .is_err());
    }
}
