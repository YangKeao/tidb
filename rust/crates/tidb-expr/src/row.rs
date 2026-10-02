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

//! Row-value (`ROW(...)`/`(...)`) comparison — `=`/`<>`/`<`/`>`/`<=`/`>=`
//! between two same-arity tuples, called from `crate::eval_in`'s own
//! `Expr::Binary` arm (when both operands are bare `Expr::Row` nodes)
//! and `crate::func::eval_in_list` (a row-value `IN`/`NOT IN` operand).
//! Real MySQL/TiDB restricts `ROW(...)` syntactically to ONLY these
//! positions (confirmed via `gorun`: a bare `SELECT ROW(1,2)` with no
//! comparison is a genuine parse-time ERROR there too) — so this crate
//! deliberately does NOT need a general-purpose `Datum::Row` variant
//! that could appear in `GROUP BY`/`ORDER BY`/`DISTINCT` dedup or be
//! projected as an ordinary column value; AST-level special-casing at
//! exactly these two call sites is enough.
//!
//! `<=>` (NULL-safe equal) uses its own combination rule: every position is
//! compared with scalar `<=>`, then the results are AND-composed. It therefore
//! never returns NULL and treats two NULLs in the same position as equal.

use std::cmp::Ordering;

use tidb_ast::BinaryOp;

use crate::coerce::bool_int;
use crate::ops::eval_binary;
use crate::{Columns, Datum, EvalError};

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
/// This contextless SDK utility serves sorting/grouping, not runtime row-value
/// predicates; those use `row_compare_in` with the statement's real context.
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

/// `l = r` for two same-arity row values — three-valued, SQL `AND`-
/// composed equality across ALL positions (confirmed via `gorun`: a
/// definite mismatch at ANY position decides the result `FALSE`
/// outright, even when a LATER position is `NULL` — `ROW(1,2) <>
/// ROW(2,NULL)` is `TRUE`, not `NULL`; only when NO position is a
/// definite mismatch AND at least one is `NULL` does the whole
/// comparison become `NULL`). Every position is checked — NOT stopped
/// at the first `NULL` — matching real SQL `AND`'s own semantics
/// (`FALSE AND NULL` is `FALSE`, not `NULL`, regardless of which
/// operand is evaluated first).
fn row_eq_in(l: &[Datum], r: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let mut null_result = None;
    let mut last_true = None;
    for (lv, rv) in l.iter().zip(r) {
        let computed = row_scalar_compare_in(BinaryOp::Eq, lv, rv, ctx)?;
        match computed {
            Datum::Int(0) => return Ok(computed),
            Datum::Null => null_result = Some(computed),
            Datum::Int(1) => last_true = Some(computed),
            _ => unreachable!("comparison worker returns only Int(0/1) or NULL"),
        }
    }
    Ok(null_result
        .or(last_true)
        .expect("nonempty row has a computed leaf"))
}

fn row_scalar_compare_in(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::ops::eval_comparison_values_in(
        op,
        left.clone(),
        right.clone(),
        crate::ops::DERIVATION_FREE_COLLATION,
        crate::ops::Operands::LITERALS,
        ctx,
    )
}

fn negate_row_predicate_in(value: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(value);
    }
    crate::eval_boolean_ready_in(
        crate::BooleanFunction::UnaryNot,
        crate::truthy_of(&value)?,
        ctx,
    )
}

/// `l <op> r` for two same-arity row values, `op` one of
/// `Eq`/`Ne`/`NullEq`/`Lt`/`Gt`/`Le`/`Ge` — see [`row_eq_in`]'s own doc for
/// equality; the four ordering operators are LEXICOGRAPHIC (confirmed
/// via `gorun`: the FIRST position where the two rows differ decides
/// the whole comparison, regardless of what follows — `ROW(2,1) <
/// ROW(1,NULL)` is `FALSE`, not `NULL`, since position 0 alone (`2 <
/// 1` is false) already decides it without ever looking at position 1's
/// own `NULL`). Real TiDB rejects a mismatched row arity outright
/// (confirmed via `gorun`: `ROW(1,2) = ROW(1,2,3)` is a genuine `ERR`)
/// — modelled here as `Unsupported` too, matching this crate's own
/// convention for other rare-but-real SQL error conditions (e.g.
/// `crate::eval_in`'s own scalar-subquery-with-multiple-columns case).
///
/// The live context now reaches scalar preparation as well as the worker:
/// unlike the former `NoColumns` route, temporal parsing observes session date
/// modes/timezone and mixed numeric text observes statement warning/error policy.
/// Collation remains the original derivation-free collation for this AST tier.
pub(crate) fn row_compare_in(
    op: BinaryOp,
    l: &[Datum],
    r: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if l.len() != r.len() {
        return Err(EvalError::Unsupported("row value arity mismatch"));
    }
    if l.is_empty() {
        // Preserve the original zero-operand tuple identities. This structural
        // edge has no scalar leaf to submit and invents no comparison witness.
        return match op {
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::NullEq
            | BinaryOp::Lt
            | BinaryOp::Gt
            | BinaryOp::Le
            | BinaryOp::Ge => Ok(bool_int(matches!(
                op,
                BinaryOp::Eq | BinaryOp::NullEq | BinaryOp::Le | BinaryOp::Ge
            ))),
            _ => Err(EvalError::Unsupported("row value comparison operator")),
        };
    }
    match op {
        BinaryOp::Eq => row_eq_in(l, r, ctx),
        BinaryOp::Ne => negate_row_predicate_in(row_eq_in(l, r, ctx)?, ctx),
        // Go `constructBinaryOpFunction` rewrites row `<=>` into one scalar
        // `<=>` per position and ComposeCNFCondition over those results.
        // NULL-safe equality is outside the six ordinary comparison profiles.
        BinaryOp::NullEq => {
            let mut last_true = None;
            for (lv, rv) in l.iter().zip(r) {
                let computed = crate::ops::eval_binary_full(
                    BinaryOp::NullEq,
                    lv.clone(),
                    rv.clone(),
                    4,
                    crate::ops::DERIVATION_FREE_COLLATION,
                    crate::ops::Operands::LITERALS,
                    ctx,
                )?;
                match computed {
                    Datum::Int(0) => return Ok(computed),
                    Datum::Int(1) => last_true = Some(computed),
                    _ => unreachable!("scalar NullEq only ever returns Int(0/1)"),
                }
            }
            Ok(last_true.expect("nonempty row has a computed leaf"))
        }
        BinaryOp::Lt | BinaryOp::Gt | BinaryOp::Le | BinaryOp::Ge => {
            let mut last_true = None;
            for (lv, rv) in l.iter().zip(r) {
                let computed = row_scalar_compare_in(BinaryOp::Eq, lv, rv, ctx)?;
                match computed {
                    Datum::Null => return Ok(computed),
                    Datum::Int(1) => last_true = Some(computed),
                    Datum::Int(0) => {
                        let less = row_scalar_compare_in(BinaryOp::Lt, lv, rv, ctx)?;
                        // Retain the original Eq-then-Lt composition, including
                        // NaN: GT/GE here are !Lt, not direct scalar GT/GE.
                        return match op {
                            BinaryOp::Lt | BinaryOp::Le => Ok(less),
                            BinaryOp::Gt | BinaryOp::Ge => negate_row_predicate_in(less, ctx),
                            _ => unreachable!(),
                        };
                    }
                    _ => unreachable!("comparison worker returns only Int(0/1) or NULL"),
                }
            }
            let equal = last_true.expect("nonempty row has a computed leaf");
            if matches!(op, BinaryOp::Le | BinaryOp::Ge) {
                Ok(equal)
            } else {
                negate_row_predicate_in(equal, ctx)
            }
        }
        _ => Err(EvalError::Unsupported("row value comparison operator")),
    }
}

#[cfg(test)]
mod tests {
    use super::compare_datums_with_collation;
    use tidb_datatype::{parse_datetime, Collation, Datum};

    #[test]
    fn row_comparison_keeps_nan_composition_nulls_and_empty_identities() {
        use super::row_compare_in;
        use tidb_ast::BinaryOp;

        for (op, nan_result, equal_result) in [
            (BinaryOp::Eq, 0, 1),
            (BinaryOp::Ne, 1, 0),
            (BinaryOp::Lt, 0, 0),
            (BinaryOp::Le, 0, 1),
            (BinaryOp::Gt, 1, 0),
            (BinaryOp::Ge, 1, 1),
        ] {
            assert_eq!(
                row_compare_in(
                    op,
                    &[Datum::Real(f64::NAN)],
                    &[Datum::Real(2.0)],
                    &crate::NoColumns
                )
                .unwrap(),
                Datum::Int(nan_result),
            );
            assert_eq!(
                row_compare_in(op, &[Datum::Int(2)], &[Datum::Int(2)], &crate::NoColumns).unwrap(),
                Datum::Int(equal_result),
            );
            // Empty tuples have no leaf: these are structural identities, not
            // fabricated scalar operands submitted as a comparison witness.
            assert_eq!(
                row_compare_in(op, &[], &[], &crate::NoColumns).unwrap(),
                Datum::Int(equal_result)
            );
        }
        let left = [Datum::Null, Datum::Int(1)];
        let right = [Datum::Int(0), Datum::Int(2)];
        assert_eq!(
            row_compare_in(BinaryOp::Eq, &left, &right, &crate::NoColumns).unwrap(),
            Datum::Int(0)
        );
        assert_eq!(
            row_compare_in(BinaryOp::Ne, &left, &right, &crate::NoColumns).unwrap(),
            Datum::Int(1)
        );
        assert_eq!(
            row_compare_in(BinaryOp::Gt, &left, &right, &crate::NoColumns).unwrap(),
            Datum::Null
        );
        assert_eq!(
            row_compare_in(BinaryOp::NullEq, &[], &[], &crate::NoColumns).unwrap(),
            Datum::Int(1)
        );
        assert!(row_compare_in(BinaryOp::Eq, &[], &[Datum::Null], &crate::NoColumns).is_err());
    }

    #[test]
    fn row_comparison_activates_statement_coercion_context() {
        use super::row_compare_in;
        use std::cell::{Cell, RefCell};
        use tidb_ast::BinaryOp;

        #[derive(Default)]
        struct Statement {
            truncations: Cell<usize>,
            date_reads: Cell<usize>,
            zone_reads: Cell<usize>,
            warnings: RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for Statement {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                self.truncations.set(self.truncations.get() + 1);
                crate::ErrorLevel::Error
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                self.date_reads.set(self.date_reads.get() + 1);
                tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE
            }
            fn time_zone(&self) -> crate::SessionTimeZone {
                self.zone_reads.set(self.zone_reads.get() + 1);
                crate::SessionTimeZone::Fixed {
                    name: "UTC".to_owned(),
                    offset_secs: 0,
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        let ctx = Statement::default();
        let time = Datum::new_time(
            parse_datetime("2026-08-14 12:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        for op in [BinaryOp::Eq, BinaryOp::NullEq] {
            assert!(matches!(
                row_compare_in(op, &[Datum::Int(12)], &[Datum::new_string("12x")], &ctx),
                Err(crate::EvalError::TruncatedWrongValue(_)),
            ));
            let (text, expected) = if op == BinaryOp::NullEq {
                ("2026-08-14 12:00:00", Datum::Int(1))
            } else {
                ("not-a-time", Datum::Null)
            };
            assert_eq!(
                row_compare_in(op, &[time.clone()], &[Datum::new_string(text)], &ctx).unwrap(),
                expected,
            );
        }
        assert_eq!(ctx.truncations.get(), 2);
        assert_eq!(ctx.date_reads.get(), 2);
        assert_eq!(ctx.zone_reads.get(), 2);
        assert_eq!(
            *ctx.warnings.borrow(),
            vec![(1292, "Incorrect datetime value: 'not-a-time'".to_owned()),]
        );
    }

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
