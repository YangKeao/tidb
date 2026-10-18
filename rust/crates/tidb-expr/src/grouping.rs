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

//! Scalar `GROUPING` metadata and grouping-id evaluation.
//!
//! TiDB rewrites a user-facing `GROUPING(...)` expression into a grouping-id
//! column plus validated metadata. Public types alias the shared pure core;
//! runtime evaluation borrows the actual id and immutable marks in the pure core.
//! Planner rewriting and protobuf admission remain unchanged.

#[cfg(test)]
use std::collections::BTreeSet;

pub use tidb_query_expr::{
    GroupingFunction, GroupingMetadata, GroupingMetadataError, GroupingMode,
};

/// Execute the original pure metadata algorithm over actual id and mark sets.
pub(crate) fn eval_grouping_in(
    grouping_id: u64,
    metadata: &GroupingMetadata,
    _ctx: &dyn crate::Columns,
) -> Result<crate::Datum, crate::EvalError> {
    crate::tikv::grouping_value::value(
        Some(grouping_id),
        Some(metadata),
        tidb_query_expr::local::ExecutionLimits::default().max_retained_bytes,
    )
}
/// Actual NULL returns before metadata demand, without a fabricated id or marks.
pub(crate) fn eval_grouping_null_in(
    _ctx: &dyn crate::Columns,
) -> Result<crate::Datum, crate::EvalError> {
    crate::tikv::grouping_value::value(
        None,
        None,
        tidb_query_expr::local::ExecutionLimits::default().max_retained_bytes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marks(values: &[u64]) -> BTreeSet<u64> {
        values.iter().copied().collect()
    }

    /// Source rows from `pkg/expression/builtin_grouping_test.go:56
    /// TestGrouping`.  The Go implementation stores this result in an
    /// `int64` with the unsigned field flag; assertions compare the actual
    /// result bits as `u64` here.
    #[test]
    fn grouping_source_vectors() {
        let rows = [
            (1, GroupingMode::BitAnd, &[1][..], 0),
            (1, GroupingMode::BitAnd, &[3][..], 0),
            (1, GroupingMode::BitAnd, &[6][..], 1),
            (2, GroupingMode::BitAnd, &[1][..], 1),
            (2, GroupingMode::BitAnd, &[3][..], 0),
            (2, GroupingMode::BitAnd, &[6][..], 0),
            (4, GroupingMode::BitAnd, &[2][..], 1),
            (4, GroupingMode::BitAnd, &[4][..], 0),
            (4, GroupingMode::BitAnd, &[6][..], 0),
            (0, GroupingMode::NumericCmp, &[0][..], 1),
            (0, GroupingMode::NumericCmp, &[2][..], 1),
            (2, GroupingMode::NumericCmp, &[0][..], 0),
            (2, GroupingMode::NumericCmp, &[1][..], 0),
            (2, GroupingMode::NumericCmp, &[2][..], 1),
            (2, GroupingMode::NumericCmp, &[3][..], 1),
            (1, GroupingMode::NumericSet, &[1, 2][..], 0),
            (1, GroupingMode::NumericSet, &[2][..], 1),
            (2, GroupingMode::NumericSet, &[1, 3][..], 1),
            (2, GroupingMode::NumericSet, &[2, 3][..], 0),
        ];

        for (grouping_id, mode, grouping_ids, expected) in rows {
            let grouping = GroupingFunction::with_metadata(mode, vec![marks(grouping_ids)])
                .expect("source metadata is valid");
            assert_eq!(
                grouping.eval(grouping_id).unwrap(),
                expected,
                "mode={mode:?} id={grouping_id} marks={grouping_ids:?}"
            );
        }
    }

    #[test]
    fn metadata_validation_matches_source_guards() {
        assert_eq!(
            GroupingMode::try_from(0),
            Err(GroupingMetadataError::InvalidMode(0))
        );
        assert_eq!(
            GroupingMode::try_from(4),
            Err(GroupingMetadataError::InvalidMode(4))
        );

        for mode in [GroupingMode::BitAnd, GroupingMode::NumericCmp] {
            assert_eq!(
                GroupingMetadata::new(mode, vec![marks(&[])]),
                Err(GroupingMetadataError::InvalidGroupingMarkCount {
                    mode,
                    index: 0,
                    count: 0,
                })
            );
            assert_eq!(
                GroupingMetadata::new(mode, vec![marks(&[1, 2])]),
                Err(GroupingMetadataError::InvalidGroupingMarkCount {
                    mode,
                    index: 0,
                    count: 2,
                })
            );
        }

        // Numeric-set mode accepts an empty set: no grouping id is needed for
        // that argument, so every row is marked as grouped (`1`).
        let grouping =
            GroupingFunction::with_metadata(GroupingMode::NumericSet, vec![marks(&[])]).unwrap();
        assert_eq!(grouping.eval(0).unwrap(), 1);

        let mut uninitialized = GroupingFunction::uninitialized();
        assert_eq!(
            uninitialized.eval(1),
            Err(GroupingMetadataError::Uninitialized)
        );
        assert_eq!(
            uninitialized.metadata(),
            Err(GroupingMetadataError::Uninitialized)
        );
        assert!(uninitialized
            .set_metadata(GroupingMode::BitAnd, vec![marks(&[1, 2])])
            .is_err());
        assert_eq!(
            uninitialized.eval(1),
            Err(GroupingMetadataError::Uninitialized)
        );
    }
}
