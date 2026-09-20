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

//! `pkg/executor` `SelectionExec`: keeps the child rows for which every filter
//! evaluates to true -- the `WHERE`/`HAVING` operator.
//!
//! A row passes when every filter is truthy; a filter that is false OR NULL
//! rejects the row (MySQL's three-valued logic).
//!
//! The executor retains its position in the current child chunk across calls,
//! stops when the output chunk is full, and returns one row at a time when a
//! filter has order-sensitive side effects. Its cached child chunk is charged
//! to the statement memory budget for its whole open lifetime. Pure filters
//! are evaluated once into a reusable selection mask. A retained filter program
//! routes demanded expressions through evaluator admission and reuses compiled
//! engine plans, with native evaluation as a correctness-preserving fallback.

use std::sync::Arc;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{vectorizable, FilterProgram};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

use crate::StatementMemory;

/// Go `SelectionExec`: filters its child's rows by a conjunction of predicates.
pub struct SelectionExec<C: Columns> {
    meta: ExecutorMeta,
    filters: FilterProgram,
    batched: bool,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Option<Chunk>,
    tracker: Arc<Tracker>,
    memory: StatementMemory,
    input_row: usize,
    selected: Vec<bool>,
    done: bool,
}

impl<C: Columns> SelectionExec<C> {
    /// Builds a selection of `child`'s rows satisfying every filter in
    /// `filters`, evaluated with `ctx`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        filters: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let tracker = memory.operator_tracker(meta.id());
        let batched = vectorizable(&filters);
        SelectionExec {
            meta,
            filters: FilterProgram::new(filters),
            batched,
            child,
            ctx,
            child_chunk: None,
            tracker,
            memory,
            input_row: 0,
            selected: Vec::new(),
            done: false,
        }
    }

    /// Whether a row satisfies every filter (all truthy). A false or NULL filter
    /// rejects the row.
    fn row_passes(&self, row: tidb_chunk::row::Row<'_>) -> Result<bool, ExecError> {
        Ok(self.filters.matches_row(&self.ctx, row)?)
    }

    /// Evaluates all pure filters into the physical-row mask used by the
    /// batched path. This mirrors Go's `VectorizedFilter` contract: filters
    /// are applied filter-major, already rejected rows are skipped, and a
    /// false or NULL result clears the row. The retained filter program owns
    /// evaluator admission and compiled-plan reuse across child chunks.
    fn evaluate_selection_mask(&mut self) -> Result<(), ExecError> {
        let child_chunk = self
            .child_chunk
            .as_ref()
            .expect("selection child chunk exists while open");
        let (selected, _) =
            self.filters
                .consider_null(&self.ctx, true, child_chunk, Vec::new(), Vec::new())?;
        self.selected = selected;
        Ok(())
    }

    fn release_child_chunk(&mut self) {
        self.child_chunk = None;
        self.selected.clear();
        self.tracker.replace_bytes_used(0);
    }
}

impl<C: Columns + Send> Executor for SelectionExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.release_child_chunk();
        self.child.open()?;
        let child_chunk = self.child.new_chunk();
        self.tracker.replace_bytes_used(child_chunk.memory_usage());
        self.child_chunk = Some(child_chunk);
        self.input_row = 0;
        self.selected.clear();
        self.done = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.grow_and_reset(self.max_chunk_size());
        if self.done {
            return Ok(());
        }
        self.memory.check()?;
        if self.batched {
            loop {
                while self.input_row < self.selected.len() {
                    if req.is_full() {
                        return Ok(());
                    }
                    if self.selected[self.input_row] {
                        let row = self
                            .child_chunk
                            .as_ref()
                            .expect("selection child chunk exists while open")
                            .get_row(self.input_row);
                        req.append_row(row);
                    }
                    self.input_row += 1;
                }

                let child_chunk = self
                    .child_chunk
                    .as_mut()
                    .expect("selection child chunk exists while open");
                let before = child_chunk.memory_usage();
                let result = self.child.next(child_chunk);
                self.tracker.consume(child_chunk.memory_usage() - before);
                result?;
                self.memory.check()?;
                if child_chunk.num_rows() == 0 {
                    self.done = true;
                    self.selected.clear();
                    return Ok(());
                }
                self.evaluate_selection_mask()?;
                self.input_row = 0;
            }
        }
        loop {
            let child_chunk = self
                .child_chunk
                .as_ref()
                .expect("selection child chunk exists while open");
            let rows = child_chunk.num_rows();
            while self.input_row < rows {
                if req.is_full() {
                    return Ok(());
                }
                let row = child_chunk.get_row(self.input_row);
                let selected = self.row_passes(row)?;
                if selected {
                    req.append_row(row);
                }
                self.input_row += 1;
                if selected && !self.batched {
                    return Ok(());
                }
            }

            let child_chunk = self
                .child_chunk
                .as_mut()
                .expect("selection child chunk exists while open");
            let before = child_chunk.memory_usage();
            let result = self.child.next(child_chunk);
            self.tracker.consume(child_chunk.memory_usage() - before);
            result?;
            self.memory.check()?;
            self.input_row = 0;
            if child_chunk.num_rows() == 0 {
                self.done = true;
                return Ok(());
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.release_child_chunk();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }

    /// Negotiate through this filter, not through its unfiltered child.
    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        Some(self)
    }
}

// A Selection is not a transparent wrapper. Its bound expressions and schema
// still describe the input row, and its cardinality is the filtered row count.
// Schema-changing offers and row cuts therefore retain TableAccess's refusal:
// aggregation/projection would invalidate the bindings, while Limit/TopN could
// discard qualifying rows. Go's LogicalSelection.PruneColumns keeps predicate
// columns and its base PushDownTopN retains the cut above the Selection.
// Predicate/range negotiation remains available to reordered joins without
// allowing them to bypass this operator's obligations.
impl<C: Columns> crate::table_access::TableAccess for SelectionExec<C> {
    fn accept_scan_filter(
        &mut self,
        filter: &crate::predicate_pushdown::PushedScanFilter,
        ctx: &crate::StmtContext,
    ) -> bool {
        self.child
            .table_access()
            .is_some_and(|access| access.accept_scan_filter(filter, ctx))
    }

    fn accept_handle_ranges(&mut self, ranges: &[crate::kv_table::IndexRange]) -> bool {
        self.child
            .table_access()
            .is_some_and(|access| access.accept_handle_ranges(ranges))
    }

    fn accept_partition_pruning(&mut self, ids: &[i64]) -> bool {
        self.child
            .table_access()
            .is_some_and(|access| access.accept_partition_pruning(ids))
    }

    fn accept_keep_order(&mut self, descending: bool) -> bool {
        self.child
            .table_access()
            .is_some_and(|access| access.accept_keep_order(descending))
    }

    fn accept_scan_estimate(&mut self, rows: f64) {
        if let Some(access) = self.child.table_access() {
            access.accept_scan_estimate(rows);
        }
    }

    fn accept_lookup_batch_size(&mut self, size: u64) -> bool {
        self.child
            .table_access()
            .is_some_and(|access| access.accept_lookup_batch_size(size))
    }

    fn accept_index_filter(&mut self) -> bool {
        self.child
            .table_access()
            .is_some_and(|access| access.accept_index_filter())
    }
}

impl<C: Columns> Drop for SelectionExec<C> {
    fn drop(&mut self) {
        self.release_child_chunk();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::NoColumns;

    use crate::{OomAction, StatementMemory};

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn string() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    #[derive(Default)]
    struct UserVariables(RefCell<HashMap<String, Datum>>);

    impl Columns for UserVariables {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_uservar(&self, name: &str) -> Option<Datum> {
            self.0.borrow().get(&name.to_ascii_lowercase()).cloned()
        }

        fn set_uservar(&self, name: &str, value: Datum) {
            self.0.borrow_mut().insert(name.to_ascii_lowercase(), value);
        }
    }

    /// A test-only source that emits one prebuilt chunk, then EOF.
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }

    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            self.meta.init_cap()
        }
        fn max_chunk_size(&self) -> usize {
            self.meta.max_chunk_size()
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    fn one_long_col_schema() -> Schema {
        let mut c = Column::new(1, long());
        c.index = 0;
        Schema::new(vec![c])
    }

    #[test]
    fn partial_fast_filter_preserves_assignment_order() {
        for fast_first in [false, true] {
            for is_null in [false, true] {
                let column = Expression::Column(one_long_col_schema().columns[0].clone());
                let assignment = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("setvar"),
                    long(),
                    vec![
                        Expression::Constant(Constant::new(Datum::Bytes(b"v".to_vec()), string())),
                        Expression::Constant(Constant::new(Datum::Int(1), long())),
                    ],
                ));
                let null_test = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("isnull"),
                    long(),
                    vec![column],
                ));
                let args = if fast_first {
                    vec![null_test, assignment]
                } else {
                    vec![assignment, null_test]
                };
                let filter = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("and"),
                    long(),
                    args,
                ));
                let mut data = Chunk::new_with_capacity(&[long()], 1);
                if is_null {
                    data.append_null(0);
                } else {
                    data.append_int64(0, 1);
                }
                let source = OneChunkSource {
                    meta: ExecutorMeta::new(one_long_col_schema(), 0, 1, 8),
                    data: Some(data),
                };
                let mut selection = SelectionExec::new(
                    ExecutorMeta::new(one_long_col_schema(), 1, 1, 8),
                    vec![filter],
                    Box::new(source),
                    UserVariables::default(),
                    StatementMemory::default(),
                );
                selection.open().unwrap();
                let mut result = selection.new_chunk();
                selection.next(&mut result).unwrap();
                assert_eq!(result.num_rows(), usize::from(is_null));
                assert_eq!(
                    selection.ctx.get_uservar("v"),
                    (!fast_first || is_null).then_some(Datum::Int(1)),
                    "fast_first={fast_first}, is_null={is_null}"
                );
                selection.close().unwrap();
            }
        }
    }

    #[test]
    fn partial_fast_filter_preserves_warning_and_error_order() {
        let integer = |value| Expression::Constant(Constant::new(Datum::Int(value), long()));
        for (name, left, right) in [("intdiv", 1, 0), ("plus", i64::MAX, 1)] {
            for fast_first in [false, true] {
                let diagnostic = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    long(),
                    vec![integer(left), integer(right)],
                ));
                let null_test = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("isnull"),
                    long(),
                    vec![Expression::Column(one_long_col_schema().columns[0].clone())],
                ));
                let args = if fast_first {
                    vec![null_test, diagnostic]
                } else {
                    vec![diagnostic, null_test]
                };
                let filter = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("and"),
                    long(),
                    args,
                ));
                let mut data = Chunk::new_with_capacity(&[long()], 1);
                data.append_int64(0, 1);
                let source = OneChunkSource {
                    meta: ExecutorMeta::new(one_long_col_schema(), 0, 1, 8),
                    data: Some(data),
                };
                let ctx = crate::StmtContext::for_query();
                let mut selection = SelectionExec::new(
                    ExecutorMeta::new(one_long_col_schema(), 1, 1, 8),
                    vec![filter],
                    Box::new(source),
                    ctx.clone(),
                    ctx.statement_memory(),
                );
                selection.open().unwrap();
                let mut result = selection.new_chunk();
                let outcome = selection.next(&mut result);
                if name == "plus" && !fast_first {
                    let error = crate::DriverError::from(outcome.unwrap_err()).to_mysql_error();
                    assert_eq!(error.code, 1690);
                } else {
                    outcome.unwrap();
                    assert_eq!(result.num_rows(), 0);
                }
                let warnings = ctx.take_warnings();
                assert_eq!(
                    warnings.len(),
                    usize::from(name == "intdiv" && !fast_first),
                    "{name}, fast_first={fast_first}"
                );
                if let Some(warning) = warnings.first() {
                    assert_eq!(warning.1, 1365);
                }
                selection.close().unwrap();
                assert_eq!(ctx.statement_memory().bytes_consumed(), 0);
            }
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn selection_engine_reuses_compilation_across_child_chunks() {
        for engine in [false, true] {
            let children = [[2, 3, 0], [4, 5, 1]]
                .into_iter()
                .map(|values| {
                    let mut data = Chunk::new_with_capacity(&[long()], values.len());
                    for value in values {
                        data.append_int64(0, value);
                    }
                    Box::new(OneChunkSource {
                        meta: ExecutorMeta::new(one_long_col_schema(), 0, 3, 3),
                        data: Some(data),
                    }) as Box<dyn Executor>
                })
                .collect();
            let source = crate::union_all::UnionAllExec::new(
                ExecutorMeta::new(one_long_col_schema(), 1, 3, 3),
                children,
            );
            let filter = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("gt"),
                long(),
                vec![
                    Expression::Column(one_long_col_schema().columns[0].clone()),
                    Expression::Constant(Constant::new(Datum::Int(1), long())),
                ],
            ));
            let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
            let mut selection = SelectionExec::new(
                ExecutorMeta::new(one_long_col_schema(), 2, 1, 1),
                vec![filter],
                Box::new(source),
                ctx.clone(),
                ctx.statement_memory(),
            );
            assert!(selection.batched);
            assert_eq!(selection.filters.tikv_compilations(), 0);
            selection.open().unwrap();
            assert_eq!(ctx.tikv_expression_rows(), 0);
            let mut req = selection.new_chunk();
            for expected in [2, 3, 4, 5] {
                selection.next(&mut req).unwrap();
                assert_eq!(req.num_rows(), 1);
                assert_eq!(req.get_row(0).get_int64(0), expected);
                assert_eq!(
                    selection.filters.tikv_compilations(),
                    u64::from(engine),
                    "engine={engine}, row={expected}"
                );
            }
            selection.next(&mut req).unwrap();
            assert_eq!(req.num_rows(), 0);
            assert_eq!(ctx.tikv_expression_rows(), if engine { 6 } else { 0 });
            assert_eq!(selection.filters.tikv_compilations(), u64::from(engine));
            selection.close().unwrap();
            assert_eq!(ctx.statement_memory().bytes_consumed(), 0);
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn selection_engine_false_and_null_skip_later_overflow() {
        for (engine, row_mode) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut data = Chunk::new_with_capacity(&[long()], 2);
            data.append_int64(0, 0);
            data.append_null(0);
            let source = OneChunkSource {
                meta: ExecutorMeta::new(one_long_col_schema(), 0, 2, 2),
                data: Some(data),
            };
            let integer = |value| Expression::Constant(Constant::new(Datum::Int(value), long()));
            let first = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("gt"),
                long(),
                vec![
                    Expression::Column(one_long_col_schema().columns[0].clone()),
                    integer(0),
                ],
            ));
            let overflow = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                long(),
                vec![integer(i64::MAX), integer(1)],
            ));
            let mut filters = vec![first, overflow];
            if row_mode {
                filters.push(Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("getvar_int"),
                    long(),
                    vec![Expression::Constant(Constant::new(
                        Datum::Bytes(b"v".to_vec()),
                        string(),
                    ))],
                )));
            }
            let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
            let mut selection = SelectionExec::new(
                ExecutorMeta::new(one_long_col_schema(), 1, 2, 2),
                filters,
                Box::new(source),
                ctx.clone(),
                ctx.statement_memory(),
            );
            assert_eq!(selection.batched, !row_mode);
            selection.open().unwrap();
            let mut req = selection.new_chunk();
            selection.next(&mut req).unwrap();
            assert_eq!(req.num_rows(), 0);
            assert_eq!(ctx.tikv_expression_rows(), if engine { 2 } else { 0 });
            // Only the first predicate is demanded, so overflow never compiles.
            assert_eq!(selection.filters.tikv_compilations(), u64::from(engine));
            assert!(ctx.take_warnings().is_empty());
            selection.close().unwrap();
            assert_eq!(ctx.statement_memory().bytes_consumed(), 0);
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn selection_engine_row_mode_preserves_side_effect_demand() {
        for engine in [false, true] {
            let mut data = Chunk::new_with_capacity(&[long()], 4);
            data.append_int64(0, 0);
            data.append_null(0);
            data.append_int64(0, 1);
            data.append_int64(0, 2);
            let source = OneChunkSource {
                meta: ExecutorMeta::new(one_long_col_schema(), 0, 4, 4),
                data: Some(data),
            };
            let column = Expression::Column(one_long_col_schema().columns[0].clone());
            let first = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("gt"),
                long(),
                vec![
                    column,
                    Expression::Constant(Constant::new(Datum::Int(0), long())),
                ],
            ));
            let variable_name =
                Expression::Constant(Constant::new(Datum::Bytes(b"v".to_vec()), string()));
            let previous = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("getvar_int"),
                long(),
                vec![variable_name.clone()],
            ));
            let increment = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                long(),
                vec![
                    previous,
                    Expression::Constant(Constant::new(Datum::Int(1), long())),
                ],
            ));
            let assignment = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("setvar"),
                long(),
                vec![variable_name, increment],
            ));
            let ctx = crate::StmtContext::for_query()
                .with_tikv_expression(engine)
                .with_user_vars(Arc::new(std::sync::Mutex::new(HashMap::new())));
            ctx.set_uservar("v", Datum::Int(0));
            let mut selection = SelectionExec::new(
                ExecutorMeta::new(one_long_col_schema(), 1, 4, 4),
                vec![first, assignment],
                Box::new(source),
                ctx.clone(),
                ctx.statement_memory(),
            );
            assert!(!selection.batched);
            selection.open().unwrap();
            assert_eq!(ctx.get_uservar("v"), Some(Datum::Int(0)));
            let mut req = selection.new_chunk();
            for expected in [1, 2] {
                selection.next(&mut req).unwrap();
                assert_eq!(req.num_rows(), 1);
                assert_eq!(req.get_row(0).get_int64(0), expected);
                assert_eq!(ctx.get_uservar("v"), Some(Datum::Int(expected)));
                // Pure predicates enter the engine; setvar retains the native
                // facade fallback and never runs ahead to the next output row.
                assert_eq!(
                    ctx.tikv_expression_rows(),
                    if engine { expected as u64 + 2 } else { 0 }
                );
                assert_eq!(selection.filters.tikv_compilations(), u64::from(engine));
            }
            selection.next(&mut req).unwrap();
            assert_eq!(req.num_rows(), 0);
            assert_eq!(ctx.get_uservar("v"), Some(Datum::Int(2)));
            assert_eq!(ctx.tikv_expression_rows(), if engine { 4 } else { 0 });
            selection.close().unwrap();
            assert_eq!(ctx.statement_memory().bytes_consumed(), 0);
        }
    }

    #[test]
    fn selection_keeps_rows_passing_predicate() {
        // Source rows: col0 in {1, 2, 3}.
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 3);
        for v in [1, 2, 3] {
            data.append_int64(0, v);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 3, 1024),
            data: Some(data),
        };

        // Filter: col0 > 1 (gt(col0, 1)).
        let mut col = Column::new(1, long());
        col.index = 0;
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(col),
                Expression::Constant(Constant::new(Datum::Int(1), long())),
            ],
        ));

        let mut sel = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 3, 1024),
            vec![filter],
            Box::new(source),
            NoColumns,
            StatementMemory::default(),
        );

        sel.open().unwrap();
        let mut req = sel.new_chunk();
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 2);
        assert_eq!(req.get_row(0).get_int64(0), 2);
        assert_eq!(req.get_row(1).get_int64(0), 3);

        // Exhausted.
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        sel.close().unwrap();
    }

    #[test]
    fn null_and_false_filters_reject_rows() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 2);
        data.append_int64(0, 5);
        data.append_null(0); // col0 IS NULL -> gt is NULL -> rejected
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 2, 1024),
            data: Some(data),
        };
        let mut col = Column::new(1, long());
        col.index = 0;
        // col0 > 10  -> false for 5, NULL for the null row: both rejected.
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(col),
                Expression::Constant(Constant::new(Datum::Int(10), long())),
            ],
        ));
        let mut sel = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 2, 1024),
            vec![filter],
            Box::new(source),
            NoColumns,
            StatementMemory::default(),
        );
        sel.open().unwrap();
        let mut req = sel.new_chunk();
        sel.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }

    #[test]
    fn selection_preserves_filtered_rows_across_requested_batches() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 8);
        for value in 1..=8 {
            data.append_int64(0, value);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 8, 8),
            data: Some(data),
        };
        let mut column = Column::new(1, long());
        column.index = 0;
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(0), long())),
            ],
        ));
        let mut selection = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 3, 3),
            vec![filter],
            Box::new(source),
            NoColumns,
            StatementMemory::default(),
        );

        selection.open().unwrap();
        let mut req = selection.new_chunk();
        req.set_required_rows(2, 3);
        let mut batches = Vec::new();
        loop {
            selection.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            batches.push(
                (0..req.num_rows())
                    .map(|row| req.get_row(row).get_int64(0))
                    .collect::<Vec<_>>(),
            );
        }

        assert_eq!(
            batches,
            vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]
        );
    }

    #[test]
    fn selection_accounts_cached_child_chunk_against_query_quota() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 64);
        for value in 0..64 {
            data.append_int64(0, value);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 0, 64),
            data: Some(data),
        };
        let mut column = Column::new(1, long());
        column.index = 0;
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            long(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(-1), long())),
            ],
        ));
        let memory = StatementMemory::new(200, OomAction::Cancel, 71);
        let mut selection = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 0, 64),
            vec![filter],
            Box::new(source),
            NoColumns,
            memory.clone(),
        );

        selection.open().unwrap();
        let mut req = selection.new_chunk();
        assert!(matches!(
            selection.next(&mut req),
            Err(ExecError::MemoryExceedForQuery { conn_id: 71 })
        ));
        selection.close().unwrap();
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn side_effecting_filter_returns_one_row_before_projection_observes_it() {
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&long()), 3);
        for value in 1..=3 {
            data.append_int64(0, value);
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(one_long_col_schema(), 0, 3, 3),
            data: Some(data),
        };
        let mut column = Column::new(1, long());
        column.index = 0;
        let variable_name =
            Expression::Constant(Constant::new(Datum::Bytes(b"v".to_vec()), string()));
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            long(),
            vec![variable_name, Expression::Column(column)],
        ));
        let mut selection = SelectionExec::new(
            ExecutorMeta::new(one_long_col_schema(), 1, 3, 3),
            vec![filter],
            Box::new(source),
            UserVariables::default(),
            StatementMemory::default(),
        );

        selection.open().unwrap();
        let mut req = selection.new_chunk();
        for expected in 1..=3 {
            selection.next(&mut req).unwrap();
            assert_eq!(req.num_rows(), 1);
            assert_eq!(req.get_row(0).get_int64(0), expected);
            assert_eq!(selection.ctx.get_uservar("v"), Some(Datum::Int(expected)));
        }
        selection.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }
}
