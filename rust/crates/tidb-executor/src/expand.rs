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

//! Serial Go ExpandExec: evaluate every grouping level over one cached chunk.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::StatementMemory;
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::Tracker;

/// Projects a cached input batch once per grouping level before advancing.
pub struct ExpandExec<C: Columns> {
    meta: ExecutorMeta,
    levels: Vec<EvaluatorSuite>,
    child: Box<dyn Executor>,
    ctx: C,
    input: Chunk,
    level: usize,
    done: bool,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
}

impl<C: Columns> ExpandExec<C> {
    /// Builds the level evaluators without swapping columns out of their input.
    pub fn new(
        meta: ExecutorMeta,
        levels: Vec<Vec<Expression>>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let input = child.new_chunk();
        let tracker = memory.operator_tracker(meta.id());
        Self {
            meta,
            levels: levels
                .into_iter()
                .map(|exprs| EvaluatorSuite::new(exprs, true))
                .collect(),
            child,
            ctx,
            input,
            level: usize::MAX,
            done: false,
            memory,
            tracker,
        }
    }
}

impl<C: Columns + Send> Executor for ExpandExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.input.reset();
        self.level = usize::MAX;
        self.done = false;
        self.tracker.replace_bytes_used(self.input.memory_usage());
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.grow_and_reset(self.max_chunk_size());
        if self.done || self.levels.is_empty() {
            return Ok(());
        }
        self.memory.check()?;
        if self.level >= self.levels.len() {
            self.input
                .set_required_rows(req.required_rows() as isize, self.meta.max_chunk_size());
            self.child.next(&mut self.input)?;
            self.tracker.replace_bytes_used(self.input.memory_usage());
            self.memory.check()?;
            if self.input.num_rows() == 0 {
                self.done = true;
                return Ok(());
            }
            self.level = 0;
        }
        if self.meta.schema().is_empty() {
            req.set_num_virtual_rows(self.input.num_rows());
        } else {
            self.levels[self.level]
                .run_with_tikv_batch(
                    &self.ctx,
                    &mut self.input,
                    req,
                    self.child.ret_field_types(),
                )
                .map_err(|error| match error {
                    EvaluatorError::Eval(error) => ExecError::Eval(error),
                    EvaluatorError::Chunk(message) => ExecError::internal(message),
                })?;
        }
        self.level += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.input = Chunk::new_empty(self.child.ret_field_types());
        self.tracker.replace_bytes_used(0);
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
}

impl<C: Columns> Drop for ExpandExec<C> {
    fn drop(&mut self) {
        self.tracker.replace_bytes_used(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldTypeCode};
    use tidb_expr::{column::Column, constant::Constant};

    #[test]
    fn zero_width_levels_preserve_virtual_rows_and_empty_input() {
        for rows in [0, 1] {
            let ctx = crate::StmtContext::for_query();
            let child = crate::table_dual::TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 1, 2, 2),
                rows,
            );
            let mut expand = ExpandExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 2, 2, 2),
                vec![vec![], vec![]],
                Box::new(child),
                ctx.clone(),
                ctx.statement_memory(),
            );
            let mut out = expand.new_chunk();
            expand.open().unwrap();
            for expected in [rows, rows, 0, 0] {
                expand.next(&mut out).unwrap();
                assert_eq!(out.num_rows(), expected);
            }
            expand.close().unwrap();
            assert_eq!(expand.tracker.bytes_consumed(), 0);
        }
    }

    #[test]
    fn cached_input_obeys_statement_memory_quota() {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let schema = Schema::new(vec![Column::new(1, ty.clone())]);
        let ctx = crate::StmtContext::for_query().with_mem_quota(1, crate::OomAction::Cancel);
        let source = Batches {
            meta: ExecutorMeta::new(schema.clone(), 1, 2, 2),
            batch: 0,
        };
        let mut expand = ExpandExec::new(
            ExecutorMeta::new(schema, 2, 2, 2),
            vec![vec![Expression::Constant(Constant::new(Datum::Null, ty))]],
            Box::new(source),
            ctx.clone(),
            ctx.statement_memory(),
        );
        expand.open().unwrap();
        let mut out = expand.new_chunk();
        assert!(matches!(
            expand.next(&mut out),
            Err(ExecError::MemoryExceedForQuery { .. })
        ));
        expand.close().unwrap();
        assert_eq!(expand.tracker.bytes_consumed(), 0);
    }

    struct Batches {
        meta: ExecutorMeta,
        batch: i64,
    }
    impl Executor for Batches {
        fn open(&mut self) -> Result<(), ExecError> {
            self.batch = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if self.batch < 2 {
                req.append_int64(0, self.batch * 10 + 1);
                req.append_int64(0, self.batch * 10 + 2);
                self.batch += 1;
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
            2
        }
        fn max_chunk_size(&self) -> usize {
            2
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    #[test]
    fn every_level_preserves_input_across_batches_and_reopen() {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        let schema = Schema::new(vec![column.clone()]);
        let source = Batches {
            meta: ExecutorMeta::new(schema.clone(), 1, 2, 2),
            batch: 0,
        };
        let ctx = crate::StmtContext::for_query();
        let mut expand = ExpandExec::new(
            ExecutorMeta::new(schema, 2, 2, 2),
            vec![
                vec![Expression::Constant(Constant::new(Datum::Null, ty.clone()))],
                vec![Expression::Column(column)],
            ],
            Box::new(source),
            ctx.clone(),
            ctx.statement_memory(),
        );
        let mut out = expand.new_chunk();
        for _ in 0..2 {
            expand.open().unwrap();
            for expected in [
                vec![Datum::Null, Datum::Null],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Null, Datum::Null],
                vec![Datum::Int(11), Datum::Int(12)],
            ] {
                expand.next(&mut out).unwrap();
                let actual: Vec<_> = (0..out.num_rows())
                    .map(|i| out.get_row(i).get_datum(0, &ty))
                    .collect();
                assert_eq!(actual, expected);
            }
            expand.next(&mut out).unwrap();
            assert_eq!(out.num_rows(), 0);
            expand.close().unwrap();
        }
    }
    struct SelectedBatches {
        meta: ExecutorMeta,
        batch: usize,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Executor for SelectedBatches {
        fn open(&mut self) -> Result<(), ExecError> {
            self.batch = 0;
            Ok(())
        }
        fn next(&mut self, out: &mut Chunk) -> Result<(), ExecError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            out.reset();
            if self.batch < 2 {
                out.append_int64(0, self.batch as i64 * 10 + 1);
                out.append_int64(0, self.batch as i64 * 10 + 2);
                if self.batch == 0 {
                    out.set_sel(Some(vec![1, 0, 1]));
                }
                self.batch += 1;
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
            3
        }
        fn max_chunk_size(&self) -> usize {
            3
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }
    fn mixed_level_matrix(require_native: bool) -> (usize, u64) {
        use tidb_ast::CiString;
        use tidb_expr::{
            constant::ParamMarker, scalar_function::ScalarFunction, BlockEncryptionMode as Mode,
        };
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let real = FieldType::new(FieldTypeCode::Double).with_decimal(-1);
        let string = FieldType::new(FieldTypeCode::VarString);
        let call = |name: &str, field, args| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let literal = |n| Expression::Constant(Constant::new(Datum::Int(n), integer.clone()));
        let text =
            |s: &str| Expression::Constant(Constant::new(Datum::new_string(s), string.clone()));
        let column = || {
            let mut c = Column::new(1, integer.clone());
            c.index = 0;
            Expression::Column(c)
        };
        let parameter = || {
            let mut c = Constant::new(Datum::Int(99), integer.clone());
            c.param_marker = Some(ParamMarker { order: 0 });
            Expression::Constant(c)
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for enabled in [false, true] {
            for mode in [Mode::Aes128Ecb, Mode::Aes192Cbc] {
                for ordered in [false, true] {
                    let mut levels = vec![];
                    for level in 0..2 {
                        let mut roots = vec![
                            if level == 0 {
                                Expression::Constant(Constant::new(Datum::Null, integer.clone()))
                            } else {
                                column()
                            },
                            literal(level),
                            call("rand", real.clone(), vec![literal(7)]),
                            call("rand", real.clone(), vec![column()]),
                            call(
                                "aes_encrypt",
                                string.clone(),
                                vec![column(), text("key"), text("1234567890123456")],
                            ),
                            parameter(),
                            call(
                                "json_schema_valid",
                                integer.clone(),
                                vec![text(r#"{"type":"integer"}"#), column()],
                            ),
                        ];
                        if ordered {
                            let get =
                                || call("getvar_int", integer.clone(), vec![text("n20-counter")]);
                            let value = call(
                                "plus",
                                integer.clone(),
                                vec![
                                    call("ifnull", integer.clone(), vec![get(), literal(0)]),
                                    column(),
                                ],
                            );
                            roots.push(call(
                                "setvar",
                                integer.clone(),
                                vec![text("n20-counter"), value],
                            ));
                            roots.push(get());
                        }
                        levels.push(roots);
                    }
                    let fields = levels[0]
                        .iter()
                        .map(|e| e.static_type().unwrap().clone())
                        .collect::<Vec<_>>();
                    let schema = Schema::new(
                        fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| Column::new(i as i64 + 10, f.clone()))
                            .collect(),
                    );
                    let ctx = crate::StmtContext::for_query()
                        .with_enable_vectorized_expression(enabled)
                        .with_prepared_params(vec![Datum::Int(7)].into())
                        .with_block_encryption_mode(mode)
                        .with_user_vars(Arc::new(std::sync::Mutex::new(
                            std::collections::HashMap::new(),
                        )));
                    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let child = SelectedBatches {
                        meta: ExecutorMeta::new(
                            Schema::new(vec![Column::new(1, integer.clone())]),
                            1,
                            3,
                            3,
                        ),
                        batch: 0,
                        calls: calls.clone(),
                    };
                    let mut expand = ExpandExec::new(
                        ExecutorMeta::new(schema, 2, 3, 3),
                        levels,
                        Box::new(child),
                        ctx.clone(),
                        ctx.statement_memory(),
                    );
                    let mut out = expand.new_chunk();
                    for reopen in 0..2 {
                        expand.open().unwrap();
                        for step in 0..4 {
                            expand.next(&mut out).unwrap();
                            assert_eq!(out.num_rows(), if step < 2 { 3 } else { 2 });
                            for row in 0..out.num_rows() {
                                assert_eq!(out.get_row(row).get_int64(1), (step % 2) as i64);
                                if step % 2 == 0 {
                                    assert!(out.get_row(row).is_null(0));
                                }
                                for col in 0..fields.len() {
                                    let raw = out.column(col).get_raw(row).as_ref().to_vec();
                                    for b in format!("{enabled}/{mode:?}/{ordered}/{reopen}/{step}/{row}/{col}/{}/{raw:?}",out.get_row(row).is_null(col)).bytes(){hash=(hash^u64::from(b)).wrapping_mul(0x100000001b3);}
                                }
                            }
                            count += 1;
                        }
                        expand.next(&mut out).unwrap();
                        assert_eq!(out.num_rows(), 0);
                        assert_eq!(
                            calls.load(std::sync::atomic::Ordering::SeqCst),
                            (reopen + 1) * 3
                        );
                        let warnings = ctx.take_warnings();
                        assert_eq!(warnings.len(), if mode == Mode::Aes128Ecb { 10 } else { 0 });
                        for b in format!("{warnings:?}").bytes() {
                            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                        }
                        expand.close().unwrap();
                        assert_eq!(expand.tracker.bytes_consumed(), 0);
                    }
                    if ordered {
                        assert_eq!(ctx.get_uservar("n20-counter"), Some(Datum::Int(112)));
                    }
                    if require_native {
                        for suite in &expand.levels {
                            let route = suite.batch_route_snapshot();
                            assert_eq!(
                                (route.prepared, route.fallback, route.refusals),
                                (4, 0, 0),
                                "flag={enabled} mode={mode:?} ordered={ordered}"
                            );
                        }
                    }
                }
            }
        }
        (count, hash)
    }
    #[test]
    fn original_mixed_levels_keep_selected_input_context_and_reopen() {
        let (count, hash) = mixed_level_matrix(false);
        eprintln!("EXPAND ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (64, 0xB42FB8BE316E6C79));
    }
    #[test]
    fn mixed_levels_execute_through_the_shared_prepared_frame() {
        assert_eq!(mixed_level_matrix(true), (64, 0xB42FB8BE316E6C79));
    }
    #[test]
    fn late_level_error_keeps_cached_input_level_and_original_column_prefix() {
        use tidb_ast::CiString;
        use tidb_expr::{scalar_function::ScalarFunction, EvalError};
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let column = || {
            let mut c = Column::new(1, integer.clone());
            c.index = 0;
            Expression::Column(c)
        };
        let literal = |n| Expression::Constant(Constant::new(Datum::Int(n), integer.clone()));
        let call = |name: &str, args| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                integer.clone(),
                args,
            ))
        };
        for enabled in [false, true] {
            let ctx = crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
            let child = Batches {
                meta: ExecutorMeta::new(
                    Schema::new(vec![Column::new(1, integer.clone())]),
                    1,
                    2,
                    2,
                ),
                batch: 0,
            };
            let levels = vec![
                vec![column(), literal(0)],
                vec![
                    column(),
                    call(
                        "if",
                        vec![
                            call("lt", vec![column(), literal(10)]),
                            literal(1),
                            call("tidb_is_ddl_owner", vec![]),
                        ],
                    ),
                ],
            ];
            let reference = EvaluatorSuite::new(levels[1].clone(), true);
            let mut original_input = Chunk::new_with_capacity(std::slice::from_ref(&integer), 2);
            original_input.append_int64(0, 11);
            original_input.append_int64(0, 12);
            let mut expected = Chunk::new_with_capacity(&[integer.clone(), integer.clone()], 2);
            let original = reference
                .run(&ctx, &mut original_input, &mut expected)
                .unwrap_err();
            let schema = Schema::new(vec![
                Column::new(1, integer.clone()),
                Column::new(2, integer.clone()),
            ]);
            let mut expand = ExpandExec::new(
                ExecutorMeta::new(schema, 2, 2, 2),
                levels,
                Box::new(child),
                ctx.clone(),
                ctx.statement_memory(),
            );
            expand.open().unwrap();
            let mut out = expand.new_chunk();
            for _ in 0..3 {
                expand.next(&mut out).unwrap();
            }
            for _ in 0..2 {
                let error = expand.next(&mut out).unwrap_err();
                assert!(
                    matches!(error,ExecError::Eval(EvalError::Unsupported(message))if message.contains("OptPropDDLOwnerInfo"))
                );
                assert!(format!("{original:?}").contains("OptPropDDLOwnerInfo"));
                assert_eq!(out.column(0), expected.column(0));
                assert_eq!(out.column(1), expected.column(1));
                assert_eq!(expand.level, 1);
                assert_eq!(expand.input.get_row(0).get_int64(0), 11);
                assert_eq!(expand.input.get_row(1).get_int64(0), 12);
                let route = expand.levels[1].batch_route_snapshot();
                assert_eq!((route.fallback, route.refusals), (0, 0));
                assert!(route.prepared > 0);
            }
            expand.close().unwrap();
            assert_eq!(expand.tracker.bytes_consumed(), 0);
        }
    }
}
