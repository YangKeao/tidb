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

//! `pkg/executor` `ProjectionExec`: evaluates a list of expressions over each
//! input row to form the output rows.
//!
//! This is the serial path: one child batch per `Next`, each input row producing
//! one output row. Go's parallel projection (worker pool) is deferred.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorProgram, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// Marks an evaluation context [`ProjectionExec`] accepts.
///
/// Go hands its session context to every projection worker goroutine; a Rust
/// context and shared worker program must be `Send + Sync`, including when
/// the whole projection moves with a join's build child. The bridge also
/// supplies the clone/lifetime proof needed by persistent projection workers.
pub trait ProjectionContext: Columns + Send + Sync {
    /// One parallel `Next`, or `None` when this context cannot share
    /// evaluation across threads.
    fn parallel_next_bridge(
        exec: &mut ProjectionExec<Self>,
        req: &mut Chunk,
    ) -> Option<Result<(), ExecError>>
    where
        Self: Sized,
    {
        let _ = (exec, req);
        None
    }
}

impl ProjectionContext for tidb_expr::NoColumns {
    fn parallel_next_bridge(
        exec: &mut ProjectionExec<Self>,
        req: &mut Chunk,
    ) -> Option<Result<(), ExecError>> {
        Some(exec.parallel_next(req))
    }
}

impl ProjectionContext for crate::StmtContext {
    /// The production statement context shares every interior-mutable handle
    /// through `Arc` + `Mutex`/atomics, so worker threads may evaluate
    /// expressions through `&StmtContext` (the same proof the hash join's
    /// probe workers and the aggregate's partial lanes rely on).
    fn parallel_next_bridge(
        exec: &mut ProjectionExec<Self>,
        req: &mut Chunk,
    ) -> Option<Result<(), ExecError>> {
        Some(exec.parallel_next(req))
    }
}

/// The `'static` snapshot every projection worker task borrows: Go's workers
/// share the executor's `evaluatorSuit` and its evaluation context.
struct ParallelProjectionShared<C> {
    program: Arc<EvaluatorProgram>,
    input_schema: Arc<[FieldType]>,
    ctx: C,
}

/// Go `projectionInputFetcher` + `projectionWorker`s, driven from the session
/// thread: the parent's `Next` fetches a child chunk into a free input chunk,
/// hands the pair to a pool worker, and releases finished outputs in fetch
/// order. `numWorkers` input and output chunks circulate, so the fetch runs at
/// most that far ahead of the parent, as Go's `inputCh`/`outputCh` bound it.
struct ParallelProjection<C> {
    shared: Arc<ParallelProjectionShared<C>>,
    result_tx: Sender<(u64, Result<(Chunk, Chunk, EvaluatorSuite), ExecError>)>,
    result_rx: Receiver<(u64, Result<(Chunk, Chunk, EvaluatorSuite), ExecError>)>,
    /// Finished lanes waiting for their turn (`output.done` in fetch order).
    reorder: BTreeMap<u64, Result<(Chunk, Chunk, EvaluatorSuite), ExecError>>,
    /// Outputs released in order, not yet handed to the parent.
    ready: VecDeque<Chunk>,
    free_inputs: Vec<Chunk>,
    free_outputs: Vec<Chunk>,
    free_suites: Vec<EvaluatorSuite>,
    inputs_allocated: usize,
    outputs_allocated: usize,
    suites_allocated: usize,
    next_seq: u64,
    next_release: u64,
    in_flight: usize,
    child_done: bool,
}

/// One execution of a physical projection, with its own evaluator and buffer.
pub struct ProjectionExec<C: Columns> {
    meta: ExecutorMeta,
    evaluator_suite: EvaluatorSuite,
    program: Arc<EvaluatorProgram>,
    input_schema: Arc<[FieldType]>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    /// Go `numWorkers`: the projection workers, zero for the serial path.
    num_workers: usize,
    parallel: Option<ParallelProjection<C>>,
}

impl<C: Columns> ProjectionExec<C> {
    /// Builds a projection of `exprs` over `child`, evaluated with `ctx`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        exprs: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        Self::with_column_evaluator(meta, exprs, child, ctx, false)
    }

    /// Go `newProjectionExec`: the physical plan controls column swapping.
    #[must_use]
    pub fn with_column_evaluator(
        meta: ExecutorMeta,
        exprs: Vec<Expression>,
        child: Box<dyn Executor>,
        ctx: C,
        avoid_column_evaluator: bool,
    ) -> Self {
        let input_schema: Arc<[FieldType]> = child
            .schema()
            .columns
            .iter()
            .map(|column| {
                column
                    .ret_type
                    .clone()
                    .expect("projection child schema column has a type")
            })
            .collect::<Vec<_>>()
            .into();
        let child_chunk = child.new_chunk();
        let program = Arc::new(EvaluatorProgram::new(exprs, avoid_column_evaluator));
        let evaluator_suite = EvaluatorSuite::from_program(Arc::clone(&program));
        ProjectionExec {
            meta,
            evaluator_suite,
            program,
            input_schema,
            child,
            ctx,
            child_chunk,
            num_workers: 0,
            parallel: None,
        }
    }

    /// Go `newProjectionExec`'s `numWorkers`: the builder passes
    /// `tidb_projection_concurrency` workers, or zero for the serial path.
    #[must_use]
    pub fn with_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    fn evaluator_error(error: EvaluatorError) -> ExecError {
        match error {
            EvaluatorError::Eval(error) => ExecError::Eval(error),
            EvaluatorError::Chunk(message) => ExecError::internal(message),
        }
    }
}

impl<C: Columns + Clone + Send + Sync + 'static> ProjectionExec<C> {
    /// Go `parallelExecute`: the next finished output in fetch order, fetching
    /// and dispatching further child chunks while a free input and output
    /// chunk exist (`projectionInputFetcher.run`), else waiting on a worker.
    fn parallel_next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let max_chunk_size = self.meta.max_chunk_size();
        req.grow_and_reset(max_chunk_size);
        if self.parallel.is_none() {
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            self.parallel = Some(ParallelProjection {
                shared: Arc::new(ParallelProjectionShared {
                    program: Arc::clone(&self.program),
                    input_schema: Arc::clone(&self.input_schema),
                    ctx: self.ctx.clone(),
                }),
                result_tx,
                result_rx,
                reorder: BTreeMap::new(),
                ready: VecDeque::new(),
                free_inputs: Vec::new(),
                free_outputs: Vec::new(),
                free_suites: Vec::new(),
                inputs_allocated: 0,
                outputs_allocated: 0,
                suites_allocated: 0,
                next_seq: 0,
                next_release: 0,
                in_flight: 0,
                child_done: false,
            });
        }
        loop {
            self.collect_parallel_results(false)?;
            let pipeline = self
                .parallel
                .as_mut()
                .expect("the parallel projection is created above");
            if let Some(mut output) = pipeline.ready.pop_front() {
                // Go `chk.SwapColumns(output.chk); e.fetcher.outputCh <- output`.
                req.swap_columns(&mut output);
                output.reset();
                pipeline.free_outputs.push(output);
                return Ok(());
            }
            if !pipeline.child_done {
                if let Some((input, output, suite)) = self.take_parallel_chunks() {
                    self.fetch_and_dispatch_parallel(input, output, suite, req.required_rows())?;
                    continue;
                }
            }
            let pipeline = self
                .parallel
                .as_ref()
                .expect("the parallel projection is created above");
            if pipeline.in_flight > 0 {
                self.collect_parallel_results(true)?;
                continue;
            }
            // The child is drained and every worker has reported: EOF.
            return Ok(());
        }
    }

    /// A free input and a free output chunk, or `None` when Go's fetcher would
    /// block on `inputCh`/`outputCh`: all `numWorkers` of either are with a
    /// worker or waiting for the parent.
    fn take_parallel_chunks(&mut self) -> Option<(Chunk, Chunk, EvaluatorSuite)> {
        let num_workers = self.num_workers.max(1);
        let pipeline = self.parallel.as_mut()?;
        let input = match pipeline.free_inputs.pop() {
            Some(input) => input,
            None if pipeline.inputs_allocated < num_workers => {
                pipeline.inputs_allocated += 1;
                self.child.new_chunk()
            }
            None => return None,
        };
        let output = match pipeline.free_outputs.pop() {
            Some(output) => output,
            None if pipeline.outputs_allocated < num_workers => {
                pipeline.outputs_allocated += 1;
                self.meta.new_chunk()
            }
            None => {
                pipeline.free_inputs.push(input);
                return None;
            }
        };
        let suite = match pipeline.free_suites.pop() {
            Some(suite) => suite,
            None if pipeline.suites_allocated < num_workers => {
                pipeline.suites_allocated += 1;
                EvaluatorSuite::from_program(Arc::clone(&pipeline.shared.program))
            }
            None => {
                pipeline.free_inputs.push(input);
                pipeline.free_outputs.push(output);
                return None;
            }
        };
        Some((input, output, suite))
    }

    /// Go `projectionInputFetcher.run` for one chunk: pull it from the child
    /// with the parent's required rows, end the fetch on an empty chunk,
    /// otherwise hand the pair to a pool worker (`projectionWorker.run`).
    fn fetch_and_dispatch_parallel(
        &mut self,
        mut input: Chunk,
        output: Chunk,
        suite: EvaluatorSuite,
        required_rows: usize,
    ) -> Result<(), ExecError> {
        let max_chunk_size = self.meta.max_chunk_size();
        input.reset();
        input.set_required_rows(
            isize::try_from(required_rows).unwrap_or(isize::MAX),
            max_chunk_size,
        );
        self.child.next(&mut input)?;
        let pipeline = self
            .parallel
            .as_mut()
            .expect("the parallel projection is created before a fetch");
        if input.num_rows() == 0 {
            pipeline.child_done = true;
            pipeline.free_inputs.push(input);
            pipeline.free_outputs.push(output);
            pipeline.free_suites.push(suite);
            return Ok(());
        }
        let seq = pipeline.next_seq;
        pipeline.next_seq += 1;
        pipeline.in_flight += 1;
        let shared = Arc::clone(&pipeline.shared);
        let result_tx = pipeline.result_tx.clone();
        crate::worker_pool::enqueue_public(Box::new(move || {
            let mut input = input;
            let mut output = output;
            let result = crate::sort_util::recover_worker_panic(|| {
                suite
                    .run_with_tikv_batch(&shared.ctx, &mut input, &mut output, &shared.input_schema)
                    .map(|()| (input, output, suite))
                    .map_err(Self::evaluator_error)
            });
            // A dropped receiver means the projection is already closed.
            let _ = result_tx.send((seq, result));
        }));
        Ok(())
    }

    /// Takes every finished pair (one of them blocking when asked), then
    /// releases outputs in fetch order and returns their inputs to the pool.
    fn collect_parallel_results(&mut self, block: bool) -> Result<(), ExecError> {
        let Some(pipeline) = self.parallel.as_mut() else {
            return Ok(());
        };
        let mut block = block && pipeline.in_flight > 0;
        loop {
            let message = if block {
                block = false;
                pipeline.result_rx.recv().map_err(|_| ())
            } else {
                pipeline.result_rx.try_recv().map_err(|_| ())
            };
            let Ok((seq, result)) = message else {
                break;
            };
            pipeline.in_flight = pipeline.in_flight.saturating_sub(1);
            pipeline.reorder.insert(seq, result);
        }
        while let Some(result) = pipeline.reorder.remove(&pipeline.next_release) {
            pipeline.next_release += 1;
            let (mut input, output, suite) = result?;
            input.reset();
            pipeline.free_inputs.push(input);
            pipeline.free_suites.push(suite);
            pipeline.ready.push_back(output);
        }
        Ok(())
    }
}

impl<C: ProjectionContext> Executor for ProjectionExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.parallel = None;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        // Go `isUnparallelExec`: `numWorkers <= 0` runs on the caller. A
        // zero-column projection carries only a virtual row count, which the
        // serial path below preserves.
        if self.num_workers > 0 && !self.meta.ret_field_types().is_empty() {
            if let Some(result) = C::parallel_next_bridge(self, req) {
                return result;
            }
        }
        let max_chunk_size = self.max_chunk_size();
        // Go calls GrowAndReset before reading RequiredRows. Growing restores
        // the maximum demand, while an ordinary reset preserves the parent's
        // current request.
        req.grow_and_reset(max_chunk_size);
        let required_rows = isize::try_from(req.required_rows()).unwrap_or(isize::MAX);
        self.child_chunk
            .set_required_rows(required_rows, max_chunk_size);
        self.child.next(&mut self.child_chunk)?;
        let child_rows = self.child_chunk.num_rows();
        if child_rows == 0 {
            return Ok(());
        }
        // Go's chunk keeps an explicit virtual-row count when its schema has
        // no columns. Column pruning can legitimately reduce a Projection to
        // zero expressions below COUNT(*); it still preserves one output row
        // per child row. The evaluator has no column on which to record that
        // cardinality, so carry it explicitly instead of turning the batch
        // into an EOF marker.
        if self.meta.ret_field_types().is_empty() {
            req.set_num_virtual_rows(child_rows);
            return Ok(());
        }
        self.evaluator_suite
            .run_with_tikv_batch(&self.ctx, &mut self.child_chunk, req, &self.input_schema)
            .map_err(Self::evaluator_error)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        // Go closes `finishCh` and waits for the fetcher and workers; a task
        // still running here finds its receiver gone and drops its chunks.
        self.parallel = None;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_dual::TableDualExec;
    use std::sync::Arc;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::evaluator::EvaluatorProgram;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn longlong() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn int_const(v: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(v), long()))
    }

    fn parameter(order: i64) -> Expression {
        let mut constant = Constant::new(Datum::Int(99), long());
        constant.param_marker = Some(tidb_expr::constant::ParamMarker { order });
        Expression::Constant(constant)
    }

    #[test]
    fn prepared_filter_and_projection_use_fresh_execution_state() {
        let predicate = parameter(0);
        let projection = parameter(1);
        for (condition, value, rows) in [
            (Datum::Null, 2, 0),
            (Datum::Int(1), 7, 1),
            (Datum::Int(0), 3, 0),
            (Datum::Int(-1), 9, 1),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_prepared_params(vec![condition, Datum::Int(value)].into());
            let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
            let selection = crate::selection::SelectionExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 1, 1, 1024),
                vec![predicate.clone()],
                Box::new(dual),
                ctx.clone(),
                ctx.statement_memory(),
            );
            let mut executor = ProjectionExec::new(
                ExecutorMeta::new(Schema::new(vec![Column::new(1, long())]), 2, 1, 1024),
                vec![projection.clone()],
                Box::new(selection),
                ctx,
            );
            executor.open().unwrap();
            let mut output = executor.new_chunk();
            executor.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), rows);
            if rows != 0 {
                assert_eq!(output.get_row(0).get_int64(0), value);
            }
            executor.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 0);
            executor.close().unwrap();
        }
    }

    #[test]
    fn prepared_program_is_shared_but_parameter_contexts_are_isolated() {
        let mut deferred = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![parameter(0), int_const(0)],
        ));
        let planning =
            crate::StmtContext::for_query().with_prepared_params(vec![Datum::Int(1)].into());
        tidb_expr::fold_constant_in_mode(
            &mut deferred,
            &planning,
            tidb_expr::ConstantFoldMode::Normal,
        );
        assert!(matches!(&deferred, Expression::Constant(c) if c.deferred_expr.is_some()));
        for expression in [parameter(0), deferred] {
            let program = Arc::new(EvaluatorProgram::new(vec![expression], false));
            let executions: Vec<_> = [Datum::Int(7), Datum::Null, Datum::Int(-3)]
                .into_iter()
                .map(|value| {
                    let program = program.clone();
                    std::thread::spawn(move || {
                        let suite = EvaluatorSuite::from_program(program);
                        let ctx = crate::StmtContext::for_query()
                            .with_prepared_params(vec![value.clone()].into());
                        let cloned = ctx.clone();
                        for _ in 0..16 {
                            let mut input = Chunk::new_empty(&[]);
                            input.set_num_virtual_rows(3);
                            let mut output = Chunk::new_with_capacity(&[long()], 3);
                            suite.run(&cloned, &mut input, &mut output).unwrap();
                            for row in 0..3 {
                                assert_eq!(output.get_row(row).get_datum(0, &long()), value);
                            }
                        }
                    })
                })
                .collect();
            for execution in executions {
                execution.join().unwrap();
            }
        }
    }

    #[test]
    fn prepared_deferred_conversion_uses_current_statement_diagnostics() {
        let target = FieldType::new(FieldTypeCode::Tiny);
        let mut constant = Constant::new(Datum::Int(99), target);
        constant.deferred_expr = Some(Box::new(parameter(0)));
        let expression = Expression::Constant(constant);
        for (level, input, expected, warning_count) in [
            (
                tidb_expr::ErrorLevel::Warn,
                "12tail",
                Some(Datum::Int(12)),
                1,
            ),
            (tidb_expr::ErrorLevel::Error, "12tail", None, 0),
            (
                tidb_expr::ErrorLevel::Ignore,
                "12tail",
                Some(Datum::Int(12)),
                0,
            ),
            (tidb_expr::ErrorLevel::Warn, "128tail", None, 1),
            (tidb_expr::ErrorLevel::Warn, "7", Some(Datum::Int(7)), 0),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_truncate_level(level)
                .with_prepared_params(vec![Datum::new_string(input)].into());
            let result = tidb_expr::eval_expression_once(&expression, &ctx);
            if let Some(expected) = expected {
                assert_eq!(result.unwrap(), expected, "{level:?} / {input}");
            } else {
                let error =
                    crate::DriverError::from(ExecError::Eval(result.unwrap_err())).to_mysql_error();
                let (code, message) = if input == "128tail" {
                    (1690, "constant 128 overflows tinyint")
                } else {
                    (1292, "Truncated incorrect DOUBLE value: '12tail'")
                };
                assert_eq!(error.code, code);
                assert_eq!(error.message, message);
                assert_eq!(
                    error.state,
                    if code == 1690 { *b"22003" } else { *b"22007" }
                );
            }
            let warnings = ctx.take_warnings();
            assert_eq!(warnings.len(), warning_count, "{level:?} / {input}");
            if warning_count != 0 {
                assert_eq!(warnings[0].1, 1292);
                assert_eq!(
                    warnings[0].2,
                    format!("Truncated incorrect DOUBLE value: '{input}'")
                );
            }
        }
        assert!(matches!(expression, Expression::Constant(c) if c.value == Datum::Int(99)));
    }

    #[test]
    fn prepared_lazy_folding_keeps_warnings_execution_local() {
        let division = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("intdiv"),
            long(),
            vec![int_const(1), parameter(1)],
        ));
        let original = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            long(),
            vec![parameter(0), int_const(7), division],
        ));
        let planning = crate::StmtContext::for_query()
            .with_prepared_params(vec![Datum::Int(1), Datum::Int(0)].into());
        let mut expression = original.clone();
        tidb_expr::fold_constant_in_mode(
            &mut expression,
            &planning,
            tidb_expr::ConstantFoldMode::Try,
        );
        assert!(planning.take_warnings().is_empty());
        for (condition, divisor, expected, warnings) in [
            (1, 0, Datum::Int(7), 0),
            (0, 0, Datum::Null, 1),
            (0, 1, Datum::Int(1), 0),
        ] {
            let ctx = crate::StmtContext::for_query()
                .with_prepared_params(vec![Datum::Int(condition), Datum::Int(divisor)].into());
            assert_eq!(
                tidb_expr::eval_expression_once(&expression, &ctx).unwrap(),
                expected
            );
            let actual = ctx.take_warnings();
            assert_eq!(actual.len(), warnings);
            if warnings != 0 {
                assert_eq!((actual[0].1, actual[0].2.as_str()), (1365, "Division by 0"));
            }
        }
    }

    /// `SELECT 1 + 1` executes end-to-end: a table-dual source feeds one virtual
    /// row to a projection of `plus(1, 1)`, producing a chunk holding `2`.
    #[test]
    fn select_one_plus_one_executes() {
        // Source: TableDual with an empty schema, one virtual row.
        let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);

        // Projection output schema: a single Long column.
        let out_col = Column::new(1, long());
        let proj_schema = Schema::new(vec![out_col]);
        let plus = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![int_const(1), int_const(1)],
        ));
        let mut proj = ProjectionExec::new(
            ExecutorMeta::new(proj_schema, 1, 1, 1024),
            vec![plus],
            Box::new(dual),
            NoColumns,
        );

        proj.open().unwrap();
        let mut req = proj.new_chunk();
        proj.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 1);
        assert_eq!(req.get_row(0).get_int64(0), 2);

        // Next batch is EOF (dual exhausted).
        proj.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        proj.close().unwrap();
    }

    /// Rows `(i, 2i)` for `i` in `0..total`, `chunk_rows` per chunk, noting
    /// the required rows of every `next` call.
    struct NumberSource {
        meta: ExecutorMeta,
        next: i64,
        total: i64,
        chunk_rows: usize,
        required: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl NumberSource {
        fn new(
            total: i64,
            chunk_rows: usize,
        ) -> (Box<Self>, std::sync::Arc<std::sync::Mutex<Vec<usize>>>) {
            let required = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut first = Column::new(1, longlong());
            first.index = 0;
            let mut second = Column::new(2, longlong());
            second.index = 1;
            let source = Box::new(NumberSource {
                meta: ExecutorMeta::new(Schema::new(vec![first, second]), 7, chunk_rows, 1024),
                next: 0,
                total,
                chunk_rows,
                required: std::sync::Arc::clone(&required),
            });
            (source, required)
        }
    }

    impl Executor for NumberSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.next = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            self.required.lock().unwrap().push(req.required_rows());
            let end = (self.next + self.chunk_rows as i64).min(self.total);
            while self.next < end {
                req.append_int64(0, self.next);
                req.append_int64(1, self.next * 2);
                self.next += 1;
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

    /// `col0 + col1` and `col1` over the number source: what the projection
    /// answers with `workers` workers, as rows in output order, plus the
    /// required rows its child saw.
    fn project_numbers(
        total: i64,
        chunk_rows: usize,
        workers: usize,
        parent_required_rows: usize,
    ) -> (Vec<(i64, i64)>, Vec<usize>) {
        let (source, required) = NumberSource::new(total, chunk_rows);
        let mut first = Column::new(1, longlong());
        first.index = 0;
        let mut second = Column::new(2, longlong());
        second.index = 1;
        let plus = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            longlong(),
            vec![
                Expression::Column(first),
                Expression::Column(second.clone()),
            ],
        ));
        let mut projection = ProjectionExec::new(
            ExecutorMeta::new(
                Schema::new(vec![Column::new(3, longlong()), Column::new(4, longlong())]),
                8,
                chunk_rows,
                1024,
            ),
            vec![plus, Expression::Column(second)],
            source,
            NoColumns,
        )
        .with_workers(workers);
        projection.open().unwrap();
        let mut rows = Vec::new();
        loop {
            let mut chunk = projection.new_chunk();
            chunk.set_required_rows(parent_required_rows as isize, 1024);
            projection.next(&mut chunk).unwrap();
            if chunk.num_rows() == 0 {
                break;
            }
            for row in 0..chunk.num_rows() {
                let row = chunk.get_row(row);
                rows.push((row.get_int64(0), row.get_int64(1)));
            }
        }
        projection.close().unwrap();
        let required = required.lock().unwrap().clone();
        (rows, required)
    }

    /// Go `projectionWorker`s evaluate chunks concurrently while
    /// `parallelExecute` hands them back in fetch order: the parallel answer
    /// is the serial answer, row for row.
    #[test]
    fn projection_workers_answer_the_serial_rows_in_order() {
        let expected: Vec<(i64, i64)> = (0..5_003).map(|i| (i * 3, i * 2)).collect();
        let (serial, _) = project_numbers(5_003, 128, 0, 1024);
        assert_eq!(serial, expected);
        for workers in [1, 3, 8] {
            let (parallel, _) = project_numbers(5_003, 128, workers, 1024);
            assert_eq!(parallel, expected, "workers={workers}");
        }
    }

    /// Go `TestProjectionParallelRequiredRows`: the fetcher forwards the
    /// parent's required rows (`parentReqRows`) to the child on every fetch.
    #[test]
    fn projection_workers_forward_the_parents_required_rows() {
        let (rows, required) = project_numbers(1_000, 1024, 4, 37);
        assert_eq!(rows.len(), 1_000);
        assert!(!required.is_empty());
        assert!(
            required.iter().all(|rows| *rows == 37),
            "every child fetch carries the parent's request: {required:?}"
        );
    }

    #[test]
    fn empty_projection_preserves_virtual_row_count() {
        let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
        let mut projection = ProjectionExec::new(
            ExecutorMeta::new(Schema::new(vec![]), 1, 3, 1024),
            Vec::new(),
            Box::new(dual),
            NoColumns,
        );

        projection.open().unwrap();
        let mut result = projection.new_chunk();
        projection.next(&mut result).unwrap();
        assert_eq!(result.num_rows(), 1);
        projection.close().unwrap();
    }

    /// The projection's per-row evaluation reads a child column: `col0 + 1` over
    /// an input row whose column 0 is 41 produces 42.
    #[test]
    fn projection_reads_child_column() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 41);

        let mut col = Column::new(7, long());
        col.index = 0;
        let expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![Expression::Column(col), int_const(1)],
        ));
        // Evaluate directly (the projection's inner loop) to confirm column reads.
        let row = input.get_row(0);
        let out = expr.eval(&NoColumns, row).unwrap();
        assert_eq!(out, Datum::Int(42));
    }

    #[test]
    fn concat_context_hosts_keep_packet_policy_in_serial_and_parallel_projection() {
        let field = FieldType::new(FieldTypeCode::Varchar)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json);
        let bad = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("json_type"),
            field.clone(),
            vec![Expression::Constant(Constant::new(
                Datum::Json(tidb_datatype::BinaryJSON::from_encoded_parts(0xff, [])),
                json,
            ))],
        ));
        let root = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("concat"),
            field.clone(),
            vec![
                Expression::Constant(Constant::new(
                    Datum::new_string(b"abc".to_vec()),
                    field.clone(),
                )),
                bad,
            ],
        ));
        for workers in [0, 2] {
            for strict in [false, true] {
                let context = if strict {
                    crate::StmtContext::for_dml(true, true, false)
                } else {
                    crate::StmtContext::for_query()
                }
                .with_max_allowed_packet(2);
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let result = projection.next(&mut output);
                if strict {
                    assert!(matches!(
                        result,
                        Err(ExecError::Eval(
                            tidb_expr::EvalError::AllowedPacketOverflowed(_)
                        ))
                    ));
                    assert!(context.take_warnings().is_empty());
                } else {
                    result.unwrap();
                    assert_eq!(output.num_rows(), 1);
                    assert!(output.get_row(0).is_null(0));
                    let warnings = context.take_warnings();
                    assert_eq!(warnings.len(), 1);
                    assert_eq!(warnings[0].1, 1301);
                }
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }

    #[test]
    fn native_value_char_cast_keeps_stmt_warnings_in_serial_and_parallel_projection() {
        let source = FieldType::new(FieldTypeCode::VarString)
            .with_collation(tidb_datatype::Collation::Binary);
        let target = FieldType::new(FieldTypeCode::VarString)
            .with_flen(0)
            .with_collation(tidb_datatype::Collation::GbkBin);
        let root = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("cast_char"),
            target.clone(),
            vec![Expression::Constant(Constant::new(
                Datum::Bytes(vec![b'a', 0xff, 0x81, 0x30]),
                source,
            ))],
        ));
        let reference = crate::StmtContext::for_query();
        let expected = root
            .clone()
            .eval(&reference, tidb_chunk::row::Row::empty())
            .unwrap();
        let expected_warnings = reference.take_warnings();
        assert_eq!(
            expected_warnings
                .iter()
                .map(|warning| warning.1)
                .collect::<Vec<_>>(),
            vec![3854, 1406]
        );
        for workers in [0, 2] {
            let context = crate::StmtContext::for_query();
            let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
            let mut projection = ProjectionExec::new(
                ExecutorMeta::new(
                    Schema::new(vec![Column::new(1, target.clone())]),
                    1,
                    1,
                    1024,
                ),
                vec![root.clone()],
                Box::new(dual),
                context.clone(),
            )
            .with_workers(workers);
            projection.open().unwrap();
            let mut output = projection.new_chunk();
            projection.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 1);
            assert_eq!(
                output.get_row(0).get_string(0).as_bytes(),
                expected.sql_string().unwrap().as_bytes()
            );
            assert_eq!(context.take_warnings(), expected_warnings);
            if workers == 0 {
                let route = projection.evaluator_suite.batch_route_snapshot();
                assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
            }
            projection.close().unwrap();
        }
    }
    #[test]
    fn native_literal_results_preserve_storage_in_serial_and_parallel_projection() {
        let cases = [
            (FieldType::new(FieldTypeCode::Float), Datum::Float32(-0.0)),
            (
                FieldType::new(FieldTypeCode::Double),
                Datum::Real(f64::from_bits(0x7ff8000000000042)),
            ),
            (
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(20)
                    .with_decimal(4),
                Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.2300")),
            ),
            (
                FieldType::new(FieldTypeCode::Json),
                Datum::Json(tidb_datatype::BinaryJSON::parse("{\"a\":[1,null]}").unwrap()),
            ),
        ];
        for (field, value) in cases {
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let root = Expression::Constant(Constant::new(value.clone(), field.clone()));
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                projection.next(&mut output).unwrap();
                let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                expected.append_datum(0, &value);
                assert_eq!(output.num_rows(), 1);
                assert_eq!(
                    output.column(0).get_raw(0).as_ref(),
                    expected.column(0).get_raw(0).as_ref()
                );
                assert!(context.take_warnings().is_empty());
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn computed_native_casts_keep_statement_diagnostics_in_serial_and_parallel_projection() {
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let call = |name: &str, field: FieldType, value: Expression| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, vec![value]))
        };
        let signed = call(
            "cast_signed",
            integer.clone(),
            Expression::Constant(Constant::new(Datum::new_string("12.5tail"), string.clone())),
        );
        let cases = [
            signed.clone(),
            call("cast_char", string, signed.clone()),
            call(
                "cast_decimal",
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(8)
                    .with_decimal(2),
                signed,
            ),
            call(
                "cast_datetime",
                FieldType::new(FieldTypeCode::Datetime),
                Expression::Constant(Constant::new(
                    Datum::Int(2026),
                    FieldType::new(FieldTypeCode::Year),
                )),
            ),
        ];
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let value = root
                .eval(&reference, tidb_chunk::row::Row::empty())
                .unwrap();
            let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
            expected.append_datum(0, &value);
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                projection.next(&mut output).unwrap();
                assert_eq!(output.num_rows(), 1);
                assert_eq!(
                    output.column(0).get_raw(0).as_ref(),
                    expected.column(0).get_raw(0).as_ref()
                );
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_json_casts_preserve_outputs_errors_and_warnings_in_projection() {
        let source = FieldType::new(FieldTypeCode::VarString)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let cast = |value: Datum, source: FieldType| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("cast_json"),
                json.clone(),
                vec![Expression::Constant(Constant::new(value, source))],
            ))
        };
        let signed = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("cast_signed"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                Datum::new_string("12.5tail"),
                source.clone(),
            ))],
        ));
        let cases = [
            cast(Datum::new_string("{\"a\":1}"), source.clone()),
            cast(Datum::new_string("bad JSON"), source.clone()),
            cast(
                Datum::UInt(u64::MAX),
                FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
            ),
            cast(Datum::Float32(1.25), FieldType::new(FieldTypeCode::Float)),
            cast(Datum::Null, FieldType::new(FieldTypeCode::VectorFloat32)),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("cast_json"),
                json.clone(),
                vec![signed],
            )),
        ];
        for root in cases {
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, json.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[json.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost JSON error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_nullable_controls_keep_demand_precision_and_diagnostics_in_projection() {
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |value, field| Expression::Constant(Constant::new(value, field));
        let json_null = || {
            call(
                "cast_json",
                json.clone(),
                vec![value(Datum::Null, integer.clone())],
            )
        };
        let bad_json = || {
            call(
                "cast_json",
                json.clone(),
                vec![value(Datum::Null, vector.clone())],
            )
        };
        let signed = call(
            "cast_signed",
            integer.clone(),
            vec![value(Datum::new_string("12.5tail"), string.clone())],
        );
        let unsigned = integer.clone().with_unsigned(true);
        let time = tidb_datatype::parse_datetime(
            "2026-10-18 12:34:56.123456",
            &chrono_tz::UTC,
            true,
            false,
        )
        .unwrap()
        .time;
        let cases = [
            call(
                "coalesce",
                integer.clone(),
                vec![
                    signed,
                    call(
                        "cast_signed",
                        integer.clone(),
                        vec![value(
                            Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![
                                1.0,
                            ])),
                            vector.clone(),
                        )],
                    ),
                ],
            ),
            call(
                "ifnull",
                json.clone(),
                vec![
                    call(
                        "cast_json",
                        json.clone(),
                        vec![value(Datum::new_string("null"), string.clone())],
                    ),
                    bad_json(),
                ],
            ),
            call("ifnull", json.clone(), vec![json_null(), bad_json()]),
            call(
                "coalesce",
                json.clone(),
                vec![
                    json_null(),
                    call(
                        "cast_json",
                        json.clone(),
                        vec![value(Datum::new_string("{\"a\":1}"), string.clone())],
                    ),
                    bad_json(),
                ],
            ),
            call(
                "coalesce",
                json.clone(),
                vec![json_null(), json_null(), json_null()],
            ),
            call(
                "cast_char",
                string.clone(),
                vec![call(
                    "ifnull",
                    unsigned.clone(),
                    vec![
                        value(Datum::Int(-1), unsigned.clone()),
                        value(Datum::UInt(u64::MAX), unsigned),
                    ],
                )],
            ),
            call(
                "coalesce",
                FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
                vec![
                    value(
                        Datum::Null,
                        FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
                    ),
                    value(
                        Datum::Time(time),
                        FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
                    ),
                ],
            ),
            call(
                "coalesce",
                FieldType::new(FieldTypeCode::Datetime),
                vec![
                    call(
                        "cast_datetime",
                        FieldType::new(FieldTypeCode::Datetime),
                        vec![value(Datum::new_string("bad date"), string.clone())],
                    ),
                    call(
                        "cast_datetime",
                        FieldType::new(FieldTypeCode::Datetime),
                        vec![value(
                            Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![
                                1.0,
                            ])),
                            vector.clone(),
                        )],
                    ),
                ],
            ),
        ];
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost nullable error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_if_case_keep_selected_errors_truth_and_diagnostics_in_projection() {
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |value, field| Expression::Constant(Constant::new(value, field));
        let one = || value(Datum::Int(1), integer.clone());
        let zero = || value(Datum::Int(0), integer.clone());
        let json_value = |text: &str| {
            call(
                "cast_json",
                json.clone(),
                vec![value(Datum::new_string(text), string.clone())],
            )
        };
        let bad_json = || {
            call(
                "cast_json",
                json.clone(),
                vec![value(
                    Datum::Null,
                    FieldType::new(FieldTypeCode::VectorFloat32),
                )],
            )
        };
        let signed = || {
            call(
                "cast_signed",
                integer.clone(),
                vec![value(Datum::new_string("12.5tail"), string.clone())],
            )
        };
        let unsigned = integer.clone().with_unsigned(true);
        let cases = [
            call(
                "if",
                integer.clone(),
                vec![
                    value(Datum::new_string("1abc"), string.clone()),
                    signed(),
                    zero(),
                ],
            ),
            call(
                "if",
                json.clone(),
                vec![json_value("null"), json_value("{\"a\":1}"), bad_json()],
            ),
            call(
                "if",
                json.clone(),
                vec![zero(), json_value("{\"a\":1}"), bad_json()],
            ),
            call(
                "case",
                json.clone(),
                vec![
                    zero(),
                    bad_json(),
                    one(),
                    json_value("{\"a\":1}"),
                    bad_json(),
                ],
            ),
            call(
                "case",
                json.clone(),
                vec![
                    zero(),
                    bad_json(),
                    value(Datum::Null, integer.clone()),
                    bad_json(),
                ],
            ),
            call(
                "case",
                json.clone(),
                vec![zero(), json_value("{\"a\":1}"), bad_json()],
            ),
            call(
                "cast_char",
                string.clone(),
                vec![call(
                    "if",
                    unsigned.clone(),
                    vec![
                        one(),
                        value(Datum::Int(-1), unsigned.clone()),
                        value(Datum::UInt(u64::MAX), unsigned),
                    ],
                )],
            ),
            call(
                "coalesce",
                json.clone(),
                vec![
                    call("case", json.clone(), vec![zero(), bad_json()]),
                    json_value("{\"a\":1}"),
                ],
            ),
        ];
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost conditional error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_logical_controls_keep_null_demand_and_prefix_warnings_in_projection() {
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |value, field| Expression::Constant(Constant::new(value, field));
        let text = |text: &str| value(Datum::new_string(text), string.clone());
        let int = |number| value(Datum::Int(number), integer.clone());
        let null = || {
            call(
                "cast_signed",
                integer.clone(),
                vec![value(Datum::Null, integer.clone())],
            )
        };
        let bad = || {
            call(
                "cast_json",
                json.clone(),
                vec![value(
                    Datum::Null,
                    FieldType::new(FieldTypeCode::VectorFloat32),
                )],
            )
        };
        let cases = [
            call("and", integer.clone(), vec![text("0abc"), bad()]),
            call("or", integer.clone(), vec![text("1abc"), bad()]),
            call("and", integer.clone(), vec![null(), int(0)]),
            call("and", integer.clone(), vec![null(), int(1)]),
            call("or", integer.clone(), vec![null(), int(1)]),
            call("or", integer.clone(), vec![null(), int(0)]),
            call("and", integer.clone(), vec![text("1abc"), bad()]),
            call("or", integer.clone(), vec![text("0abc"), bad()]),
            call("and", integer.clone(), vec![text("1abc"), text("2tail")]),
            call(
                "cast_char",
                string.clone(),
                vec![call("or", integer.clone(), vec![text("1abc"), bad()])],
            ),
        ];
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost logical error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_boolean_predicates_preserve_values_and_validation_in_projection() {
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let mut cases = Vec::new();
        for name in [
            "not",
            "isnull",
            "istrue",
            "istrue_with_null",
            "isnottrue",
            "isfalse",
            "isnotfalse",
            "isunknown",
            "isnotunknown",
        ] {
            for (value, field) in [
                (Datum::Null, integer.clone()),
                (Datum::new_string("0tail"), string.clone()),
                (Datum::new_string("1tail"), string.clone()),
                (Datum::MinNotNull, integer.clone()),
            ] {
                let child = call(
                    name,
                    integer.clone(),
                    vec![Expression::Constant(Constant::new(value, field))],
                );
                cases.push(call("cast_signed", integer.clone(), vec![child]));
            }
        }
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost logical error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_comparisons_preserve_raw_demand_and_diagnostics_in_projection() {
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |datum, field| Expression::Constant(Constant::new(datum, field));
        let text = |s: &str| value(Datum::new_string(s), string.clone());
        let bad = || call("cast_json", json.clone(), vec![text("bad JSON")]);
        let pairs = [
            (
                value(Datum::Null, integer.clone()),
                value(Datum::Null, integer.clone()),
            ),
            (
                value(Datum::Null, integer.clone()),
                value(Datum::Int(0), integer.clone()),
            ),
            (
                value(Datum::Int(0), integer.clone()),
                value(Datum::Null, integer.clone()),
            ),
            (
                value(Datum::Int(-1), integer.clone().with_unsigned(true)),
                value(Datum::Int(-1), integer.clone()),
            ),
            (
                value(Datum::Real(f64::NAN), FieldType::new(FieldTypeCode::Double)),
                value(Datum::Int(1), integer.clone()),
            ),
            (value(Datum::Null, integer.clone()), bad()),
            (text("1tail"), bad()),
            (
                call("cast_signed", integer.clone(), vec![text("1tail")]),
                bad(),
            ),
            (text("1tail"), value(Datum::Int(0), integer.clone())),
        ];
        let mut cases = Vec::new();
        for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq"] {
            for (left, right) in &pairs {
                cases.push(call(
                    "cast_signed",
                    integer.clone(),
                    vec![call(
                        name,
                        integer.clone(),
                        vec![left.clone(), right.clone()],
                    )],
                ));
            }
        }
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost logical error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_nullif_preserves_first_values_and_eager_diagnostics_in_projection() {
        use tidb_datatype::{BinaryLiteral, Decimal};
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let string = FieldType::new(FieldTypeCode::VarString)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(20)
            .with_decimal(4);
        let time_type = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        let time = tidb_datatype::parse_datetime(
            "2026-10-17 12:34:56.123456",
            &chrono_tz::UTC,
            true,
            false,
        )
        .unwrap()
        .time;
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |datum, field| Expression::Constant(Constant::new(datum, field));
        let text = |s: &str| value(Datum::new_string(s), string.clone());
        let bad = || call("cast_json", json.clone(), vec![text("bad JSON")]);
        let data = vec![
            (string.clone(), text("A"), text("a")),
            (
                string
                    .clone()
                    .with_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi),
                text("A"),
                text("a"),
            ),
            (
                integer.clone(),
                value(Datum::Int(-1), integer.clone().with_unsigned(true)),
                value(Datum::UInt(u64::MAX), integer.clone().with_unsigned(true)),
            ),
            (
                string.clone(),
                value(
                    Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff; 8])),
                    integer.clone(),
                ),
                value(Datum::UInt(u64::MAX), integer.clone().with_unsigned(true)),
            ),
            (
                decimal.clone(),
                value(
                    Datum::Decimal(Decimal::from_literal("1.2300")),
                    decimal.clone(),
                ),
                value(Datum::Int(0), integer.clone()),
            ),
            (
                json.clone(),
                call("cast_json", json.clone(), vec![text("null")]),
                call("cast_json", json.clone(), vec![text("false")]),
            ),
            (integer.clone(), value(Datum::Null, integer.clone()), bad()),
            (
                integer.clone(),
                call("cast_signed", integer.clone(), vec![text("1tail")]),
                bad(),
            ),
            (
                time_type.clone(),
                value(Datum::Time(time), time_type.clone()),
                value(Datum::Null, integer.clone()),
            ),
            (
                vector.clone(),
                value(
                    Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![1.0, 0.0])),
                    vector.clone(),
                ),
                value(
                    Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![1.0, 1.0])),
                    vector.clone(),
                ),
            ),
        ];
        let cases = data
            .into_iter()
            .map(|(field, left, right)| {
                call(
                    "if",
                    field.clone(),
                    vec![
                        call(
                            "cast_signed",
                            integer.clone(),
                            vec![value(Datum::Int(1), integer.clone())],
                        ),
                        call("nullif", field, vec![left, right]),
                        bad(),
                    ],
                )
            })
            .collect::<Vec<_>>();
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost logical error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_abs_preserves_values_overflow_and_warnings_in_projection() {
        use tidb_datatype::Decimal;
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let unsigned = integer.clone().with_unsigned(true);
        let real = FieldType::new(FieldTypeCode::Double);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(60)
            .with_decimal(6);
        let string = FieldType::new(FieldTypeCode::VarString);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |datum, field| Expression::Constant(Constant::new(datum, field));
        let mut cases = vec![
            call(
                "abs",
                integer.clone(),
                vec![value(Datum::Int(-7), integer.clone())],
            ),
            call(
                "abs",
                integer.clone(),
                vec![value(Datum::Int(i64::MIN), integer.clone())],
            ),
            call(
                "abs",
                unsigned.clone(),
                vec![value(Datum::UInt(u64::MAX), unsigned)],
            ),
            call(
                "abs",
                real.clone(),
                vec![value(
                    Datum::Real(f64::from_bits(0xfff8000000000042)),
                    real.clone(),
                )],
            ),
            call(
                "abs",
                real.clone(),
                vec![value(Datum::Real(-0.0), real.clone())],
            ),
            call(
                "abs",
                real.clone(),
                vec![value(Datum::new_string("-1.25tail"), string)],
            ),
            call(
                "abs",
                decimal.clone(),
                vec![value(
                    Datum::Decimal(Decimal::from_literal(
                        "-1234567890123456789012345678901234567890.001",
                    )),
                    decimal,
                )],
            ),
        ];
        for condition in [
            value(Datum::Int(1), integer.clone()),
            call(
                "eq",
                integer.clone(),
                vec![
                    value(Datum::Int(1), integer.clone()),
                    value(Datum::Int(1), integer.clone()),
                ],
            ),
        ] {
            cases.push(call(
                "abs",
                integer.clone(),
                vec![call(
                    "if",
                    integer.clone(),
                    vec![
                        condition,
                        value(Datum::Int(i64::MIN), integer.clone()),
                        value(Datum::Int(0), integer.clone()),
                    ],
                )],
            ));
        }
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost logical error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn native_arithmetic_preserves_signature_demand_and_errors_in_projection() {
        use tidb_datatype::{Decimal, VectorFloat32};
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let unsigned = integer.clone().with_unsigned(true);
        let real = FieldType::new(FieldTypeCode::Double);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(60)
            .with_decimal(6);
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        let string = FieldType::new(FieldTypeCode::VarString);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let call = |name: &str, field: FieldType, args: Vec<Expression>| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let value = |datum, field| Expression::Constant(Constant::new(datum, field));
        let bad = || {
            call(
                "cast_json",
                json.clone(),
                vec![value(Datum::new_string("bad JSON"), string.clone())],
            )
        };
        let mut cases = Vec::new();
        for name in ["plus", "minus", "mul"] {
            let fixtures = vec![
                (
                    integer.clone(),
                    value(Datum::Int(-7), integer.clone()),
                    value(Datum::Int(3), integer.clone()),
                ),
                (
                    integer.clone(),
                    value(Datum::Int(i64::MAX), integer.clone()),
                    value(Datum::Int(1), integer.clone()),
                ),
                (
                    unsigned.clone(),
                    value(Datum::UInt(u64::MAX), unsigned.clone()),
                    value(Datum::Int(1), integer.clone()),
                ),
                (
                    unsigned.clone(),
                    value(Datum::Int(-1), unsigned.clone()),
                    value(Datum::Int(1), integer.clone()),
                ),
                (
                    real.clone(),
                    value(Datum::Real(12.5), real.clone()),
                    value(Datum::new_string("2tail"), string.clone()),
                ),
                (
                    real.clone(),
                    value(Datum::Real(f64::MAX), real.clone()),
                    value(Datum::Real(2.0), real.clone()),
                ),
                (real.clone(), value(Datum::Null, real.clone()), bad()),
                (
                    integer.clone(),
                    value(Datum::Null, integer.clone()),
                    call("cast_signed", integer.clone(), vec![bad()]),
                ),
                (
                    decimal.clone(),
                    value(
                        Datum::Decimal(Decimal::from_literal(
                            "-1234567890123456789012345678901234567890.001",
                        )),
                        decimal.clone(),
                    ),
                    value(Datum::Int(2), integer.clone()),
                ),
                (
                    decimal.clone(),
                    value(Datum::Real(f64::MAX), real.clone()),
                    value(Datum::Decimal(Decimal::from_literal("0")), decimal.clone()),
                ),
                (
                    vector.clone(),
                    value(
                        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
                        vector.clone(),
                    ),
                    value(
                        Datum::VectorFloat32(VectorFloat32::must_create(vec![2.0, 3.0])),
                        vector.clone(),
                    ),
                ),
                (
                    vector.clone(),
                    value(
                        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                        vector.clone(),
                    ),
                    value(
                        Datum::VectorFloat32(VectorFloat32::must_create(vec![2.0, 3.0])),
                        vector.clone(),
                    ),
                ),
            ];
            for (field, left, right) in fixtures {
                cases.push(call(name, field, vec![left, right]));
            }
            cases.push(call(
                "cast_char",
                string.clone(),
                vec![call(
                    name,
                    integer.clone(),
                    vec![
                        call(
                            "abs",
                            integer.clone(),
                            vec![value(Datum::Int(-7), integer.clone())],
                        ),
                        value(Datum::Int(3), integer.clone()),
                    ],
                )],
            ));
        }
        // The native parent selects row demand. Ordinary typed arithmetic
        // keeps its separately tested complete batch operand phases.
        let cases = cases
            .into_iter()
            .map(|root| {
                let field = root.static_type().unwrap().clone();
                call(
                    "if",
                    field,
                    vec![
                        call(
                            "cast_signed",
                            integer.clone(),
                            vec![value(Datum::Int(1), integer.clone())],
                        ),
                        root,
                        bad(),
                    ],
                )
            })
            .collect::<Vec<_>>();
        for root in cases {
            let field = root.static_type().unwrap().clone();
            let reference = crate::StmtContext::for_query();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context = crate::StmtContext::for_query();
                let dual =
                    TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 0, 1, 1024), 1);
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(vec![Column::new(1, field.clone())]), 1, 1, 1024),
                    vec![root.clone()],
                    Box::new(dual),
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                let actual = projection.next(&mut output);
                match &expected {
                    Ok(value) => {
                        actual.unwrap();
                        let mut expected = Chunk::new_with_capacity(&[field.clone()], 1);
                        expected.append_datum(0, value);
                        assert_eq!(output.num_rows(), 1);
                        assert_eq!(output.get_row(0).is_null(0), expected.get_row(0).is_null(0));
                        assert_eq!(
                            output.column(0).get_raw(0).as_ref(),
                            expected.column(0).get_raw(0).as_ref()
                        );
                    }
                    Err(expected_error) => {
                        let Err(ExecError::Eval(actual_error)) = actual else {
                            panic!("lost logical error: {actual:?}");
                        };
                        assert_eq!(&actual_error, expected_error);
                        assert_eq!(output.num_rows(), 0);
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
    #[test]
    fn arithmetic_roots_keep_decimal_priority_and_native_results_in_serial_parallel_projection() {
        use tidb_datatype::Decimal;
        let integer = FieldType::new(FieldTypeCode::LongLong)
            .with_flen(20)
            .with_decimal(0);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6);
        let value = |value, field| Expression::Constant(Constant::new(value, field));
        let call = |name: &str, field, args| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        for name in ["plus", "minus", "mul"] {
            let roots = vec![
                call(
                    name,
                    decimal.clone(),
                    vec![
                        value(
                            Datum::Decimal(Decimal::from_literal("-1.2300")),
                            decimal.clone(),
                        ),
                        value(
                            Datum::Decimal(Decimal::from_literal("2.5")),
                            decimal.clone(),
                        ),
                    ],
                ),
                call(
                    name,
                    integer.clone(),
                    vec![
                        value(Datum::Int(2), integer.clone()),
                        value(Datum::Int(3), integer.clone()),
                    ],
                ),
                call(
                    name,
                    decimal.clone(),
                    vec![
                        value(
                            Datum::Decimal(Decimal::from_literal(
                                "100000000000000000000000000000000000000000000000000",
                            )),
                            decimal.clone(),
                        ),
                        value(Datum::Decimal(Decimal::from_int(2)), decimal.clone()),
                    ],
                ),
            ];
            let fields = vec![decimal.clone(), integer.clone(), decimal.clone()];
            for enabled in [true, false] {
                let reference =
                    crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
                let suite = tidb_expr::evaluator::EvaluatorSuite::new(roots.clone(), false);
                let mut input = Chunk::new_with_capacity(&[], 3);
                input.set_num_virtual_rows(3);
                let mut expected = Chunk::new_with_capacity(&fields, 0);
                suite.run(&reference, &mut input, &mut expected).unwrap();
                let warnings = reference.take_warnings();
                for workers in [0, 2] {
                    let context =
                        crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
                    let (source, _) = NumberSource::new(3, 3);
                    let schema = Schema::new(
                        fields
                            .iter()
                            .enumerate()
                            .map(|(index, field)| Column::new(index as i64 + 1, field.clone()))
                            .collect(),
                    );
                    let mut projection = ProjectionExec::new(
                        ExecutorMeta::new(schema, 1, 3, 1024),
                        roots.clone(),
                        source,
                        context.clone(),
                    )
                    .with_workers(workers);
                    projection.open().unwrap();
                    let mut output = projection.new_chunk();
                    projection.next(&mut output).unwrap();
                    assert_eq!(output.num_rows(), expected.num_rows());
                    for row in 0..output.num_rows() {
                        for column in 0..fields.len() {
                            assert_eq!(
                                output.get_row(row).is_null(column),
                                expected.get_row(row).is_null(column)
                            );
                            assert_eq!(
                                output.column(column).get_raw(row).as_ref(),
                                expected.column(column).get_raw(row).as_ref(),
                                "{name} flag={enabled} workers={workers} row={row} column={column}"
                            );
                        }
                    }
                    assert_eq!(context.take_warnings(), warnings);
                    if workers == 0 {
                        let route = projection.evaluator_suite.batch_route_snapshot();
                        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                    }
                    projection.next(&mut output).unwrap();
                    assert_eq!(output.num_rows(), 0);
                    projection.close().unwrap();
                }
            }
        }
    }
    #[test]
    fn unary_roots_and_nested_casts_keep_native_column_results_in_serial_parallel_projection() {
        let integer = longlong();
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(4);
        let json = FieldType::new(FieldTypeCode::Json)
            .with_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
        let column = |index: usize| {
            let mut value = Column::new(index as i64 + 1, integer.clone());
            value.index = index as i64;
            Expression::Column(value)
        };
        let literal =
            |value| Expression::Constant(Constant::new(Datum::Int(value), integer.clone()));
        let call = |name: &str, field, args| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let roots = vec![
            call(
                "abs",
                integer.clone(),
                vec![call("minus", integer.clone(), vec![column(0), literal(1)])],
            ),
            call("cast_decimal", decimal.clone(), vec![column(0)]),
            call(
                "cast_json",
                json.clone(),
                vec![call(
                    "cast_decimal",
                    decimal.clone(),
                    vec![call("plus", integer.clone(), vec![column(1), literal(1)])],
                )],
            ),
        ];
        let real = FieldType::new(FieldTypeCode::Double)
            .with_flen(22)
            .with_decimal(-1);
        let mut roots = roots;
        roots.push(call(
            "abs",
            real.clone(),
            vec![call(
                "mul",
                real.clone(),
                vec![
                    call("cast_double", real.clone(), vec![column(0)]),
                    call("cast_double", real.clone(), vec![literal(2)]),
                ],
            )],
        ));
        roots.push(call(
            "abs",
            decimal.clone(),
            vec![call(
                "mul",
                decimal.clone(),
                vec![
                    call("cast_decimal", decimal.clone(), vec![column(1)]),
                    call("cast_decimal", decimal.clone(), vec![literal(2)]),
                ],
            )],
        ));
        let string = FieldType::new(FieldTypeCode::Varchar)
            .with_flen(128)
            .with_collation(tidb_datatype::Collation::Utf8Mb4Bin);
        roots.push(call("any_value", integer.clone(), vec![column(0)]));
        roots.push(call("pi", real.clone(), vec![]));
        roots.push(call(
            "concat",
            string.clone(),
            vec![
                call(
                    "cast_char",
                    string.clone(),
                    vec![call("abs", integer.clone(), vec![column(0)])],
                ),
                Expression::Constant(Constant::new(Datum::new_string("x"), string.clone())),
            ],
        ));
        let mut fields = vec![
            integer.clone(),
            decimal.clone(),
            json,
            real.clone(),
            decimal,
            integer.clone(),
            real,
            string.clone(),
        ];
        let bits = integer.clone().with_unsigned(true);
        for name in [
            "bitand",
            "bitor",
            "bitxor",
            "leftshift",
            "rightshift",
            "bitneg",
        ] {
            let mut args = vec![call("abs", integer.clone(), vec![column(0)])];
            if name != "bitneg" {
                args.push(call("cast_signed", integer.clone(), vec![column(1)]));
            }
            roots.push(call(name, bits.clone(), args));
            fields.push(bits.clone());
        }
        roots.push(call(
            "plus",
            bits.clone(),
            vec![
                call("bitxor", bits.clone(), vec![column(0), column(1)]),
                literal(1),
            ],
        ));
        fields.push(bits);
        roots.push(call(
            "xor",
            integer.clone(),
            vec![call("abs", integer.clone(), vec![column(0)]), literal(1)],
        ));
        fields.push(integer.clone());
        roots.push(call(
            "plus",
            integer.clone(),
            vec![
                call("xor", integer.clone(), vec![column(0), column(1)]),
                literal(1),
            ],
        ));
        fields.push(integer.clone());
        roots.push(call(
            "xor",
            integer.clone(),
            vec![
                Expression::Constant(Constant::new(Datum::Null, integer.clone())),
                Expression::Constant(Constant::new(Datum::new_string("2tail"), string.clone())),
            ],
        ));
        fields.push(integer.clone());
        roots.push(call("unaryplus", integer.clone(), vec![column(0)]));
        fields.push(integer.clone());
        roots.push(call("unaryminus", integer.clone(), vec![column(1)]));
        fields.push(integer.clone());
        let promoted = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(21)
            .with_decimal(0);
        roots.push(call(
            "unaryminus",
            promoted.clone(),
            vec![Expression::Constant(Constant::new(
                Datum::UInt(u64::MAX),
                integer.clone().with_unsigned(true),
            ))],
        ));
        fields.push(promoted);
        let sign_decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(4);
        roots.push(call(
            "unaryplus",
            sign_decimal.clone(),
            vec![call("cast_decimal", sign_decimal.clone(), vec![column(0)])],
        ));
        fields.push(sign_decimal.clone());
        for name in ["mod", "intdiv"] {
            roots.push(call(name, integer.clone(), vec![column(0), literal(2)]));
            fields.push(integer.clone());
        }
        roots.push(call(
            "div",
            sign_decimal.clone(),
            vec![column(0), literal(2)],
        ));
        fields.push(sign_decimal.clone());
        let real = FieldType::new(FieldTypeCode::Double);
        for name in ["mod", "div"] {
            roots.push(call(
                name,
                real.clone(),
                vec![
                    column(0),
                    Expression::Constant(Constant::new(Datum::Real(2.5), real.clone())),
                ],
            ));
            fields.push(real.clone());
        }
        roots.push(call(
            "intdiv",
            integer.clone(),
            vec![
                call("cast_decimal", sign_decimal.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(
                    Datum::Decimal(tidb_datatype::Decimal::from_int(2)),
                    sign_decimal,
                )),
            ],
        ));
        fields.push(integer.clone());
        roots.push(call(
            "in",
            integer.clone(),
            vec![
                call("abs", integer.clone(), vec![column(0)]),
                literal(1),
                column(1),
            ],
        ));
        fields.push(integer.clone());
        let json = FieldType::new(FieldTypeCode::Json);
        roots.push(call(
            "in",
            integer.clone(),
            vec![
                call("cast_json", json.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(
                    Datum::Json(tidb_datatype::BinaryJSON::parse("1").unwrap()),
                    json,
                )),
                Expression::Constant(Constant::new(Datum::new_string("2"), string.clone())),
            ],
        ));
        fields.push(integer.clone());
        let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
        roots.push(call(
            "in",
            integer.clone(),
            vec![
                call("cast_time", duration.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(
                    Datum::Duration(
                        tidb_datatype::MySqlDuration::from_nanoseconds(1_000_000_000, 6).unwrap(),
                    ),
                    duration,
                )),
                column(1),
            ],
        ));
        fields.push(integer.clone());
        roots.push(call(
            "interval",
            integer.clone(),
            vec![
                call("abs", integer.clone(), vec![column(0)]),
                literal(1),
                literal(3),
                column(1),
            ],
        ));
        fields.push(integer.clone());
        let interval_real = FieldType::new(FieldTypeCode::Double).with_flags(1);
        roots.push(call(
            "interval",
            integer.clone(),
            vec![
                call("cast_double", interval_real.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(Datum::Real(1.0), interval_real.clone())),
                Expression::Constant(Constant::new(Datum::Real(2.0), interval_real.clone())),
                Expression::Constant(Constant::new(Datum::Real(3.0), interval_real)),
            ],
        ));
        fields.push(integer.clone());
        roots.push(call(
            "interval",
            integer.clone(),
            vec![
                call(
                    "cast_json",
                    FieldType::new(FieldTypeCode::Json),
                    vec![column(0)],
                ),
                literal(1),
                literal(2),
            ],
        ));
        fields.push(integer.clone());
        let extremum_start = roots.len();
        roots.push(call(
            "greatest",
            integer.clone(),
            vec![
                call("abs", integer.clone(), vec![column(0)]),
                literal(1),
                column(1),
            ],
        ));
        fields.push(integer.clone());
        let gl_real = FieldType::new(FieldTypeCode::Double);
        roots.push(call(
            "least",
            gl_real.clone(),
            vec![
                call("cast_double", gl_real.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(Datum::Real(2.5), gl_real.clone())),
            ],
        ));
        fields.push(gl_real);
        let gl_decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(40)
            .with_decimal(4);
        roots.push(call(
            "greatest",
            gl_decimal.clone(),
            vec![
                call("cast_decimal", gl_decimal.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5000")),
                    gl_decimal.clone(),
                )),
            ],
        ));
        fields.push(gl_decimal);
        roots.push(call(
            "least",
            string.clone(),
            vec![
                call(
                    "cast_char",
                    string.clone(),
                    vec![call("abs", integer.clone(), vec![column(0)])],
                ),
                Expression::Constant(Constant::new(Datum::new_string("a"), string.clone())),
            ],
        ));
        fields.push(string.clone());
        let gl_time = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
        let time = |text| {
            tidb_datatype::parse_datetime(text, &chrono_tz::UTC, true, false)
                .unwrap()
                .time
        };
        roots.push(call(
            "greatest",
            gl_time.clone(),
            vec![
                Expression::Constant(Constant::new(
                    Datum::Time(time("2020-01-01 01:02:03.123456")),
                    gl_time.clone(),
                )),
                Expression::Constant(Constant::new(
                    Datum::Time(time("2021-01-01 01:02:03.123456")),
                    gl_time.clone(),
                )),
            ],
        ));
        fields.push(gl_time);
        let mut date = time("2020-01-01 00:00:00");
        date.set_kind(tidb_datatype::TimeType::Date);
        roots.push(call(
            "greatest",
            string.clone(),
            vec![
                Expression::Constant(Constant::new(
                    Datum::Time(date),
                    FieldType::new(FieldTypeCode::Date),
                )),
                call("cast_char", string.clone(), vec![column(0)]),
            ],
        ));
        fields.push(string.clone());
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        roots.push(call(
            "least",
            vector.clone(),
            vec![
                Expression::Constant(Constant::new(
                    Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![1.0, 2.0])),
                    vector.clone(),
                )),
                Expression::Constant(Constant::new(
                    Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![2.0, 1.0])),
                    vector.clone(),
                )),
            ],
        ));
        fields.push(vector);
        let gl_json = FieldType::new(FieldTypeCode::Json);
        roots.push(call(
            "greatest",
            string.clone(),
            vec![
                call("cast_json", gl_json.clone(), vec![column(0)]),
                Expression::Constant(Constant::new(
                    Datum::Json(tidb_datatype::BinaryJSON::parse("1").unwrap()),
                    gl_json,
                )),
            ],
        ));
        fields.push(string.clone());
        let extremum_mix = std::iter::once(0)
            .chain(extremum_start..roots.len())
            .collect::<Vec<_>>();
        let charset_start = roots.len();
        let binary = string
            .clone()
            .with_collation(tidb_datatype::Collation::Binary);
        let gbk = string
            .clone()
            .with_collation(tidb_datatype::Collation::GbkBin);
        let using = || {
            call(
                "convert_using",
                gbk.clone(),
                vec![
                    call("cast_char", string.clone(), vec![column(0)]),
                    Expression::Constant(Constant::new(Datum::new_string("gbk"), string.clone())),
                ],
            )
        };
        roots.push(using());
        fields.push(gbk.clone());
        roots.push(call("to_binary", binary.clone(), vec![using()]));
        fields.push(binary.clone());
        roots.push(call(
            "from_binary",
            gbk.clone(),
            vec![call("to_binary", binary, vec![using()])],
        ));
        fields.push(gbk.clone());
        roots.push(call(
            "concat",
            string.clone(),
            vec![
                using(),
                Expression::Constant(Constant::new(Datum::new_string("x"), string.clone())),
            ],
        ));
        fields.push(string.clone());
        let charset_mix = std::iter::once(0)
            .chain(charset_start..roots.len())
            .collect::<Vec<_>>();
        let jsonsum_start = roots.len();
        let checksum = || {
            call(
                "json_sum_crc32",
                integer.clone(),
                vec![call(
                    "if",
                    string.clone(),
                    vec![
                        column(0),
                        Expression::Constant(Constant::new(
                            Datum::new_string("[1,2,3]"),
                            string.clone(),
                        )),
                        Expression::Constant(Constant::new(Datum::Null, string.clone())),
                    ],
                )],
            )
        };
        roots.push(checksum());
        fields.push(integer.clone());
        roots.push(call("plus", integer.clone(), vec![checksum(), column(1)]));
        fields.push(integer.clone());
        roots.push(call(
            "ifnull",
            integer.clone(),
            vec![
                checksum(),
                Expression::Constant(Constant::new(Datum::Int(7), integer.clone())),
            ],
        ));
        fields.push(integer.clone());
        roots.push(call("cast_char", string.clone(), vec![checksum()]));
        fields.push(string.clone());
        let jsonsum_mix = std::iter::once(0)
            .chain(jsonsum_start..roots.len())
            .collect::<Vec<_>>();
        let datetime = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
        let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
        let parsed = |result: &FieldType, clock: bool| {
            call(
                "str_to_date",
                result.clone(),
                vec![
                    call(
                        "if",
                        string.clone(),
                        vec![
                            column(0),
                            Expression::Constant(Constant::new(
                                Datum::new_string(if clock {
                                    "12:34:56.123456"
                                } else {
                                    "2020-02-29 12:34:56.123456"
                                }),
                                string.clone(),
                            )),
                            Expression::Constant(Constant::new(Datum::Null, string.clone())),
                        ],
                    ),
                    Expression::Constant(Constant::new(
                        Datum::new_string(if clock {
                            "%H:%i:%s.%f"
                        } else {
                            "%Y-%m-%d %H:%i:%s.%f"
                        }),
                        string.clone(),
                    )),
                ],
            )
        };
        let temporal_start = roots.len();
        roots.push(parsed(&datetime, false));
        fields.push(datetime.clone());
        roots.push(parsed(&duration, true));
        fields.push(duration.clone());
        roots.push(call(
            "month",
            integer.clone(),
            vec![parsed(&datetime, false)],
        ));
        fields.push(integer.clone());
        let temporal_mix = std::iter::once(0)
            .chain(temporal_start..roots.len())
            .collect::<Vec<_>>();
        let temporal_text_start = roots.len();
        roots.push(parsed(&string, false));
        fields.push(string.clone());
        roots.push(call(
            "concat",
            string.clone(),
            vec![
                parsed(&string, false),
                Expression::Constant(Constant::new(Datum::new_string("x"), string.clone())),
            ],
        ));
        fields.push(string.clone());
        let date = FieldType::new(FieldTypeCode::Date);
        roots.push(parsed(&date, false));
        fields.push(date);
        roots.push(call(
            "cast_char",
            string.clone(),
            vec![parsed(&duration, true)],
        ));
        fields.push(string.clone());
        let temporal_text_mix = std::iter::once(0)
            .chain(temporal_text_start..roots.len())
            .collect::<Vec<_>>();
        let interval_mix = [0, 7, 28, 29, 30, 31, 32, 33];
        let configurations = [
            (roots[..31].to_vec(), fields[..31].to_vec()),
            (
                interval_mix
                    .iter()
                    .map(|&index| roots[index].clone())
                    .collect(),
                interval_mix
                    .iter()
                    .map(|&index| fields[index].clone())
                    .collect(),
            ),
            (
                extremum_mix
                    .iter()
                    .map(|&index| roots[index].clone())
                    .collect(),
                extremum_mix
                    .iter()
                    .map(|&index| fields[index].clone())
                    .collect(),
            ),
            (
                charset_mix
                    .iter()
                    .map(|&index| roots[index].clone())
                    .collect(),
                charset_mix
                    .iter()
                    .map(|&index| fields[index].clone())
                    .collect(),
            ),
            (
                jsonsum_mix
                    .iter()
                    .map(|&index| roots[index].clone())
                    .collect(),
                jsonsum_mix
                    .iter()
                    .map(|&index| fields[index].clone())
                    .collect(),
            ),
            (
                temporal_mix
                    .iter()
                    .map(|&index| roots[index].clone())
                    .collect(),
                temporal_mix
                    .iter()
                    .map(|&index| fields[index].clone())
                    .collect(),
            ),
            (
                temporal_text_mix
                    .iter()
                    .map(|&index| roots[index].clone())
                    .collect(),
                temporal_text_mix
                    .iter()
                    .map(|&index| fields[index].clone())
                    .collect(),
            ),
        ];
        // Keep prior matrices intact. Each new heterogeneous suite adds a
        // family within the unchanged finite metadata allowance.
        for (roots, fields) in configurations {
            for enabled in [true, false] {
                let reference =
                    crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
                let suite = EvaluatorSuite::new(roots.clone(), false);
                let mut input = Chunk::new_with_capacity(&[integer.clone(), integer.clone()], 3);
                for row in 0..3 {
                    input.append_int64(0, row);
                    input.append_int64(1, row * 2);
                }
                let mut expected = Chunk::new_with_capacity(&fields, 0);
                suite.run(&reference, &mut input, &mut expected).unwrap();
                let warnings = reference.take_warnings();
                for workers in [0, 2] {
                    let context =
                        crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
                    let (source, _) = NumberSource::new(3, 3);
                    let schema = Schema::new(
                        fields
                            .iter()
                            .enumerate()
                            .map(|(index, field)| Column::new(index as i64 + 1, field.clone()))
                            .collect(),
                    );
                    let mut projection = ProjectionExec::new(
                        ExecutorMeta::new(schema, 1, 3, 1024),
                        roots.clone(),
                        source,
                        context.clone(),
                    )
                    .with_workers(workers);
                    projection.open().unwrap();
                    let mut output = projection.new_chunk();
                    projection.next(&mut output).unwrap();
                    assert_eq!(output.num_rows(), expected.num_rows());
                    for row in 0..output.num_rows() {
                        for column in 0..fields.len() {
                            assert_eq!(
                                output.get_row(row).is_null(column),
                                expected.get_row(row).is_null(column)
                            );
                            assert_eq!(
                                output.column(column).get_raw(row).as_ref(),
                                expected.column(column).get_raw(row).as_ref(),
                                "flag={enabled} workers={workers} row={row} column={column}"
                            );
                        }
                    }
                    assert_eq!(context.take_warnings(), warnings);
                    if workers == 0 {
                        let route = projection.evaluator_suite.batch_route_snapshot();
                        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                    }
                    projection.close().unwrap();
                }
            }
        }
    }
    #[test]
    fn control_roots_keep_null_arithmetic_demand_in_serial_parallel_projection() {
        let integer = longlong();
        let column = |index: usize| {
            let mut value = Column::new(index as i64 + 1, integer.clone());
            value.index = index as i64;
            Expression::Column(value)
        };
        let literal =
            |value| Expression::Constant(Constant::new(Datum::Int(value), integer.clone()));
        let call = |name: &str, field, args| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
        };
        let selected = || {
            call(
                "plus",
                integer.clone(),
                vec![
                    Expression::Constant(Constant::new(Datum::Null, integer.clone())),
                    call("mul", integer.clone(), vec![literal(i64::MAX), column(1)]),
                ],
            )
        };
        let roots = vec![
            call(
                "if",
                integer.clone(),
                vec![literal(1), selected(), literal(0)],
            ),
            call("coalesce", integer.clone(), vec![selected(), column(0)]),
            call(
                "case",
                integer.clone(),
                vec![literal(1), selected(), literal(0)],
            ),
            call("eq", integer.clone(), vec![selected(), column(0)]),
            call(
                "eq",
                integer.clone(),
                vec![
                    call("abs", integer.clone(), vec![column(0)]),
                    call("abs", integer.clone(), vec![column(1)]),
                ],
            ),
            call(
                "coalesce",
                integer.clone(),
                vec![
                    call("abs", integer.clone(), vec![column(0)]),
                    call("abs", integer.clone(), vec![column(1)]),
                ],
            ),
            call(
                "and",
                integer.clone(),
                vec![
                    call("abs", integer.clone(), vec![column(0)]),
                    call("abs", integer.clone(), vec![column(1)]),
                ],
            ),
        ];
        let fields = vec![integer.clone(); roots.len()];
        for enabled in [true, false] {
            let reference =
                crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
            let suite = EvaluatorSuite::new(roots.clone(), false);
            let mut input = Chunk::new_with_capacity(&[integer.clone(), integer.clone()], 3);
            for row in 0..3 {
                input.append_int64(0, row);
                input.append_int64(1, row * 2);
            }
            let mut expected = Chunk::new_with_capacity(&fields, 0);
            suite.run(&reference, &mut input, &mut expected).unwrap();
            let warnings = reference.take_warnings();
            for workers in [0, 2] {
                let context =
                    crate::StmtContext::for_query().with_enable_vectorized_expression(enabled);
                let (source, _) = NumberSource::new(3, 3);
                let schema = Schema::new(
                    fields
                        .iter()
                        .enumerate()
                        .map(|(index, field)| Column::new(index as i64 + 1, field.clone()))
                        .collect(),
                );
                let mut projection = ProjectionExec::new(
                    ExecutorMeta::new(schema, 1, 3, 1024),
                    roots.clone(),
                    source,
                    context.clone(),
                )
                .with_workers(workers);
                projection.open().unwrap();
                let mut output = projection.new_chunk();
                projection.next(&mut output).unwrap();
                assert_eq!(output.num_rows(), expected.num_rows());
                for row in 0..output.num_rows() {
                    for column in 0..fields.len() {
                        assert_eq!(
                            output.get_row(row).is_null(column),
                            expected.get_row(row).is_null(column)
                        );
                        assert_eq!(
                            output.column(column).get_raw(row).as_ref(),
                            expected.column(column).get_raw(row).as_ref(),
                            "flag={enabled} workers={workers} row={row} column={column}"
                        );
                    }
                }
                assert_eq!(context.take_warnings(), warnings);
                if workers == 0 {
                    let route = projection.evaluator_suite.batch_route_snapshot();
                    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
                }
                projection.close().unwrap();
            }
        }
    }
}
