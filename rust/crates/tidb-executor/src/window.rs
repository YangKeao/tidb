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

//! Go `pkg/executor/windows/window.go`: physical window execution.
//!
//! The executor buffers the child (whose physical plan guarantees the
//! `PARTITION BY ++ ORDER BY` order), splits the buffer into contiguous
//! partitions, and for every output row recomputes its frame. Go's
//! `rowFrameWindowProcessor` slides the frame instead; recomputing is
//! equivalent and keeps one implementation for every aggregate.
//!
//! RANGE bounds use the planner's typed CalcFuncs/CompareCols and Go's
//! monotonic cursors. Ranking and value functions share partition/peer state.

use std::cmp::Ordering;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::evaluator::{into_eval_error, EvaluatorSuite};
use tidb_expr::expression::Expression;
use tidb_expr::Columns;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, WindowAggregateEvaluator};

/// One window function's runtime form.
pub struct WindowFuncSpec {
    /// The concrete aggregate or partition-position function.
    pub func: WindowFunction,
    /// The output column's type (Go `WindowFuncDesc.RetType`).
    pub output_type: FieldType,
}

/// Go aggfuncs' partition-position functions; ordinary aggregates retain
/// the existing frame evaluator.
pub enum WindowFunction {
    /// Aggregate over the frame.
    Aggregate(AggFunc),
    /// One-based position.
    RowNumber,
    /// Peer rank, optionally without gaps.
    Rank { dense: bool },
    /// Relative rank in the partition.
    PercentRank,
    /// Fraction through the last peer.
    CumeDist,
    /// Bucket count (NULL propagates).
    Ntile(Option<u64>),
    /// FIRST/LAST/NTH_VALUE select a row from the current frame.
    Value {
        arg: Expression,
        nth: Option<u64>,
        last: bool,
    },
    /// LEAD/LAG select from the partition, regardless of the frame.
    Relative {
        arg: Expression,
        offset: u64,
        default: Option<Expression>,
        lead: bool,
    },
}

/// One ROWS frame bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowBound {
    /// Go `ast.CurrentRow`.
    CurrentRow,
    /// Go `FrameBound.UnBounded`.
    Unbounded,
    /// Go `FrameBound.Num`, with its direction.
    Offset {
        /// Go `FrameBound.Num`.
        num: u64,
        /// Go `FrameBound.Type == ast.Preceding`.
        preceding: bool,
    },
}

/// One ROWS frame.
#[derive(Clone, Debug)]
pub struct WindowFrameSpec {
    /// Go `WindowFrame.Start`.
    pub start: WindowBound,
    /// Go `WindowFrame.End`.
    pub end: WindowBound,
    /// Go's RANGE comparison expressions; absent for ROWS.
    pub range: Option<tidb_planner::logical::window::WindowFrame>,
    /// Go rangeFrameWindowProcessor.expectedCmpResult.
    pub range_desc: bool,
}

/// Expression programs are retained independently of mutable window state.
/// Only value functions have an argument; only LEAD/LAG may have a default.
struct WindowValueEvaluators {
    arg: Option<EvaluatorSuite>,
    default: Option<EvaluatorSuite>,
    aggregate: Option<WindowAggregateEvaluator>,
}

struct WindowRangeEvaluators {
    calc: Vec<EvaluatorSuite>,
    compare: Vec<EvaluatorSuite>,
}

impl WindowValueEvaluators {
    fn new(func: &WindowFunction) -> Self {
        let (arg, default) = match func {
            WindowFunction::Value { arg, .. } => (Some(arg), None),
            WindowFunction::Relative { arg, default, .. } => (Some(arg), default.as_ref()),
            _ => (None, None),
        };
        let suite = |expr: &Expression| EvaluatorSuite::new(vec![expr.clone()], true);
        Self {
            arg: arg.map(suite),
            default: default.map(suite),
            aggregate: match func {
                WindowFunction::Aggregate(func) => Some(WindowAggregateEvaluator::new(func)),
                _ => None,
            },
        }
    }
}

/// Go `pkg/executor/windows/window.go::WindowExec` (ROWS slice).
pub struct WindowExec<C: Columns> {
    meta: ExecutorMeta,
    funcs: Vec<WindowFuncSpec>,
    value_suites: Vec<WindowValueEvaluators>,
    partition_by: Vec<Expression>,
    order_by: Vec<Expression>,
    partition_suites: Vec<EvaluatorSuite>,
    order_suites: Vec<EvaluatorSuite>,
    /// First peer, exclusive last peer, one-based dense rank.
    peers: Vec<(usize, usize, usize)>,
    frame: WindowFrameSpec,
    range_suites: [Option<WindowRangeEvaluators>; 2],
    range_start: usize,
    range_end: usize,
    child: Box<dyn Executor>,
    ctx: C,
    /// Every child row, in child order.
    rows: Chunk,
    /// The child's column count; the window outputs follow it.
    child_width: usize,
    /// Contiguous partition ranges, computed once the child is drained.
    partitions: Vec<(usize, usize)>,
    /// The partition of every buffered row, so emission is O(1) per row.
    partition_of: Vec<(usize, usize)>,
    fetched: bool,
    emitted: usize,
}

impl<C: Columns> WindowExec<C> {
    /// Builds a window over `child`; `child_width` is the number of columns
    /// the child contributes to the output.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        funcs: Vec<WindowFuncSpec>,
        partition_by: Vec<Expression>,
        order_by: Vec<Expression>,
        frame: WindowFrameSpec,
        child: Box<dyn Executor>,
        ctx: C,
        child_width: usize,
    ) -> Self {
        let types = child.ret_field_types().to_vec();
        let capacity = child.init_cap();
        let value_suites = funcs
            .iter()
            .map(|spec| WindowValueEvaluators::new(&spec.func))
            .collect();
        let partition_suites = Self::key_suites(&partition_by);
        let order_suites = Self::key_suites(&order_by);
        let range_suites = Self::range_suites(&frame);
        Self {
            range_suites,
            value_suites,
            partition_suites,
            order_suites,
            meta,
            funcs,
            partition_by,
            order_by,
            peers: Vec::new(),
            frame,
            range_start: 0,
            range_end: 0,
            child,
            ctx,
            rows: Chunk::new_with_capacity(&types, capacity),
            child_width,
            partitions: Vec::new(),
            partition_of: Vec::new(),
            fetched: false,
            emitted: 0,
        }
    }

    /// Drains the child into one buffer and computes the partition ranges.
    fn fetch(&mut self) -> Result<(), ExecError> {
        let types = self.child.ret_field_types().to_vec();
        self.rows = Chunk::new_with_capacity(&types, self.child.init_cap());
        let mut chunk = self.child.new_chunk();
        loop {
            chunk.reset();
            self.child.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            for index in 0..chunk.num_rows() {
                self.rows.append_row(chunk.get_row(index));
            }
        }
        self.partitions.clear();
        self.partition_of = vec![(0, 0); self.rows.num_rows()];
        let mut start = 0;
        for index in 1..self.rows.num_rows() {
            if !self.same_partition(index - 1, index)? {
                self.partitions.push((start, index));
                start = index;
            }
        }
        if self.rows.num_rows() > 0 {
            self.partitions.push((start, self.rows.num_rows()));
        }
        for &(start, end) in &self.partitions {
            for slot in &mut self.partition_of[start..end] {
                *slot = (start, end);
            }
        }
        self.peers = vec![(0, 0, 0); self.rows.num_rows()];
        for &(start, end) in &self.partitions {
            let mut peer_start = start;
            let mut rank = 1;
            for peer_end in start + 1..=end {
                if peer_end < end
                    && Self::same_keys(
                        &self.ctx,
                        &mut self.rows,
                        &self.order_by,
                        &self.order_suites,
                        peer_end - 1,
                        peer_end,
                    )?
                {
                    continue;
                }
                self.peers[peer_start..peer_end].fill((peer_start, peer_end, rank));
                peer_start = peer_end;
                rank += 1;
            }
        }
        self.fetched = true;
        Ok(())
    }

    /// Go's partition boundary: consecutive rows belong to one partition
    /// when every `PARTITION BY` key compares equal (NULLs equal).
    fn same_partition(&mut self, left: usize, right: usize) -> Result<bool, ExecError> {
        Self::same_keys(
            &self.ctx,
            &mut self.rows,
            &self.partition_by,
            &self.partition_suites,
            left,
            right,
        )
    }

    fn key_suites(keys: &[Expression]) -> Vec<EvaluatorSuite> {
        keys.iter()
            .cloned()
            .map(|key| EvaluatorSuite::new(vec![key], true))
            .collect()
    }

    /// The private drained buffer is dense between calls. Select only the
    /// demanded row without copying its columns, and restore the buffer before
    /// propagating a normal evaluation error. Suites retain compiled programs,
    /// not result values (which could become stale for effectful expressions).
    fn eval_selected_row(
        ctx: &C,
        rows: &mut Chunk,
        suite: &EvaluatorSuite,
        row: usize,
    ) -> Result<Datum, ExecError> {
        debug_assert!(rows.sel().is_none());
        rows.set_sel(Some(vec![row]));
        let result = suite.eval_chunk(ctx, rows);
        rows.set_sel(None);
        result
            .map_err(into_eval_error)?
            .into_iter()
            .next()
            .ok_or_else(|| ExecError::internal("window expression evaluation returned no row"))
    }

    fn same_keys(
        ctx: &C,
        rows: &mut Chunk,
        keys: &[Expression],
        suites: &[EvaluatorSuite],
        left: usize,
        right: usize,
    ) -> Result<bool, ExecError> {
        debug_assert_eq!(keys.len(), suites.len());
        for (expression, suite) in keys.iter().zip(suites) {
            // Preserve left/right and key order: a mismatch must not demand
            // later keys, and later rows must not be evaluated eagerly.
            let left_value = Self::eval_selected_row(ctx, rows, suite, left)?;
            let right_value = Self::eval_selected_row(ctx, rows, suite, right)?;
            if tidb_expr::compare_datums_with_collation(
                &left_value,
                &right_value,
                tidb_expr::collation_derive::collation_of_node(expression),
            )? != Ordering::Equal
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn range_suites(frame: &WindowFrameSpec) -> [Option<WindowRangeEvaluators>; 2] {
        [
            frame.range.as_ref().and_then(|range| range.start.as_ref()),
            frame.range.as_ref().and_then(|range| range.end.as_ref()),
        ]
        .map(|bound| {
            bound.map(|bound| WindowRangeEvaluators {
                calc: Self::key_suites(&bound.calc_funcs),
                compare: Self::key_suites(&bound.compare_cols),
            })
        })
    }

    /// Go `getStartOffset`/`getEndOffset` for the ROWS frame, clamped to the
    /// partition. The end bound is exclusive.
    fn frame_range(
        &mut self,
        index: usize,
        start: usize,
        end: usize,
    ) -> Result<(usize, usize), ExecError> {
        if self.frame.range.is_some() {
            if index == start {
                self.range_start = start;
                self.range_end = start;
            }
            self.range_start = self.range_bound(index, self.range_start, end, false)?;
            self.range_end = self.range_bound(index, self.range_end, end, true)?;
            return Ok((self.range_start, self.range_end.max(self.range_start)));
        }
        let start_bound = match self.frame.start {
            WindowBound::CurrentRow => index,
            WindowBound::Unbounded => start,
            WindowBound::Offset {
                num,
                preceding: true,
            } => index.saturating_sub(usize::try_from(num).unwrap_or(usize::MAX)),
            WindowBound::Offset {
                num,
                preceding: false,
            } => index.saturating_add(usize::try_from(num).unwrap_or(usize::MAX)),
        }
        .max(start)
        .min(end);
        let end_bound = match self.frame.end {
            WindowBound::CurrentRow => index.saturating_add(1),
            WindowBound::Unbounded => end,
            WindowBound::Offset {
                num,
                preceding: true,
            } => index
                .saturating_add(1)
                .saturating_sub(usize::try_from(num).unwrap_or(usize::MAX)),
            WindowBound::Offset {
                num,
                preceding: false,
            } => index
                .saturating_add(1)
                .saturating_add(usize::try_from(num).unwrap_or(usize::MAX)),
        }
        .max(start)
        .min(end);
        Ok((start_bound, end_bound.max(start_bound)))
    }

    /// Go rangeFrameWindowProcessor advances both offsets monotonically.
    /// CompareCols reads the candidate row; CalcFuncs reads the current row.
    fn range_bound(
        &mut self,
        current: usize,
        mut cursor: usize,
        end: usize,
        is_end: bool,
    ) -> Result<usize, ExecError> {
        use tidb_planner::logical::window::BoundType;
        let bound = self.frame.range.as_ref().and_then(|frame| {
            if is_end {
                frame.end.as_ref()
            } else {
                frame.start.as_ref()
            }
        });
        let Some(bound) = bound.filter(|bound| !bound.unbounded) else {
            return Ok(if is_end {
                end
            } else {
                self.partition_of[current].0
            });
        };
        if bound.bound_type == BoundType::CurrentRow {
            return Ok(if is_end {
                self.peers[current].1
            } else {
                self.peers[current].0
            });
        }
        let suites = self.range_suites[usize::from(is_end)]
            .as_ref()
            .expect("RANGE bound suites");
        let targets = suites
            .calc
            .iter()
            .map(|suite| Self::eval_selected_row(&self.ctx, &mut self.rows, suite, current))
            .collect::<Result<Vec<_>, _>>()?;
        if targets.len() != self.order_by.len() || bound.compare_cols.len() != targets.len() {
            return Err(ExecError::internal(
                "RANGE frame comparison expressions are incomplete",
            ));
        }
        while cursor < end {
            let mut order = Ordering::Equal;
            for ((expr, suite), target) in
                bound.compare_cols.iter().zip(&suites.compare).zip(&targets)
            {
                let value = Self::eval_selected_row(&self.ctx, &mut self.rows, suite, cursor)?;
                order = tidb_expr::compare_datums_with_collation(
                    &value,
                    target,
                    tidb_expr::collation_derive::collation_of_node(expr),
                )?;
                if self.frame.range_desc {
                    order = order.reverse();
                }
                if order != Ordering::Equal {
                    break;
                }
            }
            if if is_end {
                order == Ordering::Greater
            } else {
                order != Ordering::Less
            } {
                break;
            }
            cursor += 1;
        }
        Ok(cursor)
    }
}

impl<C: Columns + Send> Executor for WindowExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.fetched = false;
        self.emitted = 0;
        self.partitions.clear();
        self.partition_of.clear();
        self.rows.reset();
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.fetched {
            self.fetch()?;
        }
        let batch = req.required_rows().min(self.meta.max_chunk_size());
        while req.num_rows() < batch && self.emitted < self.rows.num_rows() {
            let index = self.emitted;
            let (partition_start, partition_end) = self.partition_of[index];
            let (frame_start, frame_end) =
                self.frame_range(index, partition_start, partition_end)?;
            req.append_row(self.rows.get_row(index));
            for (position, spec) in self.funcs.iter().enumerate() {
                let (peer_start, peer_end, dense_rank) = self.peers[index];
                let count = partition_end - partition_start;
                let value = match &spec.func {
                    WindowFunction::Aggregate(func) => self.value_suites[position]
                        .aggregate
                        .as_ref()
                        .expect("aggregate input program")
                        .window_frame_value(
                            func,
                            &self.ctx,
                            &self.rows,
                            frame_start,
                            frame_end,
                            &spec.output_type,
                        )?,
                    WindowFunction::RowNumber => Datum::Int((index - partition_start + 1) as i64),
                    WindowFunction::Rank { dense } => Datum::Int(if *dense {
                        dense_rank
                    } else {
                        peer_start - partition_start + 1
                    } as i64),
                    WindowFunction::PercentRank => Datum::Real(if count <= 1 {
                        0.0
                    } else {
                        (peer_start - partition_start) as f64 / (count - 1) as f64
                    }),
                    WindowFunction::CumeDist => {
                        Datum::Real((peer_end - partition_start) as f64 / count as f64)
                    }
                    WindowFunction::Value { nth, last, .. } => {
                        let target = if *last {
                            frame_end.checked_sub(1)
                        } else {
                            nth.and_then(|n| n.checked_sub(1))
                                .and_then(|n| usize::try_from(n).ok())
                                .and_then(|n| frame_start.checked_add(n))
                        };
                        match target.filter(|target| *target >= frame_start && *target < frame_end)
                        {
                            Some(target) => Self::eval_selected_row(
                                &self.ctx,
                                &mut self.rows,
                                self.value_suites[position]
                                    .arg
                                    .as_ref()
                                    .expect("value argument suite"),
                                target,
                            )?,
                            None => Datum::Null,
                        }
                    }
                    WindowFunction::Relative { offset, lead, .. } => {
                        let target = usize::try_from(*offset).ok().and_then(|offset| {
                            if *lead {
                                index.checked_add(offset)
                            } else {
                                index.checked_sub(offset)
                            }
                        });
                        match target
                            .filter(|target| *target >= partition_start && *target < partition_end)
                        {
                            Some(target) => Self::eval_selected_row(
                                &self.ctx,
                                &mut self.rows,
                                self.value_suites[position]
                                    .arg
                                    .as_ref()
                                    .expect("value argument suite"),
                                target,
                            )?,
                            None => match &self.value_suites[position].default {
                                Some(default) => Self::eval_selected_row(
                                    &self.ctx,
                                    &mut self.rows,
                                    default,
                                    index,
                                )?,
                                None => Datum::Null,
                            },
                        }
                    }
                    WindowFunction::Ntile(None | Some(0)) => Datum::Null,
                    WindowFunction::Ntile(Some(buckets)) => {
                        // Go func_ntile.go: the first remainder buckets each
                        // have one more row than the quotient.
                        let quotient = count as u64 / buckets;
                        let remainder = count as u64 % buckets;
                        let position = (index - partition_start) as u64;
                        let wide_rows = (quotient + 1) * remainder;
                        let bucket = if position < wide_rows {
                            position / (quotient + 1) + 1
                        } else {
                            remainder + (position - wide_rows) / quotient + 1
                        };
                        Datum::UInt(bucket)
                    }
                };
                // Go's valueEvaluator reads EvalString rather than the enum/
                // set's encoded datum. Keep the label bytes in a string result.
                let value = if matches!(
                    spec.func,
                    WindowFunction::Value { .. } | WindowFunction::Relative { .. }
                ) && spec.output_type.eval_type() == tidb_datatype::EvalType::String
                    && !value.is_null()
                {
                    Datum::Bytes(value.sql_bytes().map_err(|_| {
                        ExecError::internal("window value cannot be read as a string")
                    })?)
                } else {
                    value
                };
                req.append_datum(self.child_width + position, &value);
            }
            self.emitted += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.rows.reset();
        self.partitions.clear();
        self.partition_of.clear();
        self.child.close()
    }

    fn schema(&self) -> &tidb_expr::schema::Schema {
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
mod selected_key_tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::{column::Column, constant::Constant, scalar_function::ScalarFunction};

    fn key(index: i64) -> Expression {
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut col = Column::new(index + 1, ty.clone());
        col.index = index;
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            ty.clone(),
            vec![
                Expression::Column(col),
                Expression::Constant(Constant::new(Datum::Int(1), ty)),
            ],
        ))
    }

    fn rows(values: &[i64]) -> Chunk {
        let mut rows =
            Chunk::new_with_capacity(&[FieldType::new(FieldTypeCode::LongLong)], values.len());
        for &value in values {
            rows.append_int64(0, value);
        }
        rows
    }

    #[cfg(feature = "tikv-expr")]
    fn seeded_value_exec(
        func: WindowFunction,
        values: &[i64],
        engine: bool,
    ) -> WindowExec<crate::StmtContext> {
        use tidb_expr::schema::Schema;
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let child_meta = ExecutorMeta::new(Schema::new(vec![Column::new(1, ty.clone())]), 0, 2, 2);
        let output_meta = ExecutorMeta::new(
            Schema::new(vec![Column::new(1, ty.clone()), Column::new(2, ty.clone())]),
            1,
            2,
            2,
        );
        let mut exec = WindowExec::new(
            output_meta,
            vec![WindowFuncSpec {
                func,
                output_type: ty,
            }],
            vec![],
            vec![],
            WindowFrameSpec {
                start: WindowBound::Unbounded,
                end: WindowBound::Unbounded,
                range: None,
                range_desc: false,
            },
            Box::new(crate::table_dual::TableDualExec::new(child_meta, 0)),
            crate::StmtContext::for_query().with_tikv_expression(engine),
            1,
        );
        // Seed the already-drained private buffer so these tests exercise the
        // real emission path independently of child fetching/key comparison.
        exec.rows = rows(values);
        exec.partition_of = vec![(0, values.len()); values.len()];
        exec.peers = vec![(0, values.len(), 1); values.len()];
        exec.fetched = true;
        exec
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn aggregate_frames_reuse_program_but_not_accumulator_or_future_rows() {
        for engine in [false, true] {
            let mut exec = seeded_value_exec(
                WindowFunction::Aggregate(AggFunc::new(
                    crate::hash_agg::AggKind::Max,
                    Some(key(0)),
                )),
                &[8, 4, 6, i64::MAX],
                engine,
            );
            exec.frame.start = WindowBound::Offset {
                num: 1,
                preceding: true,
            };
            exec.frame.end = WindowBound::CurrentRow;
            let mut output = exec.new_chunk();
            output.set_required_rows(1, 2);
            for (index, expected) in [9, 9, 7].into_iter().enumerate() {
                exec.next(&mut output).unwrap();
                assert_eq!(output.num_rows(), 1);
                assert_eq!(output.get_row(0).get_int64(1), expected);
                assert_eq!(
                    exec.ctx.tikv_expression_rows(),
                    if engine { (index * 2 + 1) as u64 } else { 0 }
                );
                assert_eq!(
                    exec.value_suites[0]
                        .aggregate
                        .as_ref()
                        .unwrap()
                        .compilations(),
                    if engine { 1 } else { 0 }
                );
            }
            // The bad row is demanded only by this fourth frame. Prior frames
            // succeeded despite sharing the same physical backing chunk.
            assert!(exec.next(&mut output).is_err());
            assert_eq!(exec.ctx.tikv_expression_rows(), if engine { 6 } else { 0 });
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn aggregate_empty_and_first_row_frames_skip_unused_errors() {
        for engine in [false, true] {
            let mut empty = seeded_value_exec(
                WindowFunction::Aggregate(AggFunc::new(
                    crate::hash_agg::AggKind::Max,
                    Some(key(0)),
                )),
                &[i64::MAX],
                engine,
            );
            empty.frame.start = WindowBound::Offset {
                num: 1,
                preceding: false,
            };
            empty.frame.end = empty.frame.start;
            let mut output = empty.new_chunk();
            empty.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 1);
            assert!(output.get_row(0).is_null(1));
            assert_eq!(empty.ctx.tikv_expression_rows(), 0);
            assert_eq!(
                empty.value_suites[0]
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .compilations(),
                0
            );

            let mut first = seeded_value_exec(
                WindowFunction::Aggregate(AggFunc::new(
                    crate::hash_agg::AggKind::FirstRow,
                    Some(key(0)),
                )),
                &[2, i64::MAX],
                engine,
            );
            first.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 2);
            for row in 0..2 {
                assert_eq!(output.get_row(row).get_int64(1), 3);
            }
            assert_eq!(first.ctx.tikv_expression_rows(), if engine { 2 } else { 0 });
            assert_eq!(
                first.value_suites[0]
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .compilations(),
                if engine { 1 } else { 0 }
            );
        }
    }

    #[cfg(feature = "tikv-expr")]
    fn set_range(
        exec: &mut WindowExec<crate::StmtContext>,
        bound: tidb_planner::logical::window::FrameBound,
        descending: bool,
    ) {
        // Use the same bound at both ends to test inclusive/exclusive scans.
        exec.order_by = bound.compare_cols.clone();
        exec.order_suites = WindowExec::<crate::StmtContext>::key_suites(&exec.order_by);
        exec.frame.range = Some(tidb_planner::logical::window::WindowFrame {
            frame_type: tidb_planner::logical::window::FrameType::Ranges,
            start: Some(bound.clone()),
            end: Some(bound),
        });
        exec.frame.range_desc = descending;
        exec.range_suites = WindowExec::<crate::StmtContext>::range_suites(&exec.frame);
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_range_scans_only_demanded_candidates() {
        use tidb_planner::logical::window::{BoundType, FrameBound};
        for engine in [false, true] {
            for descending in [false, true] {
                let values = if descending {
                    [2, 1, 0, i64::MAX]
                } else {
                    [0, 1, 2, i64::MAX]
                };
                let mut exec = seeded_value_exec(WindowFunction::RowNumber, &values, engine);
                set_range(
                    &mut exec,
                    FrameBound {
                        bound_type: BoundType::Preceding,
                        calc_funcs: vec![key(0)],
                        compare_cols: vec![key(0)],
                        ..Default::default()
                    },
                    descending,
                );
                assert_eq!(exec.frame_range(1, 0, 4).unwrap(), (1, 2));
                assert_eq!(exec.ctx.tikv_expression_rows(), if engine { 7 } else { 0 });
                // The overflowing tail did not run during either bound scan.
                assert!(exec.range_bound(1, 3, 4, false).is_err());
                assert!(exec.rows.sel().is_none());
                assert_eq!(exec.rows.num_rows(), 4);
            }
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_range_comparison_keys_short_circuit() {
        use tidb_planner::logical::window::{BoundType, FrameBound};
        for engine in [false, true] {
            let ty = FieldType::new(FieldTypeCode::LongLong);
            let constant =
                |value| Expression::Constant(Constant::new(Datum::Int(value), ty.clone()));
            let fail = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                ty.clone(),
                vec![constant(i64::MAX), constant(1)],
            ));
            let mut exec = seeded_value_exec(WindowFunction::RowNumber, &[5, 0, 2], engine);
            set_range(
                &mut exec,
                FrameBound {
                    bound_type: BoundType::Following,
                    calc_funcs: vec![key(0), constant(0)],
                    compare_cols: vec![key(0), fail],
                    ..Default::default()
                },
                false,
            );
            // Candidate key 6 is already greater than current key 3. The
            // second comparison expression would overflow if evaluated.
            assert_eq!(exec.range_bound(2, 0, 3, false).unwrap(), 0);
            assert_eq!(exec.ctx.tikv_expression_rows(), if engine { 3 } else { 0 });
            // Equal first keys do demand the erroring second comparison.
            assert!(exec.range_bound(0, 0, 3, false).is_err());
            assert!(exec.rows.sel().is_none());
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_range_peer_and_unbounded_paths_skip_expressions() {
        use tidb_planner::logical::window::{BoundType, FrameBound};
        for engine in [false, true] {
            for unbounded in [false, true] {
                let mut exec =
                    seeded_value_exec(WindowFunction::RowNumber, &[i64::MAX, i64::MAX], engine);
                set_range(
                    &mut exec,
                    FrameBound {
                        bound_type: BoundType::CurrentRow,
                        unbounded,
                        calc_funcs: vec![key(0)],
                        compare_cols: vec![key(0)],
                        ..Default::default()
                    },
                    false,
                );
                assert_eq!(exec.frame_range(0, 0, 2).unwrap(), (0, 2));
                assert_eq!(exec.ctx.tikv_expression_rows(), 0);
                assert!(exec.rows.sel().is_none());
            }
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_value_targets_are_demanded_only_when_present() {
        for engine in [false, true] {
            for (nth, last, expected) in [
                (Some(2), false, Datum::Int(3)),
                (Some(1), true, Datum::Int(3)),
                (Some(0), false, Datum::Null),
                (None, false, Datum::Null),
                (Some(3), false, Datum::Null),
            ] {
                let mut exec = seeded_value_exec(
                    WindowFunction::Value {
                        arg: key(0),
                        nth,
                        last,
                    },
                    &[i64::MAX, 2],
                    engine,
                );
                let mut output = exec.new_chunk();
                exec.next(&mut output).unwrap();
                assert_eq!(output.num_rows(), 2);
                for row in 0..2 {
                    assert_eq!(
                        output.get_row(row).get_datum(1, &exec.funcs[0].output_type),
                        expected
                    );
                }
                assert!(exec.rows.sel().is_none());
                assert_eq!(
                    exec.ctx.tikv_expression_rows(),
                    if engine && !expected.is_null() { 2 } else { 0 }
                );
            }
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_relative_skips_default_until_out_of_partition() {
        for engine in [false, true] {
            for lead in [false, true] {
                let ty = FieldType::new(FieldTypeCode::LongLong);
                let fail = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("plus"),
                    ty.clone(),
                    vec![
                        Expression::Constant(Constant::new(Datum::Int(i64::MAX), ty.clone())),
                        Expression::Constant(Constant::new(Datum::Int(1), ty)),
                    ],
                ));
                // An offset of zero always selects the current argument, so an
                // overflowing default must not run (for both LEAD and LAG).
                let mut exec = seeded_value_exec(
                    WindowFunction::Relative {
                        arg: key(0),
                        offset: 0,
                        default: Some(fail.clone()),
                        lead,
                    },
                    &[1, 2],
                    engine,
                );
                let mut output = exec.new_chunk();
                exec.next(&mut output).unwrap();
                assert_eq!(output.get_row(0).get_int64(1), 2);
                assert_eq!(output.get_row(1).get_int64(1), 3);
                assert_eq!(exec.ctx.tikv_expression_rows(), if engine { 2 } else { 0 });

                let values = if lead { [i64::MAX, 2] } else { [2, i64::MAX] };
                let mut exec = seeded_value_exec(
                    WindowFunction::Relative {
                        arg: key(0),
                        offset: 1,
                        default: Some(fail.clone()),
                        lead,
                    },
                    &values,
                    engine,
                );
                // Choose the row whose target is in-range. Its own argument
                // would overflow: only the target row may be evaluated.
                exec.emitted = if lead { 0 } else { 1 };
                let mut output = exec.new_chunk();
                output.set_required_rows(1, 2);
                exec.next(&mut output).unwrap();
                assert_eq!(output.get_row(0).get_int64(1), 3);
                assert_eq!(exec.ctx.tikv_expression_rows(), if engine { 1 } else { 0 });
                // Now demand the default at the partition boundary.
                exec.emitted = if lead { 1 } else { 0 };
                assert!(exec.next(&mut output).is_err());
                assert!(exec.rows.sel().is_none());
                assert_eq!(exec.rows.num_rows(), 2);

                // With no possible target, evaluate the default on each
                // current row, never the overflowing argument.
                let mut exec = seeded_value_exec(
                    WindowFunction::Relative {
                        arg: fail,
                        offset: u64::MAX,
                        default: Some(key(0)),
                        lead,
                    },
                    &[1, 2],
                    engine,
                );
                let mut output = exec.new_chunk();
                exec.next(&mut output).unwrap();
                assert_eq!(output.get_row(0).get_int64(1), 2);
                assert_eq!(output.get_row(1).get_int64(1), 3);
                assert_eq!(exec.ctx.tikv_expression_rows(), if engine { 2 } else { 0 });
            }
        }
    }

    #[test]
    fn window_key_selection_restores_dense_buffer_without_engine() {
        let ctx = crate::StmtContext::for_query();
        let mut rows = rows(&[1, 1, 2, i64::MAX]);
        let keys = [key(0)];
        let suites = WindowExec::<crate::StmtContext>::key_suites(&keys);
        assert!(WindowExec::same_keys(&ctx, &mut rows, &keys, &suites, 0, 1).unwrap());
        assert!(!WindowExec::same_keys(&ctx, &mut rows, &keys, &suites, 1, 2).unwrap());
        assert!(WindowExec::eval_selected_row(&ctx, &mut rows, &suites[0], 3).is_err());
        assert!(rows.sel().is_none());
        assert_eq!(rows.num_rows(), 4);
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_key_engine_preserves_demand_and_compiled_cache() {
        use std::sync::Arc;
        use tidb_expr::evaluator::EvaluatorProgram;
        let ctx = crate::StmtContext::for_query().with_tikv_expression(true);
        let mut rows = rows(&[1, 1, 2, i64::MAX]);
        let keys = [key(0)];
        let program = Arc::new(EvaluatorProgram::new(keys.to_vec(), true));
        let suites = [EvaluatorSuite::from_program(program.clone())];
        assert!(WindowExec::same_keys(&ctx, &mut rows, &keys, &suites, 0, 1).unwrap());
        assert!(!WindowExec::same_keys(&ctx, &mut rows, &keys, &suites, 1, 2).unwrap());
        assert_eq!(ctx.tikv_expression_rows(), 4);
        assert_eq!(program.tikv_compilations(), 1);
        // The overflowing row was not demanded above. It errors only now.
        assert!(WindowExec::eval_selected_row(&ctx, &mut rows, &suites[0], 3).is_err());
        assert!(rows.sel().is_none());
        assert_eq!(rows.num_rows(), 4);
        assert_eq!(
            WindowExec::eval_selected_row(&ctx, &mut rows, &suites[0], 0).unwrap(),
            Datum::Int(2)
        );
        assert_eq!(program.tikv_compilations(), 1);
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn window_key_mismatch_skips_later_erroring_key() {
        let ctx = crate::StmtContext::for_query().with_tikv_expression(true);
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut rows = Chunk::new_with_capacity(&[ty.clone(), ty], 2);
        for value in [1, 2] {
            rows.append_int64(0, value);
            rows.append_int64(1, i64::MAX);
        }
        let keys = [key(0), key(1)];
        let suites = WindowExec::<crate::StmtContext>::key_suites(&keys);
        assert!(!WindowExec::same_keys(&ctx, &mut rows, &keys, &suites, 0, 1).unwrap());
        assert_eq!(ctx.tikv_expression_rows(), 2);
        assert!(rows.sel().is_none());
        assert!(WindowExec::eval_selected_row(&ctx, &mut rows, &suites[1], 0).is_err());
        assert!(rows.sel().is_none());
    }
}
