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

//! Explicit runtime-only signed PLUS row slice. Bound-tree preparation does
//! not undo effects from earlier SqlBuild. No evaluator route, native replay,
//! error formatter, or structural-grammar expansion is installed here.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::tikv_compat::value::{
    from_scalar, snapshot_field_type, to_scalar, ValueMetadata,
};
use tidb_datatype::{Datum, DatumKind, FieldType};
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::{codec::data_type::VectorValue, expr::EvalContext, EvalType};
use tidb_query_expr::local::{
    compile_local_profiled, CallMetadata, CompileLimits, ExecutionLimits, FunctionRef, InputRow,
    LiteralKind, LocalCompileContext, LocalError, LocalExpr, LocalProgram, LocalResult,
    LocalRuntimeServices, OrdinaryCallSite, OrdinaryProfile, OrdinaryProfileSpec, OrdinarySourceId,
};

use crate::expression::Expression;

use super::context::NativeInputs;
use super::lower::{self, ColumnBinding, LoweredSpec, NodeMetadata, SourceNode};
use super::{catalog, SeedError, SeedResult};

/// Non-executable source shape. Display names are not execution identities.
/// No source text is normalized or rendered at this runtime-only boundary.
enum SourceShape {
    Literal(Option<i64>),
    Column,
    Call {
        display_name: tidb_ast::CiString,
        children: [usize; 2],
    },
}

struct ComputedOutput {
    identity: ValueMetadata,
    sql_type: FieldType,
}

/// Opaque immutable facts; no recursive Clone/Debug or native expressions.
pub(crate) struct LoweredIntPlusRow {
    core: LoweredSpec,
    profiles: OrdinaryProfileSpec,
    shapes: Box<[SourceShape]>,
    outputs: Box<[Option<ComputedOutput>]>,
    // An input slot identifies a source leaf, not its nearest enclosing call.
    binding_nodes: Box<[usize]>,
}

pub(crate) fn lower_typed_int_plus_row(
    root: &Expression,
    row_schema: &[FieldType],
    new_collation: bool,
    source_unit: u64,
    limits: CompileLimits,
) -> SeedResult<Arc<LoweredIntPlusRow>> {
    lower_plus(root, None, row_schema, new_collation, source_unit, limits)
}

/// Verify against the trusted original input supplied by the front-end.
/// Node-local PbOrigin alone does not prove parent/child ancestry. The wire
/// tree is borrowed during this walk only; it is not decoded or cloned here.
pub(crate) fn lower_pb_int_plus_row(
    root: &Expression,
    original_wire: &db_pb::Expr,
    row_schema: &[FieldType],
    new_collation: bool,
    source_unit: u64,
    limits: CompileLimits,
) -> SeedResult<Arc<LoweredIntPlusRow>> {
    lower_plus(
        root,
        Some(original_wire),
        row_schema,
        new_collation,
        source_unit,
        limits,
    )
}

enum Step<'a> {
    Visit {
        node: &'a Expression,
        wire: Option<&'a db_pb::Expr>,
        depth: usize,
    },
    Finish {
        ordinal: usize,
        selected: FunctionRef,
        field_type: tipb::FieldType,
    },
}

fn lower_plus(
    root: &Expression,
    original_wire: Option<&db_pb::Expr>,
    row_schema: &[FieldType],
    new_collation: bool,
    source_unit: u64,
    limits: CompileLimits,
) -> SeedResult<Arc<LoweredIntPlusRow>> {
    if limits.max_nodes == 0 || limits.max_depth == 0 {
        return Err(LocalError::ResourceLimit("empty PLUS lowering budget".into()).into());
    }
    if !matches!(root, Expression::ScalarFunction(_)) {
        return Err(SeedError::Admission("PLUS row requires a PLUS root"));
    }
    let consumer = if original_wire.is_some() {
        OrdinaryProfile::PbRow
    } else {
        OrdinaryProfile::TypedRow
    };
    let mut pending = vec![Step::Visit {
        node: root,
        wire: original_wire,
        depth: 1,
    }];
    let mut scheduled = 1usize;
    let mut built: Vec<(LocalExpr, usize)> = Vec::new();
    let mut nodes = Vec::new();
    let mut shapes = Vec::new();
    let mut outputs = Vec::new();
    let mut sites = Vec::new();
    let mut bindings = Vec::new();
    let mut binding_nodes = Vec::new();
    let mut schema = Vec::new();
    while let Some(step) = pending.pop() {
        match step {
            Step::Finish {
                ordinal,
                selected,
                field_type,
            } => {
                let (right, right_node) = built
                    .pop()
                    .ok_or(SeedError::Admission("missing PLUS right child"))?;
                let (left, left_node) = built
                    .pop()
                    .ok_or(SeedError::Admission("missing PLUS left child"))?;
                let Some(SourceShape::Call { children, .. }) = shapes.get_mut(ordinal) else {
                    return Err(SeedError::Admission("invalid PLUS source shape"));
                };
                *children = [left_node, right_node];
                built.push((
                    LocalExpr::Call {
                        function: selected,
                        args: vec![left, right].into_boxed_slice(),
                        return_type: field_type,
                        metadata: CallMetadata::None,
                    },
                    ordinal,
                ));
            }
            Step::Visit { node, wire, depth } => {
                if depth > limits.max_depth {
                    return Err(
                        LocalError::ResourceLimit("PLUS lowering depth exceeded".into()).into(),
                    );
                }
                let ordinal = nodes.len();
                let (sql, collation) = match node {
                    Expression::Constant(node) => (node.ret_type.as_ref(), &node.collation),
                    Expression::Column(node) => (node.ret_type.as_ref(), &node.collation),
                    Expression::ScalarFunction(node) => (node.ret_type.as_ref(), &node.collation),
                    Expression::CorrelatedColumn(_) => {
                        return Err(SeedError::Admission("correlated PLUS input"))
                    }
                };
                let sql = sql.ok_or(SeedError::Admission("missing PLUS node type"))?;
                lower::signed_longlong(sql)?;
                let origin = lower::origin(node);
                match (wire, origin) {
                    (Some(wire), Some(origin)) => {
                        lower::validate_origin(node, sql, origin)?;
                        if origin.expr_type != wire.tp
                            || origin.signature != wire.sig
                            || origin.field_type != wire.field_type
                            || origin.val != wire.val
                            || origin.child_count != wire.children.len()
                        {
                            return Err(SeedError::Admission(
                                "PLUS node differs from retained original PB input",
                            ));
                        }
                    }
                    (None, None) => {}
                    _ => {
                        return Err(SeedError::Admission(
                            "PLUS input lost homogeneous ingestion provenance",
                        ))
                    }
                }
                let mut output = None;
                let (source, shape) = match node {
                    Expression::Constant(constant) => {
                        let value = constant.literal_value().ok_or(SeedError::Admission(
                            "PLUS constant is deferred or parameterized",
                        ))?;
                        let literal = match value {
                            Datum::Int(value) => Some(*value),
                            Datum::Null => None,
                            _ => {
                                return Err(SeedError::Admission(
                                    "PLUS literal is not native Int or typed NULL",
                                ))
                            }
                        };
                        let field_type =
                            lower::projected_type(sql, origin.map(Arc::as_ref), new_collation)?;
                        let (value, metadata) = to_scalar(value, EvalType::Int)?;
                        built.push((
                            LocalExpr::Constant {
                                value,
                                field_type,
                                literal_kind: LiteralKind::Typed,
                            },
                            ordinal,
                        ));
                        (
                            SourceNode::Literal {
                                kind: metadata.kind,
                                subquery_ref_id: constant.subquery_ref_id,
                            },
                            SourceShape::Literal(literal),
                        )
                    }
                    Expression::Column(column) => {
                        if column.virtual_expr.is_some() || column.correlated_col_unique_id != 0 {
                            return Err(SeedError::Admission("virtual/correlated PLUS column"));
                        }
                        let index = usize::try_from(column.index)
                            .map_err(|_| SeedError::Admission("negative PLUS column index"))?;
                        if row_schema.get(index) != Some(sql) {
                            return Err(SeedError::Admission(
                                "PLUS column differs from declared row schema",
                            ));
                        }
                        let field_type =
                            lower::projected_type(sql, origin.map(Arc::as_ref), new_collation)?;
                        let slot = bindings.len();
                        bindings.push(ColumnBinding {
                            index,
                            sql_type: snapshot_field_type(sql),
                        });
                        binding_nodes.push(ordinal);
                        schema.push(field_type.clone());
                        built.push((LocalExpr::InputSlot { slot, field_type }, ordinal));
                        (
                            SourceNode::Column {
                                index,
                                id: column.id,
                                unique_id: column.unique_id,
                                original_name: column.orig_name.clone(),
                                hidden: column.is_hidden,
                                prefix: column.is_prefix,
                                in_operand: column.in_operand,
                                correlated_unique_id: column.correlated_col_unique_id,
                            },
                            SourceShape::Column,
                        )
                    }
                    Expression::ScalarFunction(function) => {
                        // The actual selected native path must agree with ingress.
                        if function.pb_signature().is_some() != wire.is_some() {
                            return Err(SeedError::Admission(
                                "PLUS native path differs from ingress",
                            ));
                        }
                        let selected = catalog::int_plus_row(function)?;
                        scheduled = scheduled
                            .checked_add(2)
                            .filter(|count| *count <= limits.max_nodes)
                            .ok_or_else(|| {
                                LocalError::ResourceLimit(
                                    "PLUS lowering node budget exceeded".into(),
                                )
                            })?;
                        let next = depth
                            .checked_add(1)
                            .filter(|next| *next <= limits.max_depth)
                            .ok_or_else(|| {
                                LocalError::ResourceLimit("PLUS lowering depth exceeded".into())
                            })?;
                        let field_type =
                            lower::projected_type(sql, origin.map(Arc::as_ref), new_collation)?;
                        let source = OrdinarySourceId::new(
                            source_unit,
                            u64::try_from(ordinal).map_err(|_| {
                                LocalError::ResourceLimit("PLUS source ordinal out of range".into())
                            })?,
                        );
                        sites.push(if let Some(origin) = origin {
                            OrdinaryCallSite::pb_row(
                                ordinal,
                                source,
                                origin
                                    .signature
                                    .ok_or(SeedError::Admission("missing PLUS wire signature"))?,
                            )
                        } else {
                            OrdinaryCallSite::typed_row(ordinal, source)
                        });
                        output = Some(ComputedOutput {
                            identity: ValueMetadata {
                                kind: DatumKind::Int,
                                string_collation: None,
                                decimal_declared_shape: None,
                            },
                            sql_type: snapshot_field_type(sql),
                        });
                        pending.push(Step::Finish {
                            ordinal,
                            selected,
                            field_type,
                        });
                        for (index, child) in function.args.iter().enumerate().rev() {
                            let child_wire = match wire {
                                Some(wire) => Some(wire.children.get(index).ok_or(
                                    SeedError::Admission("missing original PLUS PB child"),
                                )?),
                                None => None,
                            };
                            pending.push(Step::Visit {
                                node: child,
                                wire: child_wire,
                                depth: next,
                            });
                        }
                        (
                            SourceNode::Call { selected },
                            SourceShape::Call {
                                display_name: function.func_name.clone(),
                                children: [0; 2],
                            },
                        )
                    }
                    Expression::CorrelatedColumn(_) => unreachable!(),
                };
                nodes.push(NodeMetadata {
                    sql_type: snapshot_field_type(sql),
                    wire_origin: origin.map(Arc::clone),
                    collation: collation.into(),
                    source,
                });
                shapes.push(shape);
                outputs.push(output);
            }
        }
    }
    if built.len() != 1 {
        return Err(SeedError::Admission(
            "PLUS lowering did not produce one root",
        ));
    }
    let (expr, ordinal) = built
        .pop()
        .ok_or(SeedError::Admission("missing PLUS root"))?;
    if ordinal != 0 {
        return Err(SeedError::Admission("invalid PLUS root ordinal"));
    }
    let profiles = OrdinaryProfileSpec::new(&expr, &schema, consumer, sites, limits)?;
    let output_type = snapshot_field_type(&nodes[0].sql_type);
    Ok(Arc::new(LoweredIntPlusRow {
        core: LoweredSpec {
            expr,
            schema: schema.into_boxed_slice(),
            bindings: bindings.into_boxed_slice(),
            row_schema: row_schema
                .iter()
                .map(snapshot_field_type)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            nodes: nodes.into_boxed_slice(),
            output_type,
            new_collation,
            limits,
        },
        profiles,
        shapes: shapes.into_boxed_slice(),
        outputs: outputs.into_boxed_slice(),
        binding_nodes: binding_nodes.into_boxed_slice(),
    }))
}

// A raw native leaf boundary, never an expression-evaluation callback. Only the
// checked Chunk implementation is used in production; tests supply raw Datums.
trait NativeDatumSource {
    fn schema(&self) -> &[tipb::FieldType];
    fn read_datum(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum>;
}
impl NativeDatumSource for NativeInputs<'_> {
    fn schema(&self) -> &[tipb::FieldType] {
        self.binding_schema()
    }
    fn read_datum(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        self.read_native_datum(slot, row, expected)
    }
}

struct PlusInputs<'a, T: NativeDatumSource + ?Sized>(&'a mut T);
impl<T: NativeDatumSource + ?Sized> LocalRuntimeServices for PlusInputs<'_, T> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        self.0.schema()
    }
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        let value = self.0.read_datum(ctx, slot, row, expected)?;
        // UInt is already indistinguishable from Int AFTER to_scalar. Do not
        // move this guard to a VectorValue boundary or pre-read dead slots.
        if !matches!(value, Datum::Int(_) | Datum::Null) {
            return Err(LocalError::BindingContract(
                "PLUS demanded a non-Int native datum".into(),
            ));
        }
        let (value, _) = to_scalar(&value, EvalType::Int)
            .map_err(|error| LocalError::BindingContract(error.to_string()))?;
        Ok(VectorValue::from_scalar(&value, 1))
    }
}

pub(crate) struct PreparedIntPlusRow {
    spec: Arc<LoweredIntPlusRow>,
    program: LocalProgram,
    limits: ExecutionLimits,
}
impl PreparedIntPlusRow {
    pub(crate) fn compile(
        spec: Arc<LoweredIntPlusRow>,
        limits: ExecutionLimits,
    ) -> SeedResult<Self> {
        let program = compile_local_profiled(
            &spec.core.expr,
            &spec.core.schema,
            LocalCompileContext {
                limits: spec.core.limits,
            },
            &spec.profiles,
        )?;
        Ok(Self {
            spec,
            program,
            limits,
        })
    }

    pub(crate) fn eval_selected(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        selection: &[usize],
    ) -> SeedResult<Vec<Datum>> {
        let mut native = NativeInputs::new(&self.spec.core, chunk, row_schema, selection)?;
        let mut inputs = PlusInputs(&mut native);
        let output = self.program.eval_with_bindings(
            self.limits,
            ctx,
            chunk.physical_rows(),
            selection,
            &mut inputs,
        )?;
        self.materialize(output, selection.len())
    }

    pub(crate) fn eval_one(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        physical_row: usize,
    ) -> SeedResult<Datum> {
        self.eval_selected(ctx, chunk, row_schema, &[physical_row])?
            .pop()
            .ok_or_else(|| {
                LocalError::BindingContract("missing PLUS singleton result".into()).into()
            })
    }

    fn materialize(&self, output: VectorValue, count: usize) -> SeedResult<Vec<Datum>> {
        if output.eval_type() != EvalType::Int || output.len() != count {
            return Err(LocalError::BindingContract("invalid PLUS result shape".into()).into());
        }
        let record =
            self.spec
                .outputs
                .first()
                .and_then(Option::as_ref)
                .ok_or(SeedError::Admission(
                    "missing PLUS computed output identity",
                ))?;
        (0..count)
            .map(|index| {
                from_scalar(
                    output.get_scalar_ref(index),
                    EvalType::Int,
                    &record.identity,
                )
                .map_err(Into::into)
            })
            .collect()
    }

    #[cfg(test)]
    fn eval_test_native(
        &mut self,
        ctx: &mut EvalContext,
        physical_rows: usize,
        selection: &[usize],
        source: &mut impl NativeDatumSource,
    ) -> SeedResult<Vec<Datum>> {
        let mut inputs = PlusInputs(source);
        let output = self.program.eval_with_bindings(
            self.limits,
            ctx,
            physical_rows,
            selection,
            &mut inputs,
        )?;
        self.materialize(output, selection.len())
    }
}

#[path = "ordinary_diagnostics.rs"]
mod diagnostics;

#[cfg(test)]
#[path = "ordinary_tests.rs"]
mod tests;
