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

//! Iterative, value-free lowering of an already bound/inferred typed tree.
//! Literal Int/NULL transport is infallible in the admitted domain; dynamic
//! columns are only described here, never read. No SQL rewrite/fold runs here.

use std::sync::Arc;

use tidb_datatype::tikv_compat::value::{project_field_type, snapshot_field_type, to_scalar};
use tidb_datatype::{Datum, DatumKind, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::EvalType;
use tidb_query_expr::local::{
    CallMetadata, CompileLimits, FunctionRef, LiteralKind, LocalError, LocalExpr,
};

use crate::distsql_builtin::PbOrigin;
use crate::expr_collation::{Coercibility, CollationInfo, Repertoire};
use crate::expression::Expression;

use super::{catalog, SeedError, SeedResult};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct CollationSnapshot {
    pub coercibility: Coercibility,
    pub initialized: bool,
    pub repertoire: Repertoire,
    pub charset: String,
    pub collation: String,
    pub explicit_charset: bool,
}

impl From<&CollationInfo> for CollationSnapshot {
    fn from(info: &CollationInfo) -> Self {
        let (charset, collation) = info.charset_and_collation();
        Self {
            coercibility: info.coercibility(),
            initialized: info.has_coercibility(),
            repertoire: info.repertoire(),
            charset: charset.to_owned(),
            collation: collation.to_owned(),
            explicit_charset: info.is_explicit_charset(),
        }
    }
}

#[derive(Debug)]
pub(super) enum SourceNode {
    Literal {
        kind: DatumKind,
        subquery_ref_id: i64,
    },
    Column {
        index: usize,
        id: i64,
        unique_id: i64,
        original_name: String,
        hidden: bool,
        prefix: bool,
        in_operand: bool,
        correlated_unique_id: i64,
    },
    Call {
        selected: FunctionRef,
    },
}

// Sidecars contain no executable children, native evaluator or lazy cache.
#[derive(Debug)]
pub(super) struct NodeMetadata {
    pub sql_type: FieldType,
    pub wire_origin: Option<Arc<PbOrigin>>,
    pub collation: CollationSnapshot,
    pub source: SourceNode,
}

pub(super) struct ColumnBinding {
    pub index: usize,
    pub sql_type: FieldType,
}

// No derived Clone/Debug: LocalExpr's derived implementations recurse. The
// executable graph's custom iterative Drop is preserved by sharing with Arc.
pub(crate) struct LoweredSpec {
    pub(super) expr: LocalExpr,
    pub(super) schema: Box<[tipb::FieldType]>,
    pub(super) bindings: Box<[ColumnBinding]>,
    pub(super) row_schema: Box<[FieldType]>,
    pub(super) nodes: Box<[NodeMetadata]>,
    pub(super) output_type: FieldType,
    pub(super) new_collation: bool,
    pub(super) limits: CompileLimits,
}

pub(super) fn signed_longlong(sql: &FieldType) -> SeedResult<()> {
    if sql.code() != FieldTypeCode::LongLong || sql.is_unsigned() || sql.is_array() {
        return Err(SeedError::Admission(
            "IntControlSeed requires actual signed LongLong types",
        ));
    }
    Ok(())
}

pub(super) fn origin(node: &Expression) -> Option<&Arc<PbOrigin>> {
    match node {
        Expression::Constant(node) => node.pb_origin(),
        Expression::Column(node) => node.pb_origin(),
        Expression::ScalarFunction(node) => node.pb_origin(),
        Expression::CorrelatedColumn(_) => None,
    }
}

/// Validate provenance against the CURRENT node, not its display name. The seed
/// conservatively rejects transformed PB types/offsets/literals and replacement
/// children without origins; it never reconstructs missing protocol metadata.
pub(super) fn validate_origin(
    node: &Expression,
    sql: &FieldType,
    origin: &PbOrigin,
) -> SeedResult<()> {
    if !origin.matches_effective_type(sql) {
        return Err(SeedError::Admission(
            "PB effective type changed since ingestion",
        ));
    }
    let wire = origin.field_type.as_ref().ok_or(SeedError::Admission(
        "PB node has no original wire FieldType",
    ))?;
    if wire.tp != Some(i32::from(FieldTypeCode::LongLong.mysql_type()))
        || wire.flag() & FieldTypeFlags::UNSIGNED != 0
        || wire.array()
    {
        return Err(SeedError::Admission(
            "original PB type is outside signed LongLong seed",
        ));
    }
    let bytes = origin.val.as_deref().unwrap_or_default();
    let leaf_value = || {
        tidb_codec::decode_int(bytes)
            .ok()
            .filter(|(tail, _)| tail.is_empty())
            .map(|(_, value)| value)
    };
    let valid = match node {
        Expression::Column(column) => {
            origin.expr_type == Some(db_pb::ExprType::ColumnRef as i32)
                && origin.signature.is_none()
                && origin.child_count == 0
                && leaf_value() == Some(column.index)
        }
        Expression::Constant(constant) => {
            origin.signature.is_none()
                && origin.child_count == 0
                && match constant.literal_value() {
                    Some(Datum::Int(value)) => {
                        origin.expr_type == Some(db_pb::ExprType::Int64 as i32)
                            && leaf_value() == Some(*value)
                    }
                    Some(Datum::Null) => {
                        origin.expr_type == Some(db_pb::ExprType::Null as i32) && bytes.is_empty()
                    }
                    _ => false,
                }
        }
        Expression::ScalarFunction(function) => {
            origin.expr_type == Some(db_pb::ExprType::ScalarFunc as i32)
                && origin.signature == function.pb_signature().map(|sig| sig as i32)
                && origin.signature.is_some()
                && origin.child_count == function.args.len()
                && bytes.is_empty()
        }
        Expression::CorrelatedColumn(_) => false,
    };
    if !valid {
        return Err(SeedError::Admission(
            "PB origin is missing, stale, or has unadmitted metadata",
        ));
    }
    Ok(())
}

pub(super) fn projected_type(
    sql: &FieldType,
    origin: Option<&PbOrigin>,
    new_collation: bool,
) -> SeedResult<tipb::FieldType> {
    let signed_id = if let Some(origin) = origin {
        let wire = origin
            .field_type
            .as_ref()
            .ok_or(SeedError::Admission("missing original PB type"))?;
        // The raw optional field remains in the sidecar. 0 here is the protocol
        // default, not a guessed ID obtained by reserializing the SQL type.
        let id = wire.collate();
        if id != 0 {
            let positive = id
                .checked_abs()
                .ok_or(SeedError::Admission("invalid signed collation ID"))?;
            tidb_datatype::get_collation_by_id(positive)
                .map_err(|_| SeedError::Admission("unknown original PB collation ID"))?;
        }
        id
    } else {
        let row = tidb_datatype::get_collation_by_name(sql.collation_name())
            .map_err(|_| SeedError::Admission("unknown SQL collation name"))?;
        if row.id <= 0 {
            return Err(SeedError::Admission(
                "SQL registry collation ID is not positive",
            ));
        }
        if new_collation {
            -row.id
        } else {
            row.id
        }
    };
    Ok(project_field_type(sql, signed_id)?)
}

enum Step<'a> {
    Visit {
        node: &'a Expression,
        depth: usize,
        needs_origin: bool,
    },
    Call {
        function: FunctionRef,
        arity: usize,
        return_type: tipb::FieldType,
    },
}

pub(crate) fn lower_int_control_seed(
    root: &Expression,
    row_schema: &[FieldType],
    new_collation: bool,
    limits: CompileLimits,
) -> SeedResult<Arc<LoweredSpec>> {
    if limits.max_nodes == 0 || limits.max_depth == 0 {
        return Err(LocalError::ResourceLimit("empty lowering node/depth budget".into()).into());
    }
    let mut pending = vec![Step::Visit {
        node: root,
        depth: 1,
        needs_origin: false,
    }];
    let mut enqueued = 1usize;
    let mut built = Vec::new();
    let mut nodes = Vec::new();
    let mut bindings = Vec::new();
    let mut schema = Vec::new();
    while let Some(step) = pending.pop() {
        match step {
            Step::Call {
                function,
                arity,
                return_type,
            } => {
                let start = built
                    .len()
                    .checked_sub(arity)
                    .ok_or(SeedError::Admission("invalid lowering stack"))?;
                let args = built.drain(start..).collect::<Vec<_>>().into_boxed_slice();
                built.push(LocalExpr::Call {
                    function,
                    args,
                    return_type,
                    metadata: CallMetadata::None,
                });
            }
            Step::Visit {
                node,
                depth,
                needs_origin,
            } => {
                if depth > limits.max_depth {
                    return Err(
                        LocalError::ResourceLimit("lowering depth budget exceeded".into()).into(),
                    );
                }
                let (sql, collation) = match node {
                    Expression::Constant(node) => (node.ret_type.as_ref(), &node.collation),
                    Expression::Column(node) => (node.ret_type.as_ref(), &node.collation),
                    Expression::ScalarFunction(node) => (node.ret_type.as_ref(), &node.collation),
                    Expression::CorrelatedColumn(_) => {
                        return Err(SeedError::Admission(
                            "correlated bindings are not in IntControlSeed",
                        ))
                    }
                };
                let sql = sql.ok_or(SeedError::Admission("missing effective SQL FieldType"))?;
                signed_longlong(sql)?;
                let wire = origin(node);
                if needs_origin && wire.is_none() {
                    return Err(SeedError::Admission("PB subtree lost ingestion provenance"));
                }
                if let Some(wire) = wire {
                    validate_origin(node, sql, wire)?;
                }
                let kernel_type = projected_type(sql, wire.map(Arc::as_ref), new_collation)?;
                let source = match node {
                    Expression::Constant(constant) => {
                        let value = constant.literal_value().ok_or(SeedError::Admission(
                            "parameter/deferred constant is not a literal",
                        ))?;
                        if !matches!(value, Datum::Int(_) | Datum::Null) {
                            return Err(SeedError::Admission(
                                "literal is not a signed Int or typed Int NULL",
                            ));
                        }
                        let (value, metadata) = to_scalar(value, EvalType::Int)?;
                        built.push(LocalExpr::Constant {
                            value,
                            field_type: kernel_type,
                            literal_kind: LiteralKind::Typed,
                        });
                        SourceNode::Literal {
                            kind: metadata.kind,
                            subquery_ref_id: constant.subquery_ref_id,
                        }
                    }
                    Expression::Column(column) => {
                        if column.virtual_expr.is_some() {
                            return Err(SeedError::Admission(
                                "virtual column expression is outside IntControlSeed",
                            ));
                        }
                        let index = usize::try_from(column.index)
                            .map_err(|_| SeedError::Admission("negative column index"))?;
                        if row_schema.get(index) != Some(sql) {
                            return Err(SeedError::Admission(
                                "column SQL type/index differs from declared row schema",
                            ));
                        }
                        let slot = bindings.len();
                        bindings.push(ColumnBinding {
                            index,
                            sql_type: snapshot_field_type(sql),
                        });
                        schema.push(kernel_type.clone());
                        built.push(LocalExpr::InputSlot {
                            slot,
                            field_type: kernel_type,
                        });
                        SourceNode::Column {
                            index,
                            id: column.id,
                            unique_id: column.unique_id,
                            original_name: column.orig_name.clone(),
                            hidden: column.is_hidden,
                            prefix: column.is_prefix,
                            in_operand: column.in_operand,
                            correlated_unique_id: column.correlated_col_unique_id,
                        }
                    }
                    Expression::ScalarFunction(function) => {
                        let selected = catalog::int_control(function)?;
                        enqueued = enqueued
                            .checked_add(function.args.len())
                            .filter(|count| *count <= limits.max_nodes)
                            .ok_or_else(|| {
                                LocalError::ResourceLimit("lowering node budget exceeded".into())
                            })?;
                        pending.push(Step::Call {
                            function: selected,
                            arity: function.args.len(),
                            return_type: kernel_type,
                        });
                        for child in function.args.iter().rev() {
                            pending.push(Step::Visit {
                                node: child,
                                depth: depth + 1,
                                needs_origin: needs_origin || wire.is_some(),
                            });
                        }
                        SourceNode::Call { selected }
                    }
                    Expression::CorrelatedColumn(_) => unreachable!(),
                };
                nodes.push(NodeMetadata {
                    sql_type: snapshot_field_type(sql),
                    wire_origin: wire.map(Arc::clone),
                    collation: collation.into(),
                    source,
                });
            }
        }
    }
    if built.len() != 1 {
        return Err(SeedError::Admission("lowering did not produce one root"));
    }
    let output_type = snapshot_field_type(&nodes[0].sql_type);
    Ok(Arc::new(LoweredSpec {
        expr: built
            .pop()
            .ok_or(SeedError::Admission("missing lowered root"))?,
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
    }))
}
