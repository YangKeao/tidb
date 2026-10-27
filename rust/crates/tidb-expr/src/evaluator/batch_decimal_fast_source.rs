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

//! Factory-only native Decimal priority stage. The source owns immutable
//! import facts, never an expression evaluator or a callback into one.
use super::*;
use crate::scalar_function::{DecimalValue, NativeIntegerDecimalCast};
use tidb_datatype::FieldTypeCode;
use tidb_query_expr::local::HostSignature;
use tidb_query_expr::NativeDecimalFastOutcome as Outcome;

#[derive(Debug)]
pub(super) enum FastInput {
    Constant(Outcome),
    Column {
        index: usize,
        unsigned: bool,
        decimal: bool,
    },
    IntegerCast {
        index: usize,
        shape: NativeIntegerDecimalCast,
    },
}
impl FastInput {
    pub(super) fn read(&self, input: &Chunk, row: usize) -> LocalResult<Outcome> {
        let (index, decimal, cast, unsigned) = match self {
            Self::Constant(value) => return Ok(value.clone()),
            Self::Column {
                index,
                unsigned,
                decimal,
            } => (*index, *decimal, None, *unsigned),
            Self::IntegerCast { index, shape } => (*index, false, Some(*shape), shape.unsigned),
        };
        let column = input.column(index);
        if column.is_null(row) {
            return Ok(Outcome::Value(None));
        }
        let value = if decimal {
            DecimalValue::from_my_decimal(&column.get_my_decimal(row))
        } else if let Some(shape) = cast {
            shape.value(column.get_int64(row))
        } else {
            let bits = column.get_int64(row);
            Some(DecimalValue::from_integer(if unsigned {
                crate::coerce::Integer::Unsigned(bits as u64)
            } else {
                crate::coerce::Integer::Signed(bits)
            }))
        };
        Ok(match value {
            Some(value) => Outcome::Value(Some(value.0)),
            None => Outcome::Unsupported,
        })
    }
}
// The same closed fast-leaf batch loop serves both genuine source contracts.
// It stops physical reads after the first representation refusal.
pub(super) fn read_batch(
    value: &FastInput,
    input: &Chunk,
    native_values: Option<&RefCell<NativeValueStore>>,
    ordinal: usize,
    physical_rows: &[usize],
) -> LocalResult<VectorValue> {
    let store = native_values.ok_or_else(|| binding("fast source store missing"))?;
    let count = physical_rows.len();
    let payload = count
        .checked_mul(native_values::HANDLE_BYTES)
        .ok_or_else(|| resource("fast input vector bytes overflow"))?;
    store.borrow().precheck_import(
        payload
            .checked_add(array_bytes::<usize>(
                count
                    .checked_add(1)
                    .ok_or_else(|| resource("fast input offsets overflow"))?,
            )?)
            .ok_or_else(|| resource("fast input vector allowance overflow"))?,
    )?;
    let mut output = ChunkedVecBytes::try_with_capacities(count, payload)
        .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
    let mut unsupported = false;
    for (occurrence, &input_row) in physical_rows.iter().enumerate() {
        let observed = if unsupported {
            tidb_query_expr::NativeDecimalFastOutcome::Unsupported
        } else {
            value.read(input, input_row)?
        };
        unsupported |= matches!(
            observed,
            tidb_query_expr::NativeDecimalFastOutcome::Unsupported
        );
        let token = store.borrow_mut().insert_decimal_fast(
            observed,
            NativeValueOrigin {
                node: ordinal,
                row: InputRow {
                    occurrence,
                    input_row,
                },
            },
        )?;
        output.push_ref(Some(&token));
    }
    Ok(VectorValue::Bytes(output))
}

fn integer(field: &FieldType) -> bool {
    matches!(
        field.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
    )
}
fn arithmetic(node: SourceNodeRef<'_>) -> Option<SqlBatchHostOperation> {
    let BorrowedNodeKind::ScalarFunction(function) = node.kind() else {
        return None;
    };
    if function.get_static_type()?.code() != FieldTypeCode::NewDecimal
        || function.args.len() != 2
        || !function.args.iter().any(|arg| {
            arg.static_type()
                .is_some_and(|field| field.code() == FieldTypeCode::NewDecimal)
        })
    {
        return None;
    }
    Some(match function.func_name.lowercase() {
        "plus" => SqlBatchHostOperation::AddWindowNative,
        "minus" => SqlBatchHostOperation::SubtractWindowNative,
        "mul" => SqlBatchHostOperation::MultiplyWindowNative,
        _ => return None,
    })
}
fn cast_root(root: SourceNodeRef<'_>) -> bool {
    matches!(root.kind(),BorrowedNodeKind::ScalarFunction(function) if function.func_name.lowercase()=="cast_decimal"
        && function.get_static_type().is_some_and(|field|field.code()==FieldTypeCode::NewDecimal))
}
pub(super) fn possible(root: &Expression) -> bool {
    arithmetic(root.into()).is_some() || cast_root(root.into())
}
fn field<'a>(node: SourceNodeRef<'a>) -> Option<&'a FieldType> {
    match node.kind() {
        BorrowedNodeKind::Constant(value) => value.ret_type.as_ref(),
        BorrowedNodeKind::Column(value) => value.get_static_type(),
        BorrowedNodeKind::CorrelatedColumn(value) => value.column.get_static_type(),
        BorrowedNodeKind::ScalarFunction(value) => value.get_static_type(),
    }
}
#[derive(Clone, Copy)]
enum Storage<'a> {
    Schema(&'a [FieldType]),
    Chunk(&'a Chunk),
}
impl Storage<'_> {
    fn size(self, index: usize) -> Option<i64> {
        match self {
            Self::Schema(fields) => fields.get(index).map(get_fixed_len),
            Self::Chunk(input) => {
                (index < input.num_cols()).then(|| input.column(index).type_size())
            }
        }
    }
}

fn literal(value: &Datum, field: &FieldType) -> Outcome {
    let is_int = integer(field);
    let is_decimal = field.code() == FieldTypeCode::NewDecimal;
    if !is_int && !is_decimal {
        return Outcome::Unsupported;
    }
    let value = match value {
        Datum::Null => return Outcome::Value(None),
        Datum::Int(value) if is_int => Some(DecimalValue::from_integer(if field.is_unsigned() {
            crate::coerce::Integer::Unsigned(*value as u64)
        } else {
            crate::coerce::Integer::Signed(*value)
        })),
        Datum::UInt(value) if is_int => Some(DecimalValue::from_integer(
            crate::coerce::Integer::Unsigned(*value),
        )),
        Datum::Decimal(value) if is_decimal => value
            .to_my_decimal()
            .ok()
            .as_ref()
            .and_then(DecimalValue::from_my_decimal),
        _ => None,
    };
    value.map_or(Outcome::Unsupported, |value| Outcome::Value(Some(value.0)))
}
fn capture(
    node: SourceNodeRef<'_>,
    storage: Storage<'_>,
    budget: &mut Budget,
) -> Result<FastInput> {
    let unsupported = || FastInput::Constant(Outcome::Unsupported);
    let Some(field) = field(node) else {
        return Ok(unsupported());
    };
    if !integer(field) && field.code() != FieldTypeCode::NewDecimal {
        return Ok(unsupported());
    }
    Ok(match node.kind() {
        BorrowedNodeKind::Constant(value) => {
            let Some(value) = value.literal_value() else {
                return Ok(unsupported());
            };
            // The original constant conversion happens before broadcasting,
            // including empty input. Budget borrowed payload before converting.
            budget.add(
                native_payload_bytes(value)?
                    .checked_mul(3)
                    .ok_or_else(|| resource("fast literal allowance overflow"))?,
            )?;
            FastInput::Constant(literal(value, field))
        }
        BorrowedNodeKind::Column(column) => {
            let Ok(index) = usize::try_from(column.index) else {
                return Ok(unsupported());
            };
            let Some(actual) = storage.size(index) else {
                return match storage {
                    Storage::Schema(_) => Err(binding("fast column outside row schema").into()),
                    Storage::Chunk(_) => Ok(unsupported()),
                };
            };
            let decimal = field.code() == FieldTypeCode::NewDecimal;
            if actual
                != if decimal {
                    tidb_chunk::column::MY_DECIMAL_STRUCT_SIZE
                } else {
                    8
                }
            {
                return Ok(unsupported());
            }
            FastInput::Column {
                index,
                unsigned: field.is_unsigned(),
                decimal,
            }
        }
        BorrowedNodeKind::ScalarFunction(function)
            if field.code() == FieldTypeCode::NewDecimal
                && function.func_name.lowercase() == "cast_decimal" =>
        {
            let [Expression::Column(column)] = function.args.as_slice() else {
                return Ok(unsupported());
            };
            let Some(source) = column.get_static_type() else {
                return Ok(unsupported());
            };
            let Some(shape) = NativeIntegerDecimalCast::shape(source, field) else {
                return Ok(unsupported());
            };
            let Ok(index) = usize::try_from(column.index) else {
                return Ok(unsupported());
            };
            if storage.size(index) != Some(8) {
                return Ok(unsupported());
            }
            FastInput::IntegerCast { index, shape }
        }
        _ => unsupported(),
    })
}
pub(super) struct PreparedFast {
    pub source: Arc<BatchSource>,
    pub program: LocalBatchProgram,
    pub empty_supported: bool,
}
// Compile-time arena entries contain only borrowed source identity and shape;
// no borrowed expression escapes the bounded immutable factory.
struct Pending<N> {
    expression: N,
    depth: usize,
    force_input: bool,
}
struct Arena {
    child: Option<usize>,
    operation: Option<SqlBatchHostOperation>,
    arity: usize,
    field: FieldType,
    input: Option<FastInput>,
}
pub(super) fn compile(
    suite: &EvaluatorSuite,
    calculated_slot: usize,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
) -> Result<PreparedFast> {
    let root = suite
        .program
        .calculated
        .get(calculated_slot)
        .ok_or_else(|| binding("fast calculation slot missing"))?;
    let prepared = compile_plan(
        root,
        Storage::Schema(row_schema),
        new_collation,
        unit,
        limits,
    )?;
    let mut detached = Vec::new();
    reserve(&mut detached, row_schema.len())?;
    detached.extend(row_schema.iter().map(snapshot_field_type));
    Ok(PreparedFast {
        source: Arc::new(BatchSource {
            owner: Arc::clone(&suite.program),
            root_identity: root as *const Expression as usize,
            calculated_slot,
            output_index: suite.program.calculated_output_indexes[calculated_slot],
            row_schema: detached.into_boxed_slice(),
            plan: prepared.source,
        }),
        program: prepared.program,
        empty_supported: prepared.empty_supported,
    })
}
struct DetachedFast {
    source: SourcePlan,
    program: LocalBatchProgram,
    empty_supported: bool,
}
fn compile_plan<'a, N: SourceReference<'a>>(
    root: N,
    storage: Storage<'_>,
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
) -> Result<DetachedFast> {
    if arithmetic(root.into()).is_none() && !cast_root(root.into()) {
        return Err(binding("fast root is outside the priority arithmetic shape").into());
    }
    profile_shape(root, limits)?;
    let mut budget = Budget {
        used: 0,
        max: limits.max_metadata_bytes,
    };
    if let Storage::Schema(fields) = storage {
        for field in fields {
            budget.field(field, 1)?;
        }
    }
    let mut pending = Vec::new();
    budget.array::<Pending<N>>(1)?;
    reserve(&mut pending, 1)?;
    pending.push(Pending {
        expression: root,
        depth: 1,
        force_input: false,
    });
    let mut arena = Vec::new();
    let mut empty_supported = true;
    while let Some(Pending {
        expression,
        depth,
        force_input,
    }) = pending.pop()
    {
        if depth > limits.tree.max_depth || arena.len() >= limits.tree.max_nodes {
            return Err(resource("fast source tree policy exceeded").into());
        }
        budget.array::<Arena>(1)?;
        budget.array::<NodeRecord>(1)?;
        budget.array::<LocalExpr>(1)?;
        let fallback = native_bytes_type();
        let borrowed = field(expression.into()).unwrap_or(&fallback);
        budget.field(borrowed, 3)?;
        budget.field(&native_bytes_type(), 6)?;
        budget.array::<BuildNode>(1)?;
        let field = snapshot_field_type(borrowed);
        let op = if force_input {
            None
        } else if depth == 1 && cast_root(expression.into()) {
            Some(SqlBatchHostOperation::CastDecimalWindowNative)
        } else {
            arithmetic(expression.into())
        };
        let arity = match op {
            None => 0,
            Some(SqlBatchHostOperation::CastDecimalWindowNative) => 1,
            Some(_) => 2,
        };
        let input = if op.is_none() {
            let value = capture(expression.into(), storage, &mut budget)?;
            empty_supported &= !matches!(value, FastInput::Constant(Outcome::Unsupported));
            Some(value)
        } else {
            None
        };
        reserve(&mut arena, 1)?;
        arena.push(Arena {
            child: None,
            operation: op,
            arity,
            field,
            input,
        });
        if arity == 1 {
            // The genuine column-only priority CAST imports its checked,
            // padded coefficient directly, stopping physical reads on the
            // first representation refusal as the native owner does.
            budget.array::<Pending<N>>(1)?;
            reserve(&mut pending, 1)?;
            pending.push(Pending {
                expression,
                depth: depth + 1,
                force_input: true,
            });
        } else if arity == 2 {
            let BorrowedNodeKind::ScalarFunction(function) = expression.kind() else {
                unreachable!()
            };
            budget.array::<Pending<N>>(2)?;
            reserve(&mut pending, 2)?;
            pending.extend(function.args.iter().rev().map(|expression| Pending {
                expression: N::from(expression),
                depth: depth + 1,
                force_input: false,
            }));
        }
    }

    // Close original-preorder child boundaries before moving any arena values.
    let mut ends = Vec::new();
    budget.array::<usize>(arena.len())?;
    reserve(&mut ends, arena.len())?;
    ends.resize(arena.len(), 0);
    for index in (0..arena.len()).rev() {
        if arena[index].arity != 0 {
            let left = index + 1;
            let first_end = *ends
                .get(left)
                .ok_or_else(|| binding("fast first source missing"))?;
            let end = if arena[index].arity == 1 {
                first_end
            } else {
                let end = *ends
                    .get(first_end)
                    .ok_or_else(|| binding("fast second source missing"))?;
                if first_end <= left || end <= first_end {
                    return Err(binding("fast source subtree differs").into());
                }
                arena[index].child = Some(first_end);
                end
            };
            if end <= left || end > arena.len() {
                return Err(binding("fast source subtree extent differs").into());
            }
            ends[index] = end;
        } else {
            ends[index] = index + 1;
        }
    }
    let mut nodes = Vec::new();
    let mut bindings = Vec::new();
    let mut schema = Vec::new();
    let mut entries = Vec::new();
    let mut metadata = Vec::new();
    let mut sites = Vec::new();
    let mut build = Vec::new();
    for (ordinal, node) in arena.into_iter().enumerate() {
        let carrier = projected(&native_bytes_type(), new_collation)?;
        let kind = if let Some(operation) = node.operation {
            budget.array::<NativeHostMetadata>(1)?;
            budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
            budget.array::<OrdinaryCallSite>(2)?;
            reserve(&mut entries, 1)?;
            reserve(&mut metadata, 1)?;
            reserve(&mut sites, 1)?;
            let slot = entries.len();
            entries.push((
                operation,
                HostSignature {
                    arg_types: vec![carrier.clone(); node.arity].into_boxed_slice(),
                    return_type: carrier.clone(),
                },
            ));
            budget.array::<usize>(node.arity)?;
            let mut argument_nodes = Vec::new();
            reserve(&mut argument_nodes, node.arity)?;
            argument_nodes.push(ordinal + 1);
            if node.arity == 2 {
                argument_nodes.push(node.child.expect("joined children"));
            }
            metadata.push(NativeHostMetadata::DecimalFast {
                operation,
                argument_nodes: argument_nodes.into_boxed_slice(),
                output_node: ordinal,
                native_scope: matches!(storage, Storage::Chunk(_)),
            });
            sites.push(OrdinaryCallSite::sql_native_batch(
                ordinal,
                OrdinarySourceId::new(
                    unit,
                    u64::try_from(ordinal)
                        .map_err(|_| resource("fast source ordinal exceeds identity width"))?,
                ),
            ));
            reserve(&mut build, 1)?;
            build.push(BuildNode {
                field: carrier,
                kind: BuildKind::Host {
                    slot,
                    arity: node.arity,
                },
            });
            SourceKind::Call {
                target: CallTarget::Host(operation),
                arity: node.arity,
            }
        } else {
            budget.array::<usize>(1)?;
            budget.field(&native_bytes_type(), 2)?;
            reserve(&mut bindings, 1)?;
            reserve(&mut schema, 1)?;
            let slot = bindings.len();
            bindings.push(ordinal);
            schema.push(carrier.clone());
            reserve(&mut build, 1)?;
            build.push(BuildNode {
                field: carrier,
                kind: BuildKind::Input(slot),
            });
            SourceKind::DecimalFastInput(node.input.expect("leaf source"))
        };
        reserve(&mut nodes, 1)?;
        nodes.push(NodeRecord {
            sql: Some(node.field),
            kind,
            view: NativeView::DecimalFast,
        });
    }
    let hosts = HostCatalog::for_sql_batch(entries)?;
    let mut stack = Vec::new();
    budget.array::<LocalExpr>(build.len())?;
    reserve(&mut stack, build.len())?;
    for node in build.into_iter().rev() {
        let expr = match node.kind {
            BuildKind::Input(slot) => LocalExpr::InputSlot {
                slot,
                field_type: node.field,
            },
            BuildKind::Host { slot, arity } => {
                let mut args = Vec::new();
                reserve(&mut args, arity)?;
                for _ in 0..arity {
                    args.push(
                        stack
                            .pop()
                            .ok_or_else(|| binding("fast host child missing"))?,
                    );
                }
                LocalExpr::HostCall {
                    slot: hosts.slot(slot)?,
                    args: args.into_boxed_slice(),
                    return_type: node.field,
                }
            }
            _ => return Err(binding("fast factory emitted a different node kind").into()),
        };
        stack.push(expr);
    }
    if stack.len() != 1 {
        return Err(binding("fast source is not one tree").into());
    }
    let expr = stack.pop().expect("one root");
    let sites = lowered_call_sites(&expr, sites, limits.tree)?;
    let facts =
        BatchFacts::sql_native_batch_with_hosts(&expr, &schema, sites, limits.tree, &hosts)?;
    let program = compile_batch(
        &expr,
        &schema,
        LocalCompileContext {
            limits: limits.tree,
        },
        &facts,
    )?;
    Ok(DetachedFast {
        source: SourcePlan {
            unit,
            nodes: nodes.into_boxed_slice(),
            bindings: bindings.into_boxed_slice(),
            expr,
            schema: schema.into_boxed_slice(),
            facts,
            hosts: Some(hosts),
            host_metadata: metadata.into_boxed_slice(),
            host_probes: Box::new([]),
            needs_native_values: true,
            constant_root_shortcut: false,
            context_row_only: false,
            context_policy: None,
            limits,
        },
        program,
        empty_supported,
    })
}

pub(super) fn eval_function(
    function: &ScalarFunction,
    context: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    output_index: usize,
) -> std::result::Result<bool, EvalError> {
    let (limits, execution, maximum) = public_scalar_limits();
    eval_function_with_policies(
        function,
        context,
        input,
        output,
        output_index,
        limits,
        execution,
        maximum,
    )
}
pub(super) fn eval_function_with_policies(
    function: &ScalarFunction,
    context: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    output_index: usize,
    limits: BatchSourceLimits,
    execution: ExecutionLimits,
    maximum: usize,
) -> std::result::Result<bool, EvalError> {
    if !function
        .get_static_type()
        .is_some_and(|field| field.code() == FieldTypeCode::NewDecimal)
    {
        return Ok(false);
    }
    let root = SourceNodeRef::Function(function);
    if arithmetic(root).is_none() && !cast_root(root) {
        return Ok(false);
    }
    let mut prepared = compile_plan(root, Storage::Chunk(input), true, 1, limits)
        .map_err(|e| e.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))?;
    execute_function(
        &mut prepared,
        context,
        input,
        output,
        output_index,
        execution,
        maximum,
    )
    .map_err(|e| e.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke))
}
fn execute_function(
    prepared: &mut DetachedFast,
    context: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    output_index: usize,
    execution: ExecutionLimits,
    maximum: usize,
) -> Result<bool> {
    if input.num_rows() == 0 {
        return Ok(prepared.empty_supported);
    }
    let count = input.num_rows();
    let extent = array_bytes::<usize>(count)?
        .checked_add(array_bytes::<Option<tidb_datatype::MyDecimal>>(count)?)
        .ok_or_else(|| resource("native fast result extent overflow"))?;
    if extent > maximum {
        return Err(resource("native fast result extent exceeds policy").into());
    }
    let mut selection = Vec::new();
    selection
        .try_reserve_exact(count)
        .map_err(|_| resource("native fast selection allocation failed"))?;
    if let Some(rows) = input.sel() {
        selection.extend_from_slice(rows);
    } else {
        selection.extend(0..count);
    }
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(count)
        .map_err(|_| resource("native fast output allocation failed"))?;
    let extent = array_bytes::<usize>(selection.capacity())?
        .checked_add(array_bytes::<Option<tidb_datatype::MyDecimal>>(
            cells.capacity(),
        )?)
        .ok_or_else(|| resource("native fast reserved extent overflow"))?;
    let store = RefCell::new(NativeValueStore::new(maximum)?);
    store.borrow().precheck_import(extent)?;
    store.borrow_mut().begin_window()?;
    let source = &prepared.source;
    if source.nodes[0].view != NativeView::DecimalFast
        || !prepared.program.requires_whole_selection()
    {
        return Err(binding("native fast source contract differs").into());
    }
    let catalog = source
        .hosts
        .clone()
        .ok_or_else(|| binding("native fast host catalog missing"))?;
    let mut hosts = NativeBatchHosts::new(catalog, &source.host_metadata, context, maximum)
        .with_native_values(Some(&store));
    let mut native = native_source::Coordinates {
        inner: NativeChunk {
            source,
            native_values: Some(&store),
            input,
            selection: &selection,
        },
    };
    let mut checked = CheckedInputs {
        source: &mut native,
        hosts: Some(&mut hosts),
    };
    let result = prepared.program.eval_coordinates_with_bindings(
        execution,
        &mut EvalContext::default(),
        &selection,
        &mut checked,
    );
    drop(checked);
    if let Some(error) = hosts.take_native_error() {
        return Err(error.into());
    }
    let VectorValue::Bytes(values) = result? else {
        return Err(binding("native fast result carrier differs").into());
    };
    if values.len() != selection.len() {
        return Err(binding("native fast result length differs").into());
    }
    let retained = extent
        .checked_add(
            values
                .retained_heap_bytes()
                .ok_or_else(|| resource("native fast token extent overflow"))?,
        )
        .ok_or_else(|| resource("native fast retained extent overflow"))?;
    let stored = store.borrow();
    stored.precheck_import(retained)?;
    let mut unsupported = false;
    for (occurrence, &input_row) in selection.iter().enumerate() {
        let token = values
            .get(occurrence)
            .ok_or_else(|| binding("native fast wire NULL is not a result"))?;
        unsupported |= matches!(
            stored.decimal_fast(
                token,
                NativeValueOrigin {
                    node: 0,
                    row: InputRow {
                        occurrence,
                        input_row
                    }
                }
            )?,
            Outcome::Unsupported
        );
    }
    if unsupported {
        return Ok(false);
    }
    for (occurrence, &input_row) in selection.iter().enumerate() {
        match stored.decimal_fast(
            values.get(occurrence).expect("authenticated fast result"),
            NativeValueOrigin {
                node: 0,
                row: InputRow {
                    occurrence,
                    input_row,
                },
            },
        )? {
            Outcome::Unsupported => {
                return Err(binding("authenticated native fast result changed domain").into())
            }
            Outcome::Value(None) => cells.push(None),
            Outcome::Value(Some(value)) => match DecimalValue(*value).to_my_decimal() {
                Some(cell) => cells.push(Some(cell)),
                None => return Ok(false),
            },
        }
    }
    for cell in &cells {
        match cell {
            Some(cell) => output.append_my_decimal(output_index, cell),
            None => output.append_null(output_index),
        }
    }
    Ok(true)
}

impl PreparedBatch {
    /// Execute native's priority coefficient stage before the vector flag.
    /// false is exclusively the stage's typed Unsupported outcome. Every
    /// SDK/resource error returns Err and cannot select a different program.
    pub(super) fn execute_decimal_priority(
        &mut self,
        native_ctx: &dyn Columns,
        ctx: &mut EvalContext,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<bool> {
        let Some(profile) = self.fast_profile.as_ref() else {
            return Ok(false);
        };
        if self.selection.is_empty() {
            return Ok(profile.empty_supported);
        }
        let source = Arc::clone(&profile.source);
        if !profile.program.requires_whole_selection() {
            return Err(binding("fast priority program lost its whole-selection contract").into());
        }
        let store = RefCell::new(NativeValueStore::new(
            self.max_materialization_retained_bytes,
        )?);
        store.borrow_mut().begin_window()?;
        let catalog = source
            .hosts
            .clone()
            .ok_or_else(|| binding("fast priority host catalog missing"))?;
        let mut hosts = NativeBatchHosts::new(
            catalog,
            &source.host_metadata,
            native_ctx,
            self.max_materialization_retained_bytes,
        )
        .with_native_values(Some(&store));
        let mut native = NativeChunk {
            source: &source,
            native_values: Some(&store),
            input,
            selection: &self.selection,
        };
        let mut checked = CheckedInputs {
            source: &mut native,
            hosts: Some(&mut hosts),
        };
        let result = self
            .fast_profile
            .as_mut()
            .expect("checked priority profile")
            .program
            .eval_with_bindings(
                self.limits,
                ctx,
                input.physical_rows(),
                &self.selection,
                &mut checked,
            );
        drop(checked);
        if let Some(error) = hosts.take_native_error() {
            return Err(error.into());
        }
        let VectorValue::Bytes(values) = result.map_err(BatchFailure::from)? else {
            return Err(binding("fast priority result is not a capability vector").into());
        };
        if values.len() != self.selection.len() {
            return Err(binding("fast priority result length differs").into());
        }
        self.windows = self.windows.saturating_add(1);
        self.vector_allocations = self.vector_allocations.saturating_add(
            u64::try_from(
                source
                    .bindings
                    .len()
                    .saturating_add(source.facts.call_sites().len()),
            )
            .unwrap_or(u64::MAX),
        );
        let base = values
            .retained_heap_bytes()
            .ok_or_else(|| resource("fast priority vector size overflow"))?
            .checked_add(array_bytes::<usize>(self.selection.capacity())?)
            .ok_or_else(|| resource("fast priority selection allowance overflow"))?;
        let count = self.selection.len();
        let mut unsupported = false;
        {
            let stored = store.borrow();
            for (occurrence, &input_row) in self.selection.iter().enumerate() {
                let token = values
                    .get(occurrence)
                    .ok_or_else(|| binding("fast priority wire NULL is not a result"))?;
                unsupported |= matches!(
                    stored.decimal_fast(
                        token,
                        NativeValueOrigin {
                            node: 0,
                            row: InputRow {
                                occurrence,
                                input_row
                            }
                        }
                    )?,
                    Outcome::Unsupported
                );
            }
        }
        if unsupported {
            return Ok(false);
        }
        self.accept_retained(
            base.checked_add(array_bytes::<Option<tidb_datatype::MyDecimal>>(count)?)
                .ok_or_else(|| resource("fast priority output allowance overflow"))?,
        )?;
        let mut cells = Vec::new();
        reserve(&mut cells, count)?;
        self.accept_retained(
            base.checked_add(array_bytes::<Option<tidb_datatype::MyDecimal>>(
                cells.capacity(),
            )?)
            .ok_or_else(|| resource("fast priority reserved output allowance overflow"))?,
        )?;
        let stored = store.borrow();
        for (occurrence, &input_row) in self.selection.iter().enumerate() {
            let token = values
                .get(occurrence)
                .ok_or_else(|| binding("fast priority wire NULL is not a result"))?;
            match stored.decimal_fast(
                token,
                NativeValueOrigin {
                    node: 0,
                    row: InputRow {
                        occurrence,
                        input_row,
                    },
                },
            )? {
                Outcome::Unsupported => {
                    return Err(binding("validated fast result changed domain").into())
                }
                Outcome::Value(None) => cells.push(None),
                Outcome::Value(Some(value)) => match DecimalValue(*value).to_my_decimal() {
                    Some(cell) => cells.push(Some(cell)),
                    None => return Ok(false),
                },
            }
        }
        // Native priority publishes only after every final cell conversion
        // succeeds, and deliberately skips generic function-return coercion.
        for cell in &cells {
            match cell {
                Some(cell) => output.append_my_decimal(source.output_index, cell),
                None => output.append_null(source.output_index),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{column::Column, constant::Constant};
    use tidb_ast::CiString;
    struct Context;
    impl Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback in fast source")
        }
        fn enable_vectorized_expression(&self) -> bool {
            panic!("priority precedes vector flag")
        }
        fn div_precision_increment(&self) -> u32 {
            panic!("priority cannot read precision")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("priority cannot warn")
        }
    }
    fn call(name: &str, field: FieldType, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
    }
    fn column(index: usize, field: &FieldType) -> Expression {
        let mut column = Column::new(index as i64 + 1, field.clone());
        column.index = index as i64;
        Expression::Column(column)
    }
    #[test]
    fn fast_source_integer_cast_imports_match_native_priority_including_empty_and_wide_values() {
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6);
        let selections = vec![
            vec![],
            vec![1],
            vec![0, 1, 2],
            vec![2, 0, 2],
            (0..2050).map(|index| [1, 0, 2][index % 3]).collect(),
        ];
        for unsigned in [false, true] {
            let integer = FieldType::new(FieldTypeCode::LongLong)
                .with_flen(20)
                .with_decimal(0)
                .with_unsigned(unsigned);
            let schema = vec![integer.clone(), decimal.clone()];
            let mut input = Chunk::new_with_capacity(&schema, 3);
            for (left, right) in [
                (
                    Datum::Null,
                    Datum::Decimal(tidb_datatype::Decimal::from_int(3)),
                ),
                (
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(2)),
                ),
                (
                    if unsigned {
                        Datum::UInt(u64::MAX)
                    } else {
                        Datum::Int(i64::MAX)
                    },
                    Datum::Decimal(tidb_datatype::Decimal::from_int(0)),
                ),
            ] {
                input.append_datum(0, &left);
                input.append_datum(1, &right);
            }
            for scale in [0, 4, 30, 38, 39] {
                for name in ["plus", "minus", "mul"] {
                    let target = FieldType::new(FieldTypeCode::NewDecimal)
                        .with_flen(65)
                        .with_decimal(scale);
                    let cast = call("cast_decimal", target, vec![column(0, &integer)]);
                    let root = call(name, decimal.clone(), vec![cast, column(1, &decimal)]);
                    let owner = EvaluatorSuite::new(vec![root.clone()], false);
                    for selection in &selections {
                        input.set_sel(Some(selection.clone()));
                        let mut prepared = compile(
                            &owner,
                            0,
                            &schema,
                            true,
                            712,
                            BatchSourceLimits {
                                tree: CompileLimits::default(),
                                max_metadata_bytes: 1 << 20,
                            },
                        )
                        .unwrap();
                        let store = RefCell::new(NativeValueStore::new(64 << 20).unwrap());
                        store.borrow_mut().begin_window().unwrap();
                        let source = &prepared.source;
                        let context = Context;
                        let mut hosts = NativeBatchHosts::new(
                            source.hosts.clone().unwrap(),
                            &source.host_metadata,
                            &context,
                            64 << 20,
                        )
                        .with_native_values(Some(&store));
                        let mut native = NativeChunk {
                            source,
                            native_values: Some(&store),
                            input: &input,
                            selection,
                        };
                        let mut checked = CheckedInputs {
                            source: &mut native,
                            hosts: Some(&mut hosts),
                        };
                        let result = prepared
                            .program
                            .eval_with_bindings(
                                ExecutionLimits::default(),
                                &mut EvalContext::default(),
                                input.physical_rows(),
                                selection,
                                &mut checked,
                            )
                            .unwrap();
                        drop(checked);
                        let VectorValue::Bytes(values) = result else {
                            panic!("typed fast result")
                        };
                        let mut cells = Vec::new();
                        let mut supported = if selection.is_empty() {
                            prepared.empty_supported
                        } else {
                            true
                        };
                        for (occurrence, &input_row) in selection.iter().enumerate() {
                            let value = store
                                .borrow()
                                .decimal_fast(
                                    values.get(occurrence).unwrap(),
                                    NativeValueOrigin {
                                        node: 0,
                                        row: InputRow {
                                            occurrence,
                                            input_row,
                                        },
                                    },
                                )
                                .unwrap()
                                .clone();
                            match value {
                                Outcome::Unsupported => {
                                    supported = false;
                                }
                                Outcome::Value(None) => cells.push(None),
                                Outcome::Value(Some(value)) => {
                                    match DecimalValue(value).to_my_decimal() {
                                        Some(value) => cells.push(Some(value)),
                                        None => supported = false,
                                    }
                                }
                            }
                        }
                        let mut expected =
                            Chunk::new_with_capacity(std::slice::from_ref(&decimal), 0);
                        let Expression::ScalarFunction(function) = &root else {
                            unreachable!()
                        };
                        let expected_supported = crate::test_original_eval::with_original(|| {
                            function.vec_eval_decimal_arithmetic_in(
                                &context,
                                &input,
                                &mut expected,
                                0,
                            )
                        })
                        .unwrap();
                        assert_eq!(
                            supported,
                            expected_supported,
                            "{name} unsigned={unsigned} scale={scale} rows={}",
                            selection.len()
                        );
                        if supported {
                            let mut actual =
                                Chunk::new_with_capacity(std::slice::from_ref(&decimal), 0);
                            for cell in cells {
                                match cell {
                                    Some(cell) => actual.append_my_decimal(0, &cell),
                                    None => actual.append_null(0),
                                }
                            }
                            assert_eq!(actual.num_rows(), expected.num_rows());
                            for row in 0..actual.num_rows() {
                                assert_eq!(
                                    actual.column(0).get_raw(row).as_ref(),
                                    expected.column(0).get_raw(row).as_ref()
                                );
                            }
                        }
                        assert!(hosts.take_native_error().is_none());
                    }
                }
            }
        }
        // Strict integer/Decimal leaves preserve native's literal-shape
        // refusal before broadcasting, including an empty selection.
        let root = call(
            "plus",
            decimal.clone(),
            vec![
                Expression::Constant(Constant::new(
                    Datum::new_string("not a decimal literal"),
                    decimal.clone(),
                )),
                Expression::Constant(Constant::new(Datum::Null, decimal)),
            ],
        );
        let owner = EvaluatorSuite::new(vec![root], false);
        assert!(
            !compile(
                &owner,
                0,
                &[],
                true,
                713,
                BatchSourceLimits {
                    tree: CompileLimits::default(),
                    max_metadata_bytes: 1 << 20
                }
            )
            .unwrap()
            .empty_supported
        );
    }
    #[test]
    fn native_decimal_function_program_unwinds_and_reuses_after_demanded_cell_panic() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6);
        let function = ScalarFunction::new(
            CiString::new("plus"),
            decimal.clone(),
            vec![column(0, &integer), column(1, &decimal)],
        );
        let mut input = Chunk::new_with_capacity(&[integer, decimal.clone()], 2);
        for (a, b) in [
            (
                Datum::Int(7),
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.25")),
            ),
            (Datum::Null, Datum::Null),
        ] {
            input.append_datum(0, &a);
            input.append_datum(1, &b);
        }
        let (limits, execution, maximum) = public_scalar_limits();
        let mut prepared = compile_plan(
            SourceNodeRef::Function(&function),
            Storage::Chunk(&input),
            true,
            1,
            limits,
        )
        .unwrap();
        for selection in [vec![0, 1, 0], vec![usize::MAX], vec![1, 0, 1]] {
            input.set_sel(Some(selection));
            let mut output = Chunk::new_with_capacity(std::slice::from_ref(&decimal), 0);
            let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_function(
                    &mut prepared,
                    &Context,
                    &input,
                    &mut output,
                    0,
                    execution,
                    maximum,
                )
            }));
            if input.sel().unwrap() == [usize::MAX] {
                assert!(actual.is_err());
                assert_eq!(output.num_rows(), 0);
            } else {
                assert!(actual.unwrap().unwrap());
                let mut original = Chunk::new_with_capacity(std::slice::from_ref(&decimal), 0);
                assert!(crate::test_original_eval::with_original(
                    || function.vec_eval_decimal_arithmetic_in(&Context, &input, &mut original, 0)
                )
                .unwrap());
                assert_eq!(output.num_rows(), original.num_rows());
                for row in 0..output.num_rows() {
                    assert_eq!(
                        output.column(0).is_null(row),
                        original.column(0).is_null(row)
                    );
                    assert_eq!(
                        output.column(0).get_raw(row).as_ref(),
                        original.column(0).get_raw(row).as_ref()
                    );
                }
            }
        }
    }
}
