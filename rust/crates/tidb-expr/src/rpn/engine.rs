// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use std::any::Any;

use protobuf::{Message, ProtobufEnum};
use scoped_tls_hkt::scoped_thread_local;
use tidb_query_common::{Result, error::EvaluateError};
use tidb_query_datatype::{
    EvalType, FieldTypeAccessor,
    codec::{
        batch::LazyBatchColumnVec,
        data_type::{ScalarValue, ScalarValueRef, VectorValue},
    },
    expr::EvalContext,
};
use tidb_query_expr::{
    RpnExpression, RpnExpressionBuilder, RpnExpressionNode, RpnFnCallExtra, RpnFnMeta,
    RpnStackNode, RpnStackNodeVectorValue, ShortCircuitFnMeta,
};
use tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

use super::{
    DATE_ARITHMETIC_TAG, EXECUTION_SEED_DECLARATION, LAST_INSERT_ID_DECLARATION, NATIVE_DIAGNOSTIC_TAG, NULL_HOST_TAG, PASSWORD_STRENGTH_TAG, PATH_FIRST_HOST_TAG, REPEAT_DECLARATION, rows,
};

pub(super) fn error(message: impl Into<String>) -> tidb_query_common::Error {
    EvaluateError::Other(message.into()).into()
}

pub(super) fn decode(bytes: &[u8]) -> Result<Expr> {
    protobuf::parse_from_bytes(bytes).map_err(|err| error(err.to_string()))
}

pub(super) fn scalar_func_sig(value: i32) -> Option<ScalarFuncSig> {
    ScalarFuncSig::from_i32(value)
}

pub(super) fn expr_type(value: i32) -> Option<ExprType> {
    ExprType::from_i32(value)
}

pub(crate) use tidb_exprctx::context::SeededRandomDemand as StatementRand;

scoped_thread_local!(static INPUT: for<'a> &'a rows::NativeRows<'a>);

pub(super) fn evaluate<'a>(
    program: &'a RpnExpression,
    ctx: &mut EvalContext,
    schema: &'a [FieldType],
    input: &rows::NativeRows<'_>,
    physical: &'a LazyBatchColumnVec,
    rows: &'a [usize],
) -> Result<RpnStackNode<'a>> {
    INPUT.set(input, || {
        program.eval_decoded(ctx, schema, physical, rows, rows.len())
    })
}

/// The interpreter passes a selected row id as an ordinary, decoded Int
/// argument. A host leaf therefore reads and converts only rows it is asked for.
fn read_input(
    ctx: &mut EvalContext,
    count: usize,
    args: &[RpnStackNode<'_>],
    extra: &mut RpnFnCallExtra<'_>,
    metadata: &(dyn Any + Send),
) -> Result<VectorValue> {
    let offset = *metadata.downcast_ref::<usize>().expect("input offset");
    let mut read = |rows: &[usize]| {
        INPUT.with(|input| {
            let RpnStackNode::Vector {
                value: RpnStackNodeVectorValue::Generated { physical_value },
                ..
            } = input.read_column(ctx, offset, extra.ret_field_type, rows)?
            else {
                return Err(error("host input returned non-owned values"));
            };
            Ok(physical_value)
        })
    };
    let id = |index| match args[0].get_logical_scalar_ref(index) {
        ScalarValueRef::Int(Some(value)) => *value as usize,
        _ => unreachable!("physical row index"),
    };
    if count == 1 {
        read(&[id(0)])
    } else {
        read(&(0..count).map(id).collect::<Vec<_>>())
    }
}

fn host_call(
    _: &mut EvalContext,
    count: usize,
    args: &[RpnStackNode<'_>],
    _: &mut RpnFnCallExtra<'_>,
    metadata: &(dyn Any + Send),
) -> Result<VectorValue> {
    let slot = *metadata.downcast_ref::<usize>().expect("host call slot");
    INPUT.with(|input| input.evaluate_host_call(slot, args, count))
}

#[derive(Clone, Copy)]
enum DecimalArithmetic {
    Multiply,
    Decimal,
    Signed,
    Unsigned,
}

impl DecimalArithmetic {
    fn for_call(call: &Expr) -> Option<Self> {
        match call.get_sig() {
            ScalarFuncSig::MultiplyDecimal => Some(Self::Multiply),
            ScalarFuncSig::DivideDecimal => Some(Self::Decimal),
            ScalarFuncSig::IntDivideDecimal => {
                let unsigned = call
                    .get_children()
                    .iter()
                    .any(|arg| arg.get_field_type().as_accessor().is_unsigned());
                Some(if unsigned {
                    Self::Unsigned
                } else {
                    Self::Signed
                })
            }
            _ => None,
        }
    }

    fn evaluate(
        self,
        ctx: &mut EvalContext,
        rows: usize,
        args: &[RpnStackNode<'_>],
    ) -> Result<VectorValue> {
        use tidb_query_datatype::codec::{Error, mysql::decimal::Res};
        let kind = match self {
            Self::Multiply | Self::Decimal => EvalType::Decimal,
            Self::Signed | Self::Unsigned => EvalType::Int,
        };
        let mut result = VectorValue::with_capacity(rows, kind);
        for row in 0..rows {
            let (ScalarValueRef::Decimal(Some(left)), ScalarValueRef::Decimal(Some(right))) = (
                args[0].get_logical_scalar_ref(row),
                args[1].get_logical_scalar_ref(row),
            ) else {
                append_null(&mut result);
                continue;
            };
            if matches!(self, Self::Multiply) {
                use tidb_query_expr::impl_arithmetic::{ArithmeticOp, DecimalMultiply};
                let value = DecimalMultiply::calc(left, right)?;
                let value = match value {
                    Some(value) if value.is_zero() && left.is_negative() != right.is_negative() => {
                        use tidb_query_datatype::codec::mysql::Decimal;
                        let scale = usize::from(
                            (rows::decimal_result_scale(left)?
                                + rows::decimal_result_scale(right)?)
                            .min(30),
                        );
                        let zero = b"0.000000000000000000000000000000";
                        let len = if scale == 0 { 1 } else { scale + 2 };
                        Some(Decimal::from_bytes(&zero[..len])?.unwrap())
                    }
                    value => value,
                };
                result.push_decimal(value);
                continue;
            }
            let Some(division) = left.div(right, ctx.cfg.div_precision_increment) else {
                ctx.handle_division_by_zero()?;
                append_null(&mut result);
                continue;
            };
            let value = match division {
                Res::Ok(value) => value,
                Res::Truncated(value) => {
                    ctx.handle_truncate_err(Error::truncated_wrong_val("DECIMAL", &value))?;
                    value
                }
                Res::Overflow(value) => {
                    ctx.handle_overflow_err(Error::overflow(
                        "DECIMAL",
                        format_args!("({left} / {right})"),
                    ))?;
                    value
                }
            };
            match self {
                Self::Multiply | Self::Decimal => result.push_decimal(Some(value)),
                Self::Signed => {
                    let signed = value.as_i64();
                    if signed.is_overflow() {
                        return Err(Error::overflow(
                            "BIGINT",
                            format_args!("({left} DIV {right})"),
                        )
                        .into());
                    }
                    result.push_int(Some(signed.unwrap()));
                }
                Self::Unsigned => {
                    let unsigned = value.as_u64();
                    if unsigned.is_overflow() {
                        let signed = value.as_i64();
                        if signed.is_truncated() && signed.unwrap() == 0 {
                            result.push_int(Some(0));
                            continue;
                        }
                        return Err(Error::overflow(
                            "BIGINT UNSIGNED",
                            format_args!("({left} DIV {right})"),
                        )
                        .into());
                    }
                    result.push_int(Some(unsigned.unwrap() as i64));
                }
            }
        }
        Ok(result)
    }
}

struct Kernel {
    upstream: RpnFnMeta,
    metadata: Box<dyn Any + Send>,
    diagnostic: Option<usize>,
    check_integer_zero_divisor: bool,
    signed_remainder: bool,
    decimal_arithmetic: Option<DecimalArithmetic>,
}

fn decorated_kernel(
    ctx: &mut EvalContext,
    count: usize,
    args: &[RpnStackNode<'_>],
    extra: &mut RpnFnCallExtra<'_>,
    metadata: &(dyn Any + Send),
) -> Result<VectorValue> {
    let kernel = metadata
        .downcast_ref::<Kernel>()
        .expect("diagnostic kernel");
    if kernel.check_integer_zero_divisor {
        let level = INPUT.with(|input| input.context.division_by_zero_level());
        for row in 0..count {
            let left = args[0].get_logical_scalar_ref(row);
            let right = args[1].get_logical_scalar_ref(row);
            let zero = match right {
                ScalarValueRef::Int(Some(value)) => *value == 0,
                ScalarValueRef::Real(Some(value)) => value.into_inner() == 0.0,
                _ => false,
            };
            if zero && !is_null(left) {
                use crate::ErrorLevel;
                let error = tidb_query_datatype::codec::Error::division_by_zero();
                match level {
                    ErrorLevel::Ignore => {}
                    ErrorLevel::Warn => ctx.warnings.append_warning(error),
                    ErrorLevel::Error => return Err(error.into()),
                }
            }
        }
    }
    let result = if let Some(arithmetic) = kernel.decimal_arithmetic {
        arithmetic.evaluate(ctx, count, args)
    } else if kernel.signed_remainder {
        let mut result = VectorValue::with_capacity(count, EvalType::Int);
        for row in 0..count {
            let value = match (args[0].get_logical_scalar_ref(row), args[1].get_logical_scalar_ref(row)) {
                (ScalarValueRef::Int(Some(left)), ScalarValueRef::Int(Some(right))) => {
                    crate::ops::signed_remainder(*left, *right)?
                }
                _ => None,
            };
            result.push_int(value);
        }
        Ok(result)
    } else {
        (kernel.upstream.fn_ptr)(ctx, count, args, extra, kernel.metadata.as_ref())
    };
    result.map_err(|error| match kernel.diagnostic {
        Some(slot) => INPUT.with(|input| input.kernel_error(slot, error)),
        None => error,
    })
}

fn callback(
    name: &'static str,
    fn_ptr: RpnFnMeta,
    args_len: usize,
    field_type: FieldType,
    metadata: Box<dyn Any + Send>,
) -> RpnExpressionNode {
    RpnExpressionNode::FnCall {
        func_meta: RpnFnMeta { name, ..fn_ptr },
        args_len,
        field_type,
        metadata,
    }
}

fn meta(
    name: &'static str,
    fn_ptr: fn(
        &mut EvalContext,
        usize,
        &[RpnStackNode<'_>],
        &mut RpnFnCallExtra<'_>,
        &(dyn Any + Send),
    ) -> Result<VectorValue>,
) -> RpnFnMeta {
    RpnFnMeta {
        name,
        fn_ptr,
        validator_ptr: |_| Ok(()),
        metadata_expr_ptr: |_| Ok(Box::new(())),
    }
}

fn index(value: &[u8]) -> Result<usize> {
    let raw = <[u8; 8]>::try_from(value).map_err(|_| error("invalid host binding index"))?;
    usize::try_from(u64::from_le_bytes(raw)).map_err(|err| error(err.to_string()))
}

/// Bind a protobuf kernel using the real upstream parser. Lowered scalar
/// children are represented by typed dummy columns during this one-node build.
/// Their actual programs are appended separately, preserving host demand.
pub(super) fn root_kernel(call: &Expr, ctx: &mut EvalContext) -> Result<RpnExpressionNode> {
    let mut root = Expr::new();
    root.set_tp(call.get_tp());
    root.set_sig(call.get_sig());
    root.set_field_type(call.get_field_type().clone());
    if !call.get_val().starts_with(NATIVE_DIAGNOSTIC_TAG) {
        root.set_val(call.get_val().to_vec());
    }
    let children = call
        .get_children()
        .iter()
        .map(|child| {
            if !matches!(child.get_tp(), ExprType::ColumnRef | ExprType::ScalarFunc) {
                return child.clone();
            }
            let mut placeholder = Expr::new();
            placeholder.set_tp(ExprType::ColumnRef);
            placeholder.set_val(vec![0x80, 0, 0, 0, 0, 0, 0, 0]);
            placeholder.set_field_type(child.get_field_type().clone());
            placeholder
        })
        .collect::<Vec<_>>();
    root.set_children(children.into());
    RpnExpressionBuilder::build_from_expr_tree(root, ctx, 1)?
        .into_inner()
        .pop()
        .ok_or_else(|| error("upstream builder emitted an empty expression"))
}

// An internal annotation on the already decoded PB copy. The upstream
// builder carries output declarations through its flattening and IN rewrites,
// but its compiled callbacks no longer carry a ScalarFuncSig. Zero marks
// decorated integer division. Positive markers hold a one-based host slot.
const WIRE_CALLBACK: u32 = 51000;

fn wire_cast_host(node: &Expr) -> Result<Option<super::HostCall>> {
    use crate::distsql_builtin::pb_type_to_field_type;
    use crate::scalar_function::{PbBuiltin, PreparedLocalBuiltin, ScalarFunction};
    use tidb_proto::tipb::FieldType as WireField;

    let sig = node.get_sig();
    let [child] = node.get_children() else {
        return Ok(None);
    };
    let year = i32::from(tidb_datatype::FieldTypeCode::Year.mysql_type());
    // These casts use TiDB's warning and partial-date contract.
    if !matches!(sig, ScalarFuncSig::CastStringAsReal | ScalarFuncSig::CastJsonAsReal)
        && !(sig == ScalarFuncSig::CastIntAsTime && child.get_field_type().get_tp() == year)
    {
        return Ok(None);
    }
    let native_type = |field: &FieldType| -> Result<tidb_datatype::FieldType> {
        let bytes = field.write_to_bytes().map_err(|err| error(err.to_string()))?;
        let wire = <WireField as prost::Message>::decode(bytes.as_slice())
            .map_err(|err| error(err.to_string()))?;
        Ok(pb_type_to_field_type(&wire))
    };
    let source = native_type(child.get_field_type())?;
    let target = native_type(node.get_field_type())?;
    let sig = tidb_proto::tipb::ScalarFuncSig::try_from(sig.value())
        .map_err(|err| error(err.to_string()))?;
    let builtin = PbBuiltin::new(sig).ok_or_else(|| error("missing SQL cast signature"))?;
    let operand = crate::expression::Expression::Column(crate::column::Column::new(0, source.clone()));
    let scalar = ScalarFunction::from_pb(builtin, target, vec![operand])
        .map_err(|err| error(format!("{err:?}")))?;
    Ok(Some(super::HostCall {
        output: node.get_field_type().clone(),
        arguments: vec![source],
        operation: super::HostOperation::LocalBuiltin(Box::new(PreparedLocalBuiltin::new(&scalar))),
    }))
}

pub(super) fn build_wire(
    mut tree: Expr,
    ctx: &mut EvalContext,
    schema: &[FieldType],
    physical_fields: &[tidb_datatype::FieldType],
    hosts: &mut Vec<super::HostCall>,
) -> Result<RpnExpression> {
    fn prepare_declaration(node: &mut Expr, hosts: &mut Vec<super::HostCall>) -> Result<()> {
        if node.get_tp() == ExprType::MysqlBit && node.get_val().len() > 8 {
            return Err(error("BIT literal exceeds 64 bits"));
        }
        if node.get_tp() == ExprType::ScalarFunc {
            let binding = if node.get_sig() == ScalarFuncSig::IntDivideInt {
                Some(0)
            } else if let Some(host) = wire_cast_host(node)? {
                hosts.push(host);
                Some(hosts.len() as u64)
            } else {
                None
            };
            if let Some(binding) = binding {
                node.mut_field_type()
                    .mut_unknown_fields()
                    .add_varint(WIRE_CALLBACK, binding);
            }
        }
        for child in node.mut_children().iter_mut() {
            prepare_declaration(child, hosts)?;
        }
        Ok(())
    }

    // Decode/validate the complete declared tree and let the storage engine
    // choose eager versus short circuit nodes, including its depth budget.
    prepare_declaration(&mut tree, hosts)?;
    let program = RpnExpressionBuilder::build_from_expr_tree(tree, ctx, schema.len())?;
    Ok(adapt_wire_inputs(program, schema, physical_fields))
}

fn adapt_wire_inputs(
    program: RpnExpression,
    schema: &[FieldType],
    physical_fields: &[tidb_datatype::FieldType],
) -> RpnExpression {
    let mut adapted = Vec::with_capacity(program.len());
    for node in program.into_inner() {
        match node {
            RpnExpressionNode::ColumnRef { offset } => {
                append_input(&mut adapted, offset, &schema[offset], physical_fields);
            }
            RpnExpressionNode::ShortCircuitFnCall {
                func_meta,
                args,
                field_type,
            } => adapted.push(RpnExpressionNode::ShortCircuitFnCall {
                func_meta,
                args: args
                    .into_vec()
                    .into_iter()
                    .map(|arg| adapt_wire_inputs(arg, schema, physical_fields))
                    .collect(),
                field_type,
            }),
            RpnExpressionNode::FnCall {
                func_meta,
                metadata,
                mut field_type,
                args_len,
            } if field_type.get_unknown_fields().get(WIRE_CALLBACK).is_some() => {
                let binding = field_type
                    .get_unknown_fields()
                    .get(WIRE_CALLBACK)
                    .expect("marked wire declaration")
                    .varint[0];
                field_type
                    .mut_unknown_fields()
                    .fields
                    .as_mut()
                    .expect("marked wire declaration")
                    .remove(&WIRE_CALLBACK);
                let (name, callback_meta, metadata): (_, _, Box<dyn Any + Send>) = if binding == 0 {
                    let name = func_meta.name;
                    let kernel = Box::new(Kernel {
                        upstream: func_meta,
                        metadata,
                        diagnostic: None,
                        check_integer_zero_divisor: true,
                        signed_remainder: false,
                        decimal_arithmetic: None,
                    });
                    (name, meta(name, decorated_kernel), kernel)
                } else {
                    let name = "TiDB wire cast";
                    let slot = (binding - 1) as usize;
                    (name, meta(name, host_call), Box::new(slot))
                };
                adapted.push(callback(name, callback_meta, args_len, field_type, metadata));
            }
            node => adapted.push(node),
        }
    }
    RpnExpression::from(adapted)
}

fn append_input(
    nodes: &mut Vec<RpnExpressionNode>,
    offset: usize,
    field_type: &FieldType,
    physical_fields: &[tidb_datatype::FieldType],
) {
    if physical_fields
        .get(offset)
        .is_some_and(|field| borrowable_field(field, field_type).is_some())
    {
        nodes.push(RpnExpressionNode::ColumnRef { offset: offset + 1 });
    } else {
        nodes.push(RpnExpressionNode::ColumnRef { offset: 0 });
        nodes.push(callback(
            "TiDB input",
            meta("TiDB input", read_input),
            1,
            field_type.clone(),
            Box::new(offset),
        ));
    }
}

pub(super) fn build(
    tree: Expr,
    ctx: &mut EvalContext,
    input_count: usize,
    physical_fields: &[tidb_datatype::FieldType],
    scalar_guards: bool,
) -> Result<RpnExpression> {
    let mut nodes = Vec::new();
    compile(
        tree,
        ctx,
        input_count,
        physical_fields,
        scalar_guards,
        &mut nodes,
    )?;
    Ok(RpnExpression::from(nodes))
}

fn compile(
    mut tree: Expr,
    ctx: &mut EvalContext,
    input_count: usize,
    physical_fields: &[tidb_datatype::FieldType],
    scalar_guards: bool,
    nodes: &mut Vec<RpnExpressionNode>,
) -> Result<()> {
    match tree.get_tp() {
        ExprType::ColumnRef => {
            let (_, offset) = tidb_codec::decode_int(tree.get_val())
                .map_err(|_| error("invalid column index"))?;
            let offset = usize::try_from(offset).map_err(|err| error(err.to_string()))?;
            if offset >= input_count {
                return Err(error("column reference out of bounds"));
            }
            append_input(nodes, offset, tree.get_field_type(), physical_fields);
        }
        ExprType::ScalarFunc => {
            let sig = tree.get_sig();
            let argc = tree.get_children().len();
            let special = if tree.get_val() == REPEAT_DECLARATION {
                Some(ShortCircuitFnMeta {
                    sig,
                    fn_ptr: benchmark,
                })
            } else if tree.get_val() == LAST_INSERT_ID_DECLARATION {
                Some(ShortCircuitFnMeta { sig, fn_ptr: last_insert_id })
            } else if let Some(slot) = tree.get_val()
                .strip_prefix(EXECUTION_SEED_DECLARATION)
                .or_else(|| tree.get_val().strip_prefix(PATH_FIRST_HOST_TAG))
                .or_else(|| tree.get_val().strip_prefix(DATE_ARITHMETIC_TAG))
                .or_else(|| tree.get_val().strip_prefix(PASSWORD_STRENGTH_TAG))
            {
                let fn_ptr = if tree.get_val().starts_with(PATH_FIRST_HOST_TAG) {
                    json_paths_first
                } else if tree.get_val().starts_with(DATE_ARITHMETIC_TAG) {
                    date_arithmetic
                } else if tree.get_val().starts_with(PASSWORD_STRENGTH_TAG) {
                    password_strength
                } else {
                    seeded_rand
                };
                let slot = index(slot)?;
                let literal = integer_root(slot as i64);
                let mut args = tree
                    .take_children()
                    .into_vec()
                    .into_iter()
                    .map(|child| build(child, ctx, input_count, physical_fields, scalar_guards))
                    .collect::<Result<Vec<_>>>()?;
                args.push(literal);
                nodes.push(RpnExpressionNode::ShortCircuitFnCall {
                    func_meta: ShortCircuitFnMeta {
                        sig,
                        fn_ptr,
                    },
                    args: args.into_boxed_slice(),
                    field_type: tree.take_field_type(),
                });
                return Ok(());
            } else {
                branch_meta(sig)
            };
            if let Some(func_meta) = special {
                let args = tree
                    .take_children()
                    .into_vec()
                    .into_iter()
                    .map(|child| build(child, ctx, input_count, physical_fields, scalar_guards))
                    .collect::<Result<Vec<_>>>()?;
                nodes.push(RpnExpressionNode::ShortCircuitFnCall {
                    func_meta,
                    args: args.into_boxed_slice(),
                    field_type: tree.take_field_type(),
                });
                return Ok(());
            }
            if sig == ScalarFuncSig::Rand && argc == 0 {
                nodes.push(callback(
                    "TiDB RAND",
                    meta("TiDB RAND", unseeded_rand),
                    0,
                    tree.take_field_type(),
                    Box::new(()),
                ));
                return Ok(());
            }
            let mut root = if sig == ScalarFuncSig::Unspecified {
                let slot = index(
                    tree.get_val()
                        .strip_prefix(NULL_HOST_TAG)
                        .unwrap_or_else(|| tree.get_val()),
                )?;
                callback(
                    "TiDB host",
                    meta("TiDB host", host_call),
                    argc,
                    tree.get_field_type().clone(),
                    Box::new(slot),
                )
            } else {
                root_kernel(&tree, ctx)?
            };
            if let RpnExpressionNode::FnCall {
                func_meta,
                metadata,
                field_type,
                args_len,
            } = root
            {
                let diagnostic = tree
                    .get_val()
                    .strip_prefix(NATIVE_DIAGNOSTIC_TAG)
                    .map(index)
                    .transpose()?;
                let check_integer_zero_divisor = matches!(
                    sig,
                    ScalarFuncSig::IntDivideInt
                        | ScalarFuncSig::ModInt
                        | ScalarFuncSig::ModReal
                        | ScalarFuncSig::ModIntUnsignedUnsigned
                        | ScalarFuncSig::ModIntUnsignedSigned
                        | ScalarFuncSig::ModIntSignedUnsigned
                        | ScalarFuncSig::ModIntSignedSigned
                );
                let decimal_arithmetic = DecimalArithmetic::for_call(&tree);
                root = if diagnostic.is_some()
                    || check_integer_zero_divisor
                    || decimal_arithmetic.is_some()
                {
                    let kernel = Kernel {
                        upstream: func_meta,
                        metadata,
                        diagnostic,
                        check_integer_zero_divisor,
                        signed_remainder: sig == ScalarFuncSig::ModIntSignedSigned
                            || sig == ScalarFuncSig::ModInt && tree.get_children().iter().all(|arg| {
                                !arg.get_field_type().as_accessor().is_unsigned()
                            }),
                        decimal_arithmetic,
                    };
                    callback(
                        func_meta.name,
                        meta(func_meta.name, decorated_kernel),
                        args_len,
                        field_type,
                        Box::new(kernel),
                    )
                } else {
                    RpnExpressionNode::FnCall {
                        func_meta,
                        metadata,
                        field_type,
                        args_len,
                    }
                };
            }
            let argc = match &root {
                RpnExpressionNode::FnCall { args_len, .. } => *args_len,
                _ => argc,
            };
            if argc < tree.get_children().len() {
                // The original IN initializer packs constants into its hash table.
                // Match its reverse-tail swaps to retain the same dynamic order.
                let children = tree.mut_children();
                let mut end = children.len();
                for index in (1..end).rev() {
                    if !matches!(
                        children[index].get_tp(),
                        ExprType::ColumnRef | ExprType::ScalarFunc
                    ) {
                        end -= 1;
                        children.as_mut_slice().swap(index, end);
                    }
                }
                if end != argc {
                    return Err(error("upstream kernel changed its operand list"));
                }
                children.truncate(end);
            }
            let policy = if argc > 1 && tree.get_val().starts_with(NULL_HOST_TAG) {
                Some(null_guard as Branch)
            } else if scalar_guards
                && argc == 2
                && scalar_null_rhs(sig)
                && !quiet_operand(&tree.get_children()[1], physical_fields)
            {
                Some(scalar_null_guard as Branch)
            } else {
                None
            };
            if let Some(fn_ptr) = policy {
                let field_type = tree.take_field_type();
                let mut args = tree
                    .take_children()
                    .into_vec()
                    .into_iter()
                    .map(|child| build(child, ctx, input_count, physical_fields, scalar_guards))
                    .collect::<Result<Vec<_>>>()?;
                let mut apply = (0..argc)
                    .map(|offset| RpnExpressionNode::ColumnRef { offset })
                    .collect::<Vec<_>>();
                apply.push(root);
                args.push(RpnExpression::from(apply));
                nodes.push(RpnExpressionNode::ShortCircuitFnCall {
                    func_meta: ShortCircuitFnMeta { sig, fn_ptr },
                    args: args.into_boxed_slice(),
                    field_type,
                });
                return Ok(());
            }
            for child in tree.take_children().into_vec() {
                compile(
                    child,
                    ctx,
                    input_count,
                    physical_fields,
                    scalar_guards,
                    nodes,
                )?;
            }
            nodes.push(root);
        }
        _ => {
            nodes.extend(RpnExpressionBuilder::build_from_expr_tree(tree, ctx, 0)?.into_inner());
        }
    }
    Ok(())
}

fn branch_meta(sig: ScalarFuncSig) -> Option<ShortCircuitFnMeta> {
    use ScalarFuncSig::*;
    let fn_ptr = match sig {
        IfInt | IfReal | IfDecimal | IfString | IfTime | IfDuration | IfJson | IfVectorFloat32 => {
            choose_if
        }
        IfNullInt
        | IfNullReal
        | IfNullDecimal
        | IfNullString
        | IfNullTime
        | IfNullDuration
        | IfNullJson
        | IfNullVectorFloat32
        | CoalesceInt
        | CoalesceReal
        | CoalesceDecimal
        | CoalesceString
        | CoalesceTime
        | CoalesceDuration
        | CoalesceJson
        | CoalesceVectorFloat32 => first_nonnull,
        CaseWhenInt
        | CaseWhenReal
        | CaseWhenDecimal
        | CaseWhenString
        | CaseWhenTime
        | CaseWhenDuration
        | CaseWhenJson
        | CaseWhenVectorFloat32 => case_when,
        Concat => concat,
        ConcatWs => concat_ws,
        IntervalInt | IntervalReal => interval,
        Elt => elt,
        MakeSet => make_set,
        LogicalAnd => logical_and,
        LogicalOr => logical_or,
        _ => return None,
    };
    Some(ShortCircuitFnMeta { sig, fn_ptr })
}

fn interval(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let fields = args
        .iter()
        .map(|arg| Some(native_field(arg.ret_field_type(schema))))
        .collect::<Vec<_>>();
    let mut output = VectorValue::with_capacity(count, EvalType::Int);
    INPUT.with(|host| {
        for row in rows.chunks(1) {
            let mut query_error = None;
            let read = |index: usize| {
                let result = operand_value(
                    ctx,
                    schema,
                    input,
                    row,
                    &args[index],
                    fields[index].as_ref().unwrap(),
                );
                result.map_err(|error| {
                    query_error = Some(error);
                    crate::EvalError::Unsupported("INTERVAL operand failed")
                })
            };
            let result = crate::builtin_ext::interval_lazy(&fields, read, host.context);
            if let Some(error) = query_error {
                return Err(error);
            }
            let crate::Datum::Int(result) = result.map_err(|error| host.native_error(error))?
            else {
                return Err(error("invalid INTERVAL result"));
            };
            output.push_int(Some(result));
        }
        Ok(output)
    })
}

fn native_field(field: &FieldType) -> tidb_datatype::FieldType {
    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::from_mysql_type(
        field.get_tp() as u8,
    ))
    .with_flags(field.get_flag())
    .with_flen(i64::from(field.get_flen()))
    .with_decimal(i64::from(field.get_decimal()))
    .with_charset_name(field.get_charset())
}

fn operand_value(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    row: &[usize],
    arg: &RpnExpression,
    field: &tidb_datatype::FieldType,
) -> Result<crate::Datum> {
    let value = arg.eval_decoded(ctx, schema, input, row, 1)?;
    rows::from_query_value(value.get_logical_scalar_ref(0), field)
}

fn elt(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let mut output = VectorValue::with_capacity(count, EvalType::Bytes);
    for row in rows.chunks(1) {
        let index = args[0].eval_decoded(ctx, schema, input, row, 1)?;
        let ScalarValueRef::Int(index) = index.get_logical_scalar_ref(0) else {
            return Err(error("ELT index must be an integer"));
        };
        let arg = index
            .and_then(|&index| usize::try_from(index).ok())
            .filter(|&index| index > 0)
            .and_then(|index| args.get(index));
        if let Some(arg) = arg {
            let value = arg.eval_decoded(ctx, schema, input, row, 1)?;
            push(&mut output, value.get_logical_scalar_ref(0));
        } else {
            output.push_bytes(None);
        }
    }
    Ok(output)
}

fn make_set(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let mut output = VectorValue::with_capacity(count, EvalType::Bytes);
    for row in rows.chunks(1) {
        let mask = args[0].eval_decoded(ctx, schema, input, row, 1)?;
        let ScalarValueRef::Int(mask) = mask.get_logical_scalar_ref(0) else {
            return Err(error("MAKE_SET mask must be an integer"));
        };
        let Some(&mask) = mask else {
            output.push_bytes(None);
            continue;
        };
        let mut result = Vec::new();
        let mut need_separator = false;
        for (position, arg) in args[1..].iter().take(64).enumerate() {
            if mask as u64 & (1 << position) != 0 {
                let value = arg.eval_decoded(ctx, schema, input, row, 1)?;
                if let ScalarValueRef::Bytes(Some(bytes)) = value.get_logical_scalar_ref(0) {
                    if need_separator {
                        result.push(b',');
                    }
                    result.extend_from_slice(bytes);
                    need_separator = true;
                }
            }
        }
        output.push_bytes(Some(result));
    }
    Ok(output)
}

fn truth(value: ScalarValueRef<'_>) -> Option<bool> {
    match value {
        ScalarValueRef::Int(value) => value.map(|value| *value != 0),
        _ => unreachable!("condition has an integer declaration"),
    }
}

fn push(output: &mut VectorValue, value: ScalarValueRef<'_>) {
    match value.to_owned() {
        ScalarValue::Int(value) => output.push_int(value),
        ScalarValue::Real(value) => output.push_real(value),
        ScalarValue::Decimal(value) => output.push_decimal(value),
        ScalarValue::Bytes(value) => output.push_bytes(value),
        ScalarValue::DateTime(value) => output.push_date_time(value),
        ScalarValue::Duration(value) => output.push_duration(value),
        ScalarValue::Json(value) => output.push_json(value),
        ScalarValue::Enum(value) => output.push_enum(value),
        ScalarValue::Set(value) => output.push_set(value),
        ScalarValue::VectorFloat32(value) => output.push_vector_float32(value),
    }
}

fn eval_selected<'a>(
    expr: &'a RpnExpression,
    ctx: &mut EvalContext,
    schema: &'a [FieldType],
    input: &'a LazyBatchColumnVec,
    rows: &'a [usize],
) -> Result<Option<RpnStackNode<'a>>> {
    if rows.is_empty() {
        Ok(None)
    } else {
        expr.eval_decoded(ctx, schema, input, rows, rows.len())
            .map(Some)
    }
}

fn choose_if(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let condition = args[0].eval_decoded(ctx, schema, input, rows, count)?;
    let mut selected = [Vec::new(), Vec::new()];
    for (index, row) in rows.iter().enumerate() {
        let nil_or_false = !truth(condition.get_logical_scalar_ref(index)).unwrap_or(false);
        selected[usize::from(nil_or_false)].push(*row);
    }
    let kind = EvalType::try_from(args[1].ret_field_type(schema).as_accessor().tp())
        .map_err(|err| error(err.to_string()))?;
    let results = [
        eval_selected(&args[1], ctx, schema, input, &selected[0])?,
        eval_selected(&args[2], ctx, schema, input, &selected[1])?,
    ];
    let mut output = VectorValue::with_capacity(count, kind);
    let mut offsets = [0, 0];
    for index in 0..count {
        let branch = usize::from(!truth(condition.get_logical_scalar_ref(index)).unwrap_or(false));
        push(
            &mut output,
            results[branch]
                .as_ref()
                .expect("selected branch")
                .get_logical_scalar_ref(offsets[branch]),
        );
        offsets[branch] += 1;
    }
    Ok(output)
}

fn first_nonnull(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let kind = EvalType::try_from(args[0].ret_field_type(schema).as_accessor().tp())
        .map_err(|err| error(err.to_string()))?;
    let mut output = VectorValue::with_capacity(count, kind);
    for row in rows.chunks(1) {
        let mut found = false;
        for arg in args {
            let value = arg.eval_decoded(ctx, schema, input, row, 1)?;
            let scalar = value.get_logical_scalar_ref(0);
            if !is_null(scalar) {
                push(&mut output, scalar);
                found = true;
                break;
            }
        }
        if !found {
            append_null(&mut output);
        }
    }
    Ok(output)
}

fn case_when(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let kind = EvalType::try_from(args[1].ret_field_type(schema).as_accessor().tp())
        .map_err(|err| error(err.to_string()))?;
    let mut output = VectorValue::with_capacity(count, kind);
    for row in rows.chunks(1) {
        let mut pairs = args.chunks_exact(2);
        let mut selected = None;
        for pair in &mut pairs {
            let condition = pair[0].eval_decoded(ctx, schema, input, row, 1)?;
            if truth(condition.get_logical_scalar_ref(0)).unwrap_or(false) {
                selected = Some(&pair[1]);
                break;
            }
        }
        if let Some(selected) = selected.or_else(|| pairs.remainder().first()) {
            let result = selected.eval_decoded(ctx, schema, input, row, 1)?;
            push(&mut output, result.get_logical_scalar_ref(0));
        } else {
            append_null(&mut output);
        }
    }
    Ok(output)
}

pub(super) fn append_null(output: &mut VectorValue) {
    match output {
        VectorValue::Int(_) => output.push_int(None),
        VectorValue::Real(_) => output.push_real(None),
        VectorValue::Decimal(_) => output.push_decimal(None),
        VectorValue::Bytes(_) => output.push_bytes(None),
        VectorValue::DateTime(_) => output.push_date_time(None),
        VectorValue::Duration(_) => output.push_duration(None),
        VectorValue::Json(_) => output.push_json(None),
        VectorValue::Enum(_) => output.push_enum(None),
        VectorValue::Set(_) => output.push_set(None),
        VectorValue::VectorFloat32(_) => output.push_vector_float32(None),
    }
}

fn logical_and(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    logical(ctx, schema, input, rows, count, args, false)
}

fn logical_or(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    logical(ctx, schema, input, rows, count, args, true)
}

fn logical(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
    early: bool,
) -> Result<VectorValue> {
    let mut output = VectorValue::with_capacity(count, EvalType::Int);
    for row in rows.chunks(1) {
        let mut null = false;
        let mut decisive = false;
        for arg in args {
            let value = arg.eval_decoded(ctx, schema, input, row, 1)?;
            match truth(value.get_logical_scalar_ref(0)) {
                Some(value) if value == early => {
                    decisive = true;
                    break;
                }
                Some(_) => {}
                None => null = true,
            }
        }
        let result = match (decisive, null) {
            (true, _) => Some(i64::from(early)),
            (false, false) => Some(i64::from(!early)),
            (false, true) => None,
        };
        output.push_int(result);
    }
    Ok(output)
}

mod shapes;

#[derive(Clone, Copy)]
pub(super) enum RpnArgTypes {
    Fixed(&'static [EvalType]),
    Uniform(EvalType),
    OptionalTail(&'static [EvalType]),
    Repeated {
        prefix: &'static [EvalType],
        group: &'static [EvalType],
        prefix_json_values: bool,
        json_values: bool,
    },
    Unused,
    Custom,
}

pub(super) struct Signature {
    pub(super) return_type: EvalType,
    pub(super) argument_types: RpnArgTypes,
}

impl Signature {
    pub(super) fn validate(&self, call: &Expr) -> Result<()> {
        let args = call.get_children();
        for (index, child) in args.iter().enumerate() {
            let required = match self.argument_types {
                RpnArgTypes::Fixed(types) if types.len() == args.len() => types.get(index).copied(),
                RpnArgTypes::Uniform(kind) => Some(kind),
                RpnArgTypes::OptionalTail(types) => types.get(index).copied(),
                RpnArgTypes::Repeated { prefix, group, .. } => prefix
                    .get(index)
                    .copied()
                    .or_else(|| group.get((index - prefix.len()) % group.len()).copied()),
                _ => None,
            };
            if let Some(required) = required {
                let actual = EvalType::try_from(child.get_field_type().as_accessor().tp())
                    .map_err(|err| error(err.to_string()))?;
                if required != actual {
                    return Err(error("operand needs its declared SQL cast"));
                }
            }
        }
        if branch_meta(call.get_sig()).is_some()
            || call.get_sig() == ScalarFuncSig::Rand && call.get_children().is_empty()
        {
            return Ok(());
        }
        root_kernel(call, &mut EvalContext::default()).map(|_| ())
    }
}

pub(super) fn resolve_rpn_function(
    sig: ScalarFuncSig,
    field: &FieldType,
    _: &[Expr],
) -> Result<Signature> {
    let recorded = (!format!("{sig:?}").starts_with("Cast"))
        .then(|| shapes::shape(sig))
        .flatten();
    let (return_type, argument_types) = match recorded {
        Some(shape) => shape,
        None => (
            EvalType::try_from(field.as_accessor().tp()).map_err(|err| error(err.to_string()))?,
            RpnArgTypes::Custom,
        ),
    };
    Ok(Signature {
        return_type,
        argument_types,
    })
}

pub(super) fn bind_cast_in_union(expr: &mut Expr) -> Result<()> {
    let mut metadata = tipb::InUnionMetadata::new();
    metadata.set_in_union(true);
    expr.set_val(
        metadata
            .write_to_bytes()
            .map_err(|err| error(err.to_string()))?,
    );
    Ok(())
}

pub(super) fn index_field() -> FieldType {
    let mut field = FieldType::new();
    field.set_tp(8);
    field
}

fn is_null(value: ScalarValueRef<'_>) -> bool {
    match value {
        ScalarValueRef::Int(value) => value.is_none(),
        ScalarValueRef::Real(value) => value.is_none(),
        ScalarValueRef::Decimal(value) => value.is_none(),
        ScalarValueRef::Bytes(value) => value.is_none(),
        ScalarValueRef::DateTime(value) => value.is_none(),
        ScalarValueRef::Duration(value) => value.is_none(),
        ScalarValueRef::Json(value) => value.is_none(),
        ScalarValueRef::Enum(value) => value.is_none(),
        ScalarValueRef::Set(value) => value.is_none(),
        ScalarValueRef::VectorFloat32(value) => value.is_none(),
    }
}

fn integer_root(value: i64) -> RpnExpression {
    RpnExpression::from(vec![RpnExpressionNode::Constant {
        value: ScalarValue::Int(Some(value)),
        field_type: index_field(),
    }])
}

fn unseeded_rand(
    _: &mut EvalContext,
    count: usize,
    _: &[RpnStackNode<'_>],
    _: &mut RpnFnCallExtra<'_>,
    _: &(dyn Any + Send),
) -> Result<VectorValue> {
    let mut output = VectorValue::with_capacity(count, EvalType::Real);
    INPUT.with(|host| {
        for _ in 0..count {
            let value = host.next_random()?;
            output.push_real(tidb_query_datatype::codec::data_type::Real::new(value).ok());
        }
        Ok(output)
    })
}

fn seeded_rand(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let slot = args[1].eval_decoded(ctx, schema, input, rows, count)?;
    let ScalarValueRef::Int(Some(slot)) = slot.get_logical_scalar_ref(0) else {
        return Err(error("RAND seed slot is invalid"));
    };
    let slot = *slot as usize;
    let mut output = VectorValue::with_capacity(count, EvalType::Real);
    INPUT.with(|host| {
        for row in rows.chunks(1) {
            let value = match host
                .statement_seeded_random(slot)
                .ok_or_else(|| error("RAND seed slot is missing"))?
            {
                StatementRand::Draw(value) => value,
                StatementRand::Initialize(init) => {
                    let seed = args[0].eval_decoded(ctx, schema, input, row, 1)?;
                    let ScalarValueRef::Int(seed) = seed.get_logical_scalar_ref(0) else {
                        return Err(error("RAND seed is not an integer"));
                    };
                    init(seed.copied().unwrap_or(0))
                }
            };
            output.push_real(tidb_query_datatype::codec::data_type::Real::new(value).ok());
        }
        Ok(output)
    })
}

fn json_paths_first(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let (slot, args) = args.split_last().expect("JSON host call slot");
    let slot = slot.eval_decoded(ctx, schema, input, rows, count)?;
    let ScalarValueRef::Int(Some(slot)) = slot.get_logical_scalar_ref(0) else {
        return Err(error("invalid JSON host call slot"));
    };
    INPUT.with(|host| {
        let order: Vec<_> = if host.scalar_operand_demand() {
            (0..args.len()).collect()
        } else {
            super::json_modify_operand_order(args.len()).collect()
        };
        let mut values: Vec<_> = std::iter::repeat_with(|| None).take(args.len()).collect();
        for index in order {
            values[index] = Some(args[index].eval_decoded(ctx, schema, input, rows, count)?);
        }
        let values: Vec<_> = values.into_iter().map(|value| value.expect("operand evaluated")).collect();
        host.evaluate_host_call(*slot as usize, &values, count)
    })
}

fn date_arithmetic(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    selected: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    use crate::time_fn::date_arithmetic as native;
    let [date, amount, unit, slot] = args else {
        return Err(error("invalid date arithmetic arguments"));
    };
    let slot = slot.eval_decoded(ctx, schema, input, selected, count)?;
    let ScalarValueRef::Int(Some(slot)) = slot.get_logical_scalar_ref(0) else {
        return Err(error("date arithmetic slot is invalid"));
    };
    INPUT.with(|host| {
        let call = &host.host_calls[*slot as usize];
        let super::HostOperation::DateArithmetic { subtract, result } = &call.operation else {
            return Err(error("date arithmetic operation is missing"));
        };
        let [date_field, amount_field, unit_field] = call.arguments.as_slice() else {
            return Err(error("invalid date arithmetic types"));
        };
        let native_error = |err| host.native_error(err);
        let mut output = rows::output_vector(&call.output, count)?;
        let unit = unit.eval_decoded(ctx, schema, input, &selected[..1], 1)?;
        let unit = rows::from_query_value(unit.get_logical_scalar_ref(0), unit_field)?;
        let Some(unit) = crate::arg_eval_type::eval_string(&unit).map_err(native_error)? else {
            for _ in 0..count {
                rows::append(&mut output, tidb_datatype::Datum::Null)?;
            }
            return Ok(output);
        };
        let unit = tidb_goruntime::GoString::from_bytes(unit).to_utf8_lossy_go();
        let amounts = amount.eval_decoded(ctx, schema, input, selected, count)?;
        let mut intervals = Vec::with_capacity(count);
        let reread = amount_field.eval_type() == tidb_datatype::EvalType::Decimal
            && !crate::time_fn::calendar::is_composite_unit(&unit)
            && !unit.eq_ignore_ascii_case("SECOND");
        for (row, index) in selected.iter().enumerate() {
            let mut value = rows::from_query_value(amounts.get_logical_scalar_ref(row), amount_field)?;
            if reread && !value.is_null() {
                let value_again = amount.eval_decoded(ctx, schema, input, std::slice::from_ref(index), 1)?;
                value = rows::from_query_value(value_again.get_logical_scalar_ref(0), amount_field)?;
            }
            intervals.push(native::interval(&unit, value, amount_field.decimal(), host.context)
                .map_err(native_error)?);
        }
        let dates = date.eval_decoded(ctx, schema, input, selected, count)?;
        let dates = (0..count).map(|row| {
            let value = rows::from_query_value(dates.get_logical_scalar_ref(row), date_field)?;
            native::date(&unit, value, host.context).map_err(native_error)
        }).collect::<Result<Vec<_>>>()?;
        for (date, interval) in dates.into_iter().zip(intervals) {
            let value = native::operation(result, date_field, &unit, *subtract, date, interval, host.context)
                .map_err(native_error)?;
            rows::append(&mut output, value)?;
        }
        Ok(output)
    })
}

fn password_strength(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    selected: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    use crate::builtin_ext::{crypto, misc::opt_prop_error};
    let [passwords, slot] = args else {
        return Err(error("invalid password strength arguments"));
    };
    let slot = slot.eval_decoded(ctx, schema, input, selected, count)?;
    let ScalarValueRef::Int(Some(slot)) = slot.get_logical_scalar_ref(0) else {
        return Err(error("password strength slot is invalid"));
    };
    INPUT.with(|host| {
        let call = &host.host_calls[*slot as usize];
        let native_error = |err| host.native_error(err);
        // Go's vector entry acquires both properties before reading the
        // password column, and applies the policy even to short strings.
        let user = crypto::password_validation_user(host.context).map_err(native_error)?;
        let vars = tidb_expropt::SessionVarsPropReader
            .get_session_vars(host.context)
            .map_err(|err| host.native_error(opt_prop_error(err)))?;
        let passwords = passwords.eval_decoded(ctx, schema, input, selected, count)?;
        let globals = crypto::EvalGlobalVars(&*vars);
        let enabled = crypto::password_validation_enabled(&globals).map_err(native_error)?;
        let mut output = rows::output_vector(&call.output, count)?;
        for index in 0..count {
            let password = rows::from_query_value(passwords.get_logical_scalar_ref(index), &call.arguments[0])?;
            let value = if password.is_null() {
                tidb_datatype::Datum::Null
            } else if enabled {
                crypto::validate_password_strength_with_user(&password, &globals, user.as_ref())
                    .map_err(native_error)?
            } else {
                tidb_datatype::Datum::Int(0)
            };
            rows::append(&mut output, value)?;
        }
        Ok(output)
    })
}

fn last_insert_id(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    INPUT.with(|host| {
        let vars = tidb_expropt::SessionVarsPropReader
            .get_session_vars(host.context)
            .map_err(|err| host.native_error(crate::builtin_ext::misc::opt_prop_error(err)))?;
        let values = args[0].eval_decoded(ctx, schema, input, rows, count)?;
        let mut result = VectorValue::with_capacity(count, EvalType::Int);
        let mut last = None;
        for index in 0..count {
            let ScalarValueRef::Int(value) = values.get_logical_scalar_ref(index) else {
                return Err(error("LAST_INSERT_ID argument is not an integer"));
            };
            result.push_int(value.copied());
            if let Some(&value) = value {
                last = Some(value);
            }
        }
        if let Some(value) = last {
            vars.set_last_insert_id(value as u64);
        }
        Ok(result)
    })
}

fn benchmark(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    if INPUT.with(|host| host.typed_batch) {
        let repetitions = args[0].eval_decoded(ctx, schema, input, rows, count)?;
        let ScalarValueRef::Int(Some(repetitions)) = repetitions.get_logical_scalar_ref(0) else {
            return Err(error("typed BENCHMARK count is not an integer"));
        };
        for _ in 0..*repetitions {
            args[1].eval_decoded(ctx, schema, input, rows, count)?;
        }
        return Ok(VectorValue::Int(vec![Some(0); count].into()));
    }
    let mut output = VectorValue::with_capacity(count, EvalType::Int);
    for row in rows.chunks(1) {
        let repetitions = args[0].eval_decoded(ctx, schema, input, row, 1)?;
        let ScalarValueRef::Int(repetitions) = repetitions.get_logical_scalar_ref(0) else {
            return Err(error("BENCHMARK count is not an integer"));
        };
        if let Some(&repetitions) = repetitions.filter(|&&count| count >= 0) {
            for _ in 0..repetitions {
                args[1].eval_decoded(ctx, schema, input, row, 1)?;
            }
            output.push_int(Some(0));
        } else {
            output.push_int(None);
        }
    }
    Ok(output)
}

fn scalar_null_rhs(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        PlusInt
            | PlusIntUnsignedUnsigned
            | PlusIntUnsignedSigned
            | PlusIntSignedUnsigned
            | PlusIntSignedSigned
            | PlusDecimal
            | MinusInt
            | MinusReal
            | MinusDecimal
            | MultiplyInt
            | MultiplyIntUnsigned
            | MultiplyReal
            | MultiplyDecimal
            | DivideReal
            | DivideDecimal
            | IntDivideInt
            | IntDivideDecimal
            | ModDecimal
    )
}
type Branch = fn(
    &mut EvalContext,
    &[FieldType],
    &LazyBatchColumnVec,
    &[usize],
    usize,
    &[RpnExpression],
) -> Result<VectorValue>;

fn scalar_null_guard(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    if INPUT.with(|host| host.scalar_operand_demand()) {
        null_guard(ctx, schema, input, rows, count, args)
    } else {
        apply_operands(ctx, schema, input, rows, args, false)
    }
}

fn null_guard(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    let kind = output_type(args.last().expect("guarded call"), &[])?;
    let mut output = VectorValue::with_capacity(count, kind);
    for row in rows.chunks(1) {
        output.append(&mut apply_operands(ctx, schema, input, row, args, true)?);
    }
    Ok(output)
}

fn output_type(expr: &RpnExpression, schema: &[FieldType]) -> Result<EvalType> {
    EvalType::try_from(expr.ret_field_type(schema).as_accessor().tp())
        .map_err(|err| error(err.to_string()))
}

fn apply_operands(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    args: &[RpnExpression],
    skip_null: bool,
) -> Result<VectorValue> {
    let (apply, operands) = args.split_last().expect("guarded call");
    let mut physical = Vec::with_capacity(operands.len());
    let mut fields = Vec::with_capacity(operands.len());
    for (index, expr) in operands.iter().enumerate() {
        let result = expr.eval_decoded(ctx, schema, input, rows, rows.len())?;
        let mut column = VectorValue::with_capacity(rows.len(), output_type(expr, schema)?);
        for position in 0..rows.len() {
            let value = result.get_logical_scalar_ref(position);
            if skip_null && index + 1 < operands.len() && is_null(value) {
                let mut output = VectorValue::with_capacity(1, output_type(apply, &[])?);
                append_null(&mut output);
                return Ok(output);
            }
            push(&mut column, value);
        }
        fields.push(expr.ret_field_type(schema).clone());
        physical.push(column);
    }
    let offsets = (0..rows.len()).collect::<Vec<_>>();
    let physical = LazyBatchColumnVec::from(physical);
    let RpnStackNode::Vector {
        value: RpnStackNodeVectorValue::Generated { physical_value },
        ..
    } = apply.eval_decoded(ctx, &fields, &physical, &offsets, rows.len())?
    else {
        return Err(error("callback returned a non-owned result"));
    };
    Ok(physical_value)
}

pub(super) fn borrowable_field(
    native: &tidb_datatype::FieldType,
    query: &FieldType,
) -> Option<EvalType> {
    if native.is_array() || i32::from(native.code().mysql_type()) != query.get_tp() {
        return None;
    }
    match EvalType::try_from(query.as_accessor().tp()).ok()? {
        EvalType::Int if native.code().is_type_integer() => Some(EvalType::Int),
        EvalType::Bytes if native.code().is_string() => Some(EvalType::Bytes),
        _ => None,
    }
}

pub(super) fn needs_scalar_guard(tree: &Expr, fields: &[tidb_datatype::FieldType]) -> bool {
    let children = tree.get_children();
    (children.len() == 2 && scalar_null_rhs(tree.get_sig()) && !quiet_operand(&children[1], fields))
        || children
            .iter()
            .any(|child| needs_scalar_guard(child, fields))
}

fn quiet_operand(expr: &Expr, fields: &[tidb_datatype::FieldType]) -> bool {
    match expr.get_tp() {
        ExprType::ColumnRef => tidb_codec::decode_int(expr.get_val())
            .ok()
            .and_then(|(_, index)| usize::try_from(index).ok())
            .and_then(|offset| fields.get(offset))
            .is_some_and(|field| borrowable_field(field, expr.get_field_type()).is_some()),
        ExprType::ScalarFunc => false,
        // Constants are decoded and validated by the compiler.
        _ => true,
    }
}

fn concat(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    concat_branch(ctx, schema, input, rows, count, args, false)
}

fn concat_ws(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> Result<VectorValue> {
    if INPUT.with(|host| host.scalar_operand_demand()) {
        return concat_branch(ctx, schema, input, rows, count, args, true);
    }
    let mut columns = Vec::with_capacity(args.len());
    for arg in args {
        columns.push(arg.eval_decoded(ctx, schema, input, rows, count)?);
    }
    let output = INPUT.with(|host| {
        crate::string_fn::concat_ws_columns(
            count,
            columns.len(),
            |column, row| match columns[column].get_logical_scalar_ref(row) {
                ScalarValueRef::Bytes(value) => value,
                _ => None,
            },
            host.context,
        )
        .map_err(|error| host.native_error(error))
    })?;
    let mut result = VectorValue::with_capacity(count, EvalType::Bytes);
    for value in output {
        result.push_bytes(value);
    }
    Ok(result)
}

fn concat_branch(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
    with_separator: bool,
) -> Result<VectorValue> {
    let mut output = VectorValue::with_capacity(count, EvalType::Bytes);
    INPUT.with(|host| {
        for row in rows.chunks(1) {
            let mut query_error = None;
            let operand = |index: usize| {
                let result = args[index]
                    .eval_decoded(ctx, schema, input, row, 1)
                    .map_err(|error| {
                        query_error = Some(error);
                        crate::EvalError::Unsupported("shared operand failed")
                    })?;
                match result.get_logical_scalar_ref(0) {
                    ScalarValueRef::Bytes(Some(bytes)) => Ok(crate::Datum::new_bytes_datum(bytes)),
                    ScalarValueRef::Bytes(None) => Ok(crate::Datum::Null),
                    _ => Err(crate::EvalError::Unsupported(
                        "concatenation requires bytes",
                    )),
                }
            };
            let result = if with_separator {
                crate::string_fn::concat_ws_from(args.len(), operand, host.context)
            } else {
                crate::string_fn::concat_from(args.len(), operand, host.context)
            };
            if let Some(error) = query_error {
                return Err(error);
            }
            let result = result.map_err(|error| host.native_error(error))?;
            output.push_bytes(result.as_raw_bytes().map(<[u8]>::to_vec));
        }
        Ok(output)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_query_datatype::expr::Flag;

    fn constant(value: i64) -> Expr {
        let mut encoded = Vec::new();
        tidb_codec::encode_int(&mut encoded, value);
        let mut node = Expr::new();
        node.set_tp(ExprType::Int64);
        node.set_field_type(index_field());
        node.set_val(encoded);
        node
    }

    fn call(sig: ScalarFuncSig, left: Expr, right: Expr) -> Expr {
        let mut node = Expr::new();
        node.set_tp(ExprType::ScalarFunc);
        node.set_field_type(index_field());
        node.set_sig(sig);
        node.set_children(vec![left, right].into());
        node
    }

    #[test]
    fn wire_logic_keeps_upstream_flattening_and_depth_budget() {
        let fallback = call(ScalarFuncSig::EqInt, constant(1), constant(1));
        for alternating in [false, true] {
            let mut tree = fallback.clone();
            for depth in 0..36 {
                let sig = if alternating && depth % 2 == 0 {
                    ScalarFuncSig::LogicalOr
                } else {
                    ScalarFuncSig::LogicalAnd
                };
                tree = call(sig, constant(1), tree);
            }
            let mut config = tidb_query_datatype::expr::EvalConfig::default();
            config.flag |= Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION;
            let mut ctx = EvalContext::new(std::sync::Arc::new(config));
            let original =
                RpnExpressionBuilder::build_from_expr_tree(tree.clone(), &mut ctx, 0).unwrap();
            let adapted = build_wire(tree, &mut ctx, &[], &[], &mut Vec::new()).unwrap();
            assert_eq!(format!("{adapted:?}"), format!("{original:?}"));
            let [RpnExpressionNode::ShortCircuitFnCall { args, .. }] = adapted.as_ref() else {
                panic!("expected a short-circuit root");
            };
            assert_eq!(args.len(), if alternating { 2 } else { 37 });
        }
    }

    #[test]
    fn wire_logic_checks_types_even_in_an_unreachable_operand() {
        let mut invalid = Expr::new();
        invalid.set_tp(ExprType::String);
        let mut string = FieldType::new();
        string.set_tp(253);
        invalid.set_field_type(string);
        let tree = call(ScalarFuncSig::LogicalAnd, constant(0), invalid);
        let mut config = tidb_query_datatype::expr::EvalConfig::default();
        config.flag |= Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION;
        let mut ctx = EvalContext::new(std::sync::Arc::new(config));
        assert!(build_wire(tree, &mut ctx, &[], &[], &mut Vec::new()).is_err());
    }
}
