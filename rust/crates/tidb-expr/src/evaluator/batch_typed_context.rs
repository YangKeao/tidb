// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Typed live reads, deferred values and per-source invocation broadcasts.
use super::*;
#[derive(Debug)]
pub(in super::super) struct NativeTypedContext {
    // Absence is the original batch deferred-wrapper forward operation.
    pub descriptor: Option<Box<NativeTypedDescriptor>>,
    pub target: NativeEvalType,
    pub cache_slot: Option<usize>,
    pub argument: Option<usize>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
#[derive(Debug)]
pub(in super::super) enum NativeTypedDescriptor {
    Context(crate::typed_context_value::TypedContextValue),
    Literal {
        value: Datum,
        field: Option<FieldType>,
    },
    Column {
        index: i64,
        field: Option<FieldType>,
    },
    Normalize {
        field: FieldType,
    },
    NormalizeRoot {
        field: FieldType,
        unsigned: bool,
    },
    RootForward {
        unsigned: bool,
    },
}
impl From<crate::typed_context_value::TypedContextValue> for NativeTypedDescriptor {
    fn from(value: crate::typed_context_value::TypedContextValue) -> Self {
        Self::Context(value)
    }
}
impl NativeTypedDescriptor {
    fn demands_child(&self) -> bool {
        match self {
            Self::Context(value) => value.demands_child(),
            Self::Normalize { .. } | Self::NormalizeRoot { .. } | Self::RootForward { .. } => true,
            _ => false,
        }
    }
    fn validate_child_type(&self) -> std::result::Result<(), EvalError> {
        match self {
            Self::Context(value) => value.validate_child_type(),
            Self::Normalize { .. } | Self::NormalizeRoot { .. } | Self::RootForward { .. } => {
                Ok(())
            }
            _ => unreachable!("leaf has no typed child"),
        }
    }
    fn finish_child(
        &self,
        value: Datum,
        ctx: &dyn Columns,
        target: NativeEvalType,
    ) -> std::result::Result<Datum, EvalError> {
        match self {
            Self::Context(descriptor) => descriptor.finish_child(value, ctx, target),
            Self::RootForward { unsigned } => {
                Ok(crate::scalar_function::native_batch_integer_value(
                    crate::scalar_function::native_batch_integer_bits(value)?,
                    *unsigned,
                ))
            }
            Self::Normalize { field } | Self::NormalizeRoot { field, .. } => {
                let value = crate::scalar_function::normalize_numeric_row_value(
                    value,
                    Some(field),
                    target,
                    ctx,
                )?;
                // The recursive integer batch rejects each row before the next
                // SQL row runs, even under an unsigned deferred wrapper.
                let bits = crate::scalar_function::native_batch_integer_bits(value)?;
                Ok(crate::scalar_function::native_batch_integer_value(
                    bits,
                    matches!(self, Self::NormalizeRoot { unsigned: true, .. }),
                ))
            }
            _ => unreachable!("typed leaf cannot consume a child"),
        }
    }
    fn begin_bounded(
        &self,
        ctx: &dyn Columns,
        target: NativeEvalType,
        maximum: usize,
        input: Option<&Chunk>,
        row: InputRow,
    ) -> std::result::Result<crate::typed_context_value::TypedRead<'_>, EvalError> {
        let value = match self {
            Self::Context(descriptor) => return descriptor.begin_bounded(ctx, target, maximum),
            Self::Literal { value, field } => {
                let field = field
                    .as_ref()
                    .ok_or(EvalError::Unsupported("constant has no result type"))?;
                let value = crate::tikv::identity_actual::copy_bounded(value, maximum)?;
                crate::constant::typed_constant_value(field, ctx, target, value)?
            }
            Self::Column { index, field } => {
                let input = input.ok_or_else(|| {
                    EvalError::ExpressionRuntimeFailure(
                        crate::ExpressionRuntimeFailure::from_local_eval(
                            binding("typed column has no input owner"),
                            None,
                        ),
                    )
                })?;
                if row.input_row >= input.physical_rows() {
                    return Err(EvalError::ExpressionRuntimeFailure(
                        crate::ExpressionRuntimeFailure::from_local_eval(
                            binding("typed column physical row is outside input"),
                            None,
                        ),
                    ));
                }
                if target == NativeEvalType::Int
                    && !field.as_ref().is_some_and(FieldType::is_hybrid)
                {
                    let index = usize::try_from(*index)
                        .ok()
                        .filter(|&i| i < input.num_cols())
                        .ok_or(EvalError::Unsupported("invalid integer column index"))?;
                    let column = input.column(index);
                    if column.is_null(row.input_row) {
                        Datum::Null
                    } else {
                        Datum::Int(column.get_int64(row.input_row))
                    }
                } else {
                    let index = crate::column::column_read_index(
                        field.is_some(),
                        *index,
                        input.num_cols(),
                    )?;
                    let field = field.as_ref().expect("original column type guard");
                    let column = input.column(index);
                    let bytes = if column.is_null(row.input_row) {
                        0
                    } else {
                        column.raw_cells().get(row.input_row).len()
                    };
                    if bytes > maximum {
                        return Err(EvalError::ExpressionRuntimeFailure(
                            crate::ExpressionRuntimeFailure::from_local_eval(
                                resource("typed column copy allowance exhausted"),
                                None,
                            ),
                        ));
                    }
                    let value = input.physical_row(row.input_row).get_datum(index, field);
                    let value = crate::expression::apply_enum_set_integer_view(value, Some(field));
                    let value = crate::scalar_function::normalize_numeric_row_value(
                        value,
                        Some(field),
                        target,
                        ctx,
                    )?;
                    if target == NativeEvalType::Int {
                        crate::scalar_function::native_batch_integer_value(
                            crate::scalar_function::native_batch_integer_bits(value)?,
                            false,
                        )
                    } else {
                        value
                    }
                }
            }
            Self::Normalize { .. } | Self::NormalizeRoot { .. } | Self::RootForward { .. } => {
                unreachable!("normalizer needs the real child")
            }
        };
        Ok(crate::typed_context_value::TypedRead::Ready(value))
    }
}
impl NativeTypedContext {
    pub(in super::super) fn window_forward(&self) -> bool {
        self.descriptor
            .as_deref()
            .is_none_or(|d| matches!(d, NativeTypedDescriptor::RootForward { .. }))
    }
}
impl<'a> NativeBatchHosts<'a> {
    pub(in super::super) fn with_typed_cache_slots(mut self, count: usize) -> LocalResult<Self> {
        if count == 0 {
            return Ok(self);
        }
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(array_bytes::<Option<Datum>>(count)?)
                .and_then(|n| n.checked_add(count))
                .ok_or_else(|| resource("typed cache layout overflow"))?,
        )?;
        let mut seen = Vec::new();
        reserve(&mut seen, count)?;
        seen.resize(count, false);
        for metadata in self.metadata {
            if let NativeHostMetadata::TypedContext(NativeTypedContext {
                cache_slot: Some(index),
                argument,
                descriptor,
                ..
            }) = metadata
            {
                let Some(slot) = seen.get_mut(*index) else {
                    return Err(binding("typed cache slot outside source"));
                };
                if *slot || argument.is_some() || descriptor.is_none() {
                    return Err(binding("typed cache source aliases another phase"));
                }
                *slot = true;
            }
        }
        if seen.iter().any(|used| !*used) {
            return Err(binding("typed cache slot lacks its source"));
        }
        let mut values = Vec::new();
        reserve(&mut values, count)?;
        values.resize_with(count, || None);
        self.typed_values = values.into_boxed_slice();
        self.check_bytes(self.retained_bytes()?)?;
        Ok(self)
    }
    pub(super) fn validate_typed_context(
        &mut self,
        invocation: &HostInvocation<'_>,
    ) -> LocalResult<()> {
        let Some(NativeHostMetadata::TypedContext(meta)) =
            self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("typed context source absent"));
        };
        if self.native_values.is_none()
            || invocation.arg_types.len() != usize::from(meta.argument.is_some())
            || !matches!(
                meta.output_role,
                NativeValueRole::CharInput | NativeValueRole::Output
            )
            || meta
                .cache_slot
                .is_some_and(|i| i >= self.typed_values.len())
        {
            return Err(binding("typed context source differs from its graph"));
        }
        if let Some(descriptor) = &meta.descriptor {
            if descriptor.demands_child() != meta.argument.is_some()
                || (meta.argument.is_some() && meta.cache_slot.is_some())
            {
                return Err(binding("typed context child policy differs"));
            }
            if descriptor.demands_child() {
                if let Err(error) = descriptor.validate_child_type() {
                    return self.native_failure(error);
                }
            }
        } else if meta.argument.is_none() || meta.cache_slot.is_some() {
            return Err(binding("typed forward requires its actual child"));
        }
        Ok(())
    }
    pub(super) fn finish_typed_context(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        if !matches!(task.demand, SqlDemand::TypedContext { consumed: false }) {
            return Err(binding("typed context phase consumed"));
        }
        let (slot, row) = (task.slot, task.row);
        let Some(NativeHostMetadata::TypedContext(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("typed context metadata absent"));
        };
        if meta.argument.is_some() != token.is_some() {
            return Err(binding("typed context demand differs"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("typed context store absent"))?;
        let origin = meta.argument.map(|node| NativeValueOrigin { node, row });
        let base = self.retained_bytes()?;
        let copied = if let (Some(token), Some(origin)) = (token, origin) {
            native_payload_bytes(
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?,
            )?
            .checked_mul(2)
            .ok_or_else(|| resource("typed child copy extent overflow"))?
        } else {
            0
        };
        let live = base
            .checked_add(copied)
            .ok_or_else(|| resource("typed context live extent overflow"))?;
        self.check_bytes(live)?;
        let SqlDemand::TypedContext { consumed } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("authenticated typed task")
            .demand
        else {
            unreachable!()
        };
        *consumed = true;
        let maximum = self.max_retained_bytes - live;
        let cached = meta.cache_slot.and_then(|i| self.typed_values[i].as_ref());
        let value = tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
            let result = if let Some(cached) = cached {
                crate::tikv::identity_actual::copy_bounded(cached, maximum)
            } else if let (Some(token), Some(origin)) = (token, origin) {
                let value = store
                    .borrow()
                    .clone_for_kernel(token, NativeValueRole::CharInput, origin)
                    .map_err(|error| {
                        EvalError::ExpressionRuntimeFailure(
                            crate::ExpressionRuntimeFailure::from_local_eval(error, None),
                        )
                    })?;
                if let Some(descriptor) = &meta.descriptor {
                    descriptor.finish_child(value, self.context, meta.target)
                } else {
                    Ok(value)
                }
            } else {
                match meta
                    .descriptor
                    .as_ref()
                    .expect("validated typed read")
                    .begin_bounded(self.context, meta.target, maximum, self.input, row)?
                {
                    crate::typed_context_value::TypedRead::Ready(value) => Ok(value),
                    crate::typed_context_value::TypedRead::Child(_) => {
                        Err(EvalError::ExpressionRuntimeFailure(
                            crate::ExpressionRuntimeFailure::from_local_eval(
                                binding("typed context read lost its demanded child"),
                                None,
                            ),
                        ))
                    }
                }
            };
            crate::scalar_function::eager_values::audit_work_result(result)
        });
        if let (Some(token), Some(origin)) = (token, origin) {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let save = meta.cache_slot.filter(|&i| self.typed_values[i].is_none());
        let extra = if save.is_some() {
            native_payload_bytes(&value)?
                .checked_mul(2)
                .ok_or_else(|| resource("typed cache value extent overflow"))?
        } else {
            0
        };
        let held = live
            .checked_add(extra)
            .ok_or_else(|| resource("typed cache live extent overflow"))?;
        self.check_bytes(held)?;
        let saved = if save.is_some() {
            match crate::tikv::identity_actual::copy_bounded(&value, self.max_retained_bytes - held)
            {
                Ok(value) => Some(value),
                Err(error) => return self.native_failure(error),
            }
        } else {
            None
        };
        let output =
            self.finish_native_value(id, value, meta.output_node, meta.output_role, row, held)?;
        if let Some(index) = save {
            self.typed_values[index] = saved;
        }
        self.check_bytes(self.retained_bytes()?)?;
        Ok(HostStep::Ready(output))
    }
}
