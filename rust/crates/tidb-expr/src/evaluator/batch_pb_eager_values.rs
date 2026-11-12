// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Selected PB eager values in the existing SDK host frame. Raw operands are
//! retained as capabilities until NULL stops demand or the original body runs.
use super::*;

pub(in super::super) struct NativePbEagerValues {
    pub arguments: Box<[usize]>,
    pub kernel: PbValueKernel,
    pub name: Box<str>,
    pub result_type: Option<FieldType>,
    pub integer_batch_result: bool,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(in super::super) struct PbCastKernel {
    pub kernel: crate::scalar_function::PbCastValues,
    pub source_type: Option<FieldType>,
    pub input: NativeValuePreparation,
}
impl PbCastKernel {
    fn eval_value(
        &self,
        value: Datum,
        result: Option<&FieldType>,
        context: &dyn Columns,
    ) -> std::result::Result<Datum, EvalError> {
        let target = self.kernel.source();
        let value = match self.input {
            NativeValuePreparation::Literal => crate::constant::typed_constant_value(
                self.source_type
                    .as_ref()
                    .ok_or(EvalError::Unsupported("constant has no result type"))?,
                context,
                target,
                value,
            )?,
            NativeValuePreparation::Computed => {
                crate::scalar_function::normalize_numeric_row_value(
                    value,
                    self.source_type.as_ref(),
                    target,
                    context,
                )?
            }
            NativeValuePreparation::TypedReady => value,
        };
        let value = match (target, value) {
            (NativeEvalType::Int, Datum::UInt(v)) => Datum::Int(v as i64),
            (NativeEvalType::Real, Datum::Float32(v)) => Datum::Real(v),
            (_, v) => v,
        };
        self.kernel
            .eval_value(value, self.source_type.as_ref(), result, context)
    }
}
pub(in super::super) enum PbValueKernel {
    Eager(crate::scalar_function::PbEagerValues),
    Cast(Box<PbCastKernel>),
    Json(Box<crate::scalar_function::PbJsonValues>),
}
impl PbValueKernel {
    pub(in super::super) fn signature(&self) -> tidb_proto::tipb::ScalarFuncSig {
        match self {
            Self::Eager(kernel) => kernel.signature(),
            Self::Cast(meta) => meta.kernel.signature(),
            Self::Json(kernel) => kernel.signature(),
        }
    }
    fn stops_on_null(&self) -> bool {
        matches!(self, Self::Eager(_) | Self::Cast(_))
    }
    fn null_result(
        &self,
        observed: &Datum,
        arity: usize,
        index: usize,
        context: &dyn Columns,
        result: Option<&FieldType>,
    ) -> std::result::Result<Datum, EvalError> {
        match self {
            Self::Eager(kernel) => kernel.null_result(observed, arity, index, context),
            Self::Cast(meta) => {
                meta.kernel
                    .eval_value(observed.clone(), meta.source_type.as_ref(), result, context)
            }
            Self::Json(_) => unreachable!("JSON demands every raw argument"),
        }
    }
    fn eval_values(
        &self,
        values: &mut [Datum],
        context: &dyn Columns,
        result: Option<&FieldType>,
    ) -> std::result::Result<Datum, EvalError> {
        match self {
            Self::Eager(kernel) => kernel.eval_values(values, context),
            Self::Cast(meta) => {
                if values.len() != 1 {
                    return Err(crate::scalar_function::PbCastValues::arity_error());
                }
                meta.eval_value(std::mem::take(&mut values[0]), result, context)
            }
            Self::Json(kernel) => kernel.eval_values(values, context),
        }
    }
}

impl NativeBatchHosts<'_> {
    pub(super) fn resume_pb_eager_values(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::PbEagerValues { tokens } = &task.demand else {
            return Err(binding("PB eager phase differs"));
        };
        let (slot, row, arity, next) = (task.slot, task.row, task.arity, tokens.len());
        let Some(NativeHostMetadata::PbEagerValues(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("PB eager metadata absent"));
        };
        if meta.arguments.len() != arity
            || (token.is_none() != (arity == 0))
            || next >= arity.max(1)
        {
            return Err(binding("PB eager phase exceeds actual arity"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("PB eager store absent"))?;
        let origin = |index| NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        let is_null = {
            let values = store.borrow();
            for (index, token) in tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
            match token {
                Some(token) => values
                    .get(token, NativeValueRole::CharInput, origin(next))?
                    .is_null(),
                None => false,
            }
        };
        let is_null = is_null && meta.kernel.stops_on_null();
        if let Some(token) = token {
            let owned = token
                .try_into()
                .map_err(|_| binding("PB eager capability width differs"))?;

            let SqlDemand::PbEagerValues { tokens } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated PB eager task")
                .demand
            else {
                unreachable!()
            };
            tokens
                .try_reserve_exact(1)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            tokens.push(owned);

            if !is_null && next + 1 < arity {
                return Ok(HostStep::NeedArg(HostArgRequest {
                    index: next + 1,
                    mode: ArgMode::Fresh,
                }));
            }
        }

        let SqlDemand::PbEagerValues { tokens } = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated PB eager task")
                .demand,
            SqlDemand::Consumed,
        ) else {
            unreachable!()
        };
        let demanded = if is_null { 1 } else { arity };
        {
            let values = store.borrow();
            for (index, token) in tokens
                .iter()
                .enumerate()
                .filter(|(index, _)| !is_null || *index == next)
            {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
        }

        let mut ready = Vec::new();
        ready
            .try_reserve_exact(demanded)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;

        {
            let values = store.borrow();
            for (index, token) in tokens
                .iter()
                .enumerate()
                .filter(|(index, _)| !is_null || *index == next)
            {
                ready.push(values.clone_for_kernel(
                    token,
                    NativeValueRole::CharInput,
                    origin(index),
                )?);
            }
        }
        let check_tokens = || -> LocalResult<()> {
            let values = store.borrow();
            for (index, token) in tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
            Ok(())
        };

        let result = (|| {
            (if is_null {
                meta.kernel.null_result(
                    &ready[0],
                    arity,
                    next,
                    self.context,
                    meta.result_type.as_ref(),
                )
            } else {
                meta.kernel
                    .eval_values(&mut ready, self.context, meta.result_type.as_ref())
            })
        })();
        check_tokens()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        drop(ready);
        let value =
            crate::scalar_function::interpret_integer_result(value, meta.result_type.as_ref());

        let result = crate::scalar_function::coerce_function_result(
            &meta.name,
            meta.result_type.as_ref(),
            value,
        );
        check_tokens()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = if meta.integer_batch_result {
            let result =
                present_integer_batch_value(value, meta.result_type.as_ref(), self.context);
            check_tokens()?;
            match result {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            }
        } else {
            value
        };
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row)
            .map(HostStep::Ready)
    }
}
