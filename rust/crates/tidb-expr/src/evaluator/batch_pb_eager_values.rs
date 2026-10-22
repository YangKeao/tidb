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
pub(in super::super) enum PbValueKernel {
    Eager(crate::scalar_function::PbEagerValues),
    Json(Box<crate::scalar_function::PbJsonValues>),
}
impl PbValueKernel {
    pub(in super::super) fn signature(&self) -> tidb_proto::tipb::ScalarFuncSig {
        match self {
            Self::Eager(kernel) => kernel.signature(),
            Self::Json(kernel) => kernel.signature(),
        }
    }
    fn stops_on_null(&self) -> bool {
        matches!(self, Self::Eager(_))
    }
    fn null_result(
        &self,
        observed: &Datum,
        arity: usize,
        index: usize,
        context: &dyn Columns,
    ) -> std::result::Result<Datum, EvalError> {
        match self {
            Self::Eager(kernel) => kernel.null_result(observed, arity, index, context),
            Self::Json(_) => unreachable!("JSON demands every raw argument"),
        }
    }
    fn eval_values(
        &self,
        values: &mut [Datum],
        context: &dyn Columns,
    ) -> std::result::Result<Datum, EvalError> {
        match self {
            Self::Eager(kernel) => kernel.eval_values(values, context),
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
            self.check_bytes(
                self.retained_bytes()?
                    .checked_add(native_values::HANDLE_BYTES)
                    .ok_or_else(|| resource("PB eager prefix extent overflow"))?,
            )?;
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
            self.check_bytes(self.retained_bytes()?)?;
            if !is_null && next + 1 < arity {
                return Ok(HostStep::NeedArg(HostArgRequest {
                    index: next + 1,
                    mode: ArgMode::Fresh,
                }));
            }
        }
        let base = self.retained_bytes()?;
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
        let copied = {
            let values = store.borrow();
            tokens
                .iter()
                .enumerate()
                .filter(|(index, _)| !is_null || *index == next)
                .try_fold(0usize, |sum, (index, token)| {
                    sum.checked_add(
                        native_payload_bytes(values.get(
                            token,
                            NativeValueRole::CharInput,
                            origin(index),
                        )?)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("PB eager copy extent overflow"))?,
                    )
                    .ok_or_else(|| resource("PB eager input extent overflow"))
                })?
        };
        let mut live = base
            .checked_add(copied)
            .and_then(|n| n.checked_add(array_bytes::<Datum>(demanded).ok()?))
            .ok_or_else(|| resource("PB eager argument layout overflow"))?;
        self.check_bytes(live)?;
        let mut ready = Vec::new();
        ready
            .try_reserve_exact(demanded)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        live = base
            .checked_add(copied)
            .and_then(|n| n.checked_add(array_bytes::<Datum>(ready.capacity()).ok()?))
            .ok_or_else(|| resource("PB eager argument capacity overflow"))?;
        self.check_bytes(live)?;
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
        let maximum = self.max_retained_bytes - live;
        let result = tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
            crate::scalar_function::eager_values::audit_work_result(if is_null {
                meta.kernel
                    .null_result(&ready[0], arity, next, self.context)
            } else {
                meta.kernel.eval_values(&mut ready, self.context)
            })
        });
        check_tokens()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        drop(ready);
        let value =
            crate::scalar_function::interpret_integer_result(value, meta.result_type.as_ref());
        let held = base
            .checked_add(
                native_payload_bytes(&value)?
                    .checked_mul(2)
                    .ok_or_else(|| resource("PB eager result allowance overflow"))?,
            )
            .ok_or_else(|| resource("PB eager result extent overflow"))?;
        self.check_bytes(held)?;
        let result = coerce_optional_value_result(
            &meta.name,
            meta.result_type.as_ref(),
            value,
            self.max_retained_bytes - held,
        );
        check_tokens()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = if meta.integer_batch_result {
            let result = present_integer_batch_value(
                value,
                meta.result_type.as_ref(),
                self.context,
                self.max_retained_bytes - held,
            );
            check_tokens()?;
            match result {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            }
        } else {
            value
        };
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
            .map(HostStep::Ready)
    }
}
