// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Stateful random services and lazy AES use the existing argument broker.
use super::*;
use crate::builtin_ext::crypto::AesPreparation;

pub(in super::super) struct NativeFinalState {
    pub arguments: Box<[usize]>,
    pub name: &'static str,
    // The original, borrowed ScalarFunction occurrence; never a compiled slot.
    pub function_key: usize,
    pub constant_seed: bool,
    pub result_type: FieldType,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) struct FinalTask {
    pub next: usize,
    pub immediate: bool,
    aes: Option<AesPreparation>,
    tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
}
impl FinalTask {
    pub(super) fn start(meta: &NativeFinalState, ctx: &dyn Columns) -> Self {
        let aes = meta
            .name
            .starts_with("aes_")
            .then(|| AesPreparation::new(meta.name, meta.arguments.len(), ctx));
        let immediate = match &aes {
            Some(aes) => aes.validate_arity().is_err(),
            None => meta.name == "tidb_is_ddl_owner" || meta.arguments.is_empty(),
        };
        Self {
            next: 0,
            immediate,
            aes,
            tokens: Vec::new(),
        }
    }
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(array_bytes::<[u8; native_values::HANDLE_BYTES]>(
                self.tokens.capacity(),
            )?)
            .and_then(|n| {
                self.aes
                    .as_ref()
                    .map_or(Some(0), AesPreparation::owned_payload_bytes)
                    .and_then(|more| n.checked_add(more))
            })
            .ok_or_else(|| resource("final state continuation extent overflow"))
    }
}
enum Progress {
    Ready(Datum),
    Cipher(crate::tikv::EvaluatedBytesOp, crate::tikv::EvaluatedArgs),
    More,
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_final_state(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::FinalState(state) = &task.demand else {
            return Err(binding("final state phase differs"));
        };
        let (slot, row, index) = (task.slot, task.row, state.next);
        let Some(NativeHostMetadata::FinalState(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("final state metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("final state store absent"))?;
        let origin = |i: usize| -> LocalResult<NativeValueOrigin> {
            Ok(NativeValueOrigin {
                node: *meta
                    .arguments
                    .get(i)
                    .ok_or_else(|| binding("final state child ordinal absent"))?,
                row,
            })
        };
        for (i, previous) in state.tokens.iter().enumerate() {
            store
                .borrow()
                .get(previous, NativeValueRole::CharInput, origin(i)?)?;
        }
        if let Some(token) = token {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin(index)?)?;
        }
        let base = self.retained_bytes()?;
        self.check_bytes(base)?;
        let maximum = self.max_retained_bytes - base;
        let SqlDemand::FinalState(mut state) = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated final state task")
                .demand,
            SqlDemand::Consumed,
        ) else {
            unreachable!()
        };
        if let Some(token) = token {
            reserve(&mut state.tokens, 1)?;
            state.tokens.push(
                token
                    .try_into()
                    .map_err(|_| binding("final state capability width differs"))?,
            );
        }
        // Invalid RAND arity still demands every real child. No payload is
        // cloned merely to reject it once that demanded prefix completes.
        let more_rand = meta.name == "rand" && index + 1 < meta.arguments.len();
        let lease = if more_rand || (meta.name == "rand" && meta.arguments.len() > 1) {
            None
        } else {
            token
                .map(|token| {
                    NativeValueStore::lease_for_kernel(
                        store,
                        token,
                        NativeValueRole::CharInput,
                        origin(index)?,
                    )
                })
                .transpose()?
        };
        let mut remaining = maximum;
        let result = tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
            let result = (|| -> std::result::Result<Progress, EvalError> {
                let result = match meta.name {
                    "rand" => {
                        if more_rand {
                            return Ok(Progress::More);
                        }
                        match meta.arguments.len() {
                            0 => crate::math_fn::eval_rand_values(
                                &[],
                                self.context,
                                Some(meta.function_key),
                                meta.constant_seed,
                            )?,
                            1 => crate::math_fn::eval_rand_values(
                                std::slice::from_ref(
                                    lease.as_ref().expect("actual seed reply").datum(),
                                ),
                                self.context,
                                Some(meta.function_key),
                                meta.constant_seed,
                            )?,
                            _ => return Err(EvalError::Unsupported("bad function arity")),
                        }
                    }
                    "tidb_is_ddl_owner" => crate::scalar_function::stateful::ddl_owner(
                        meta.arguments.len(),
                        self.context,
                    )?,
                    "aes_encrypt" | "aes_decrypt" => {
                        let aes = state.aes.as_mut().expect("captured AES preparation");
                        aes.validate_arity()?;
                        assert_eq!(aes.next_argument(), index, "authenticated AES ordinal");
                        let datum = lease.as_ref().expect("actual AES argument reply").datum();
                        return Ok(match aes.accept(datum, self.context)? {
                            Some((operation, args)) => Progress::Cipher(operation, args),
                            None => Progress::More,
                        });
                    }
                    _ => unreachable!("closed final state source"),
                };
                Ok(Progress::Ready(
                    crate::tikv::identity_actual::coerce_result(
                        meta.name,
                        &meta.result_type,
                        result,
                        tidb_query_expr::local::value_kernel_work_remaining()
                            .expect("finite final state scope"),
                    )?,
                ))
            })();
            remaining = tidb_query_expr::local::value_kernel_work_remaining()
                .expect("finite preparation scope");
            crate::scalar_function::eager_values::audit_work_result(result)
        });
        // A callback may have reset the store. No Ref is live during services;
        // authenticate the leased slot as well as every earlier demanded input.
        for (i, previous) in state.tokens.iter().enumerate() {
            store
                .borrow()
                .authenticate(previous, NativeValueRole::CharInput, origin(i)?)?;
        }
        drop(lease);
        // Warning callbacks have returned and every real input is authenticated
        // again BEFORE any prepared value crosses into the cipher worker.
        let result = match result {
            Ok(Progress::Cipher(operation, args)) => {
                let cache = self.slots[id.slot]
                    .ready_preparation
                    .as_ref()
                    .map(|guard| guard.cache());
                tidb_query_expr::local::with_value_kernel_work_limit(remaining, || {
                    let result = (|| {
                        let value = crate::tikv::evaluate_closed_args(cache, operation, args)?
                            .into_bytes()?
                            .map_or(Datum::Null, Datum::new_string);
                        crate::tikv::identity_actual::coerce_result(
                            meta.name,
                            &meta.result_type,
                            value,
                            tidb_query_expr::local::value_kernel_work_remaining()
                                .expect("finite cipher return scope"),
                        )
                        .map(Progress::Ready)
                    })();
                    crate::scalar_function::eager_values::audit_work_result(result)
                })
            }
            result => result,
        };
        for (i, previous) in state.tokens.iter().enumerate() {
            store
                .borrow()
                .get(previous, NativeValueRole::CharInput, origin(i)?)?;
        }
        match result {
            Err(error) => self.native_failure(error),
            Ok(Progress::Ready(value)) => self
                .finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
                .map(HostStep::Ready),
            Ok(Progress::Cipher(..)) => unreachable!("closed cipher already materialized"),
            Ok(Progress::More) => {
                state.next = index + 1;
                self.check_bytes(
                    base.checked_add(state.retained_bytes()?)
                        .ok_or_else(|| resource("final state pending extent overflow"))?,
                )?;
                self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("live consumed final state task")
                    .demand = SqlDemand::FinalState(state);
                Ok(HostStep::NeedArg(HostArgRequest {
                    index: index + 1,
                    mode: ArgMode::Fresh,
                }))
            }
        }
    }
}
