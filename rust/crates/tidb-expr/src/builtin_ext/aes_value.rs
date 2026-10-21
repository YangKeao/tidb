// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! AES argument phases over actual values; neither an expression nor a driver.
use super::{aes_operation, sql_string_bytes};
use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp};
use crate::{BlockEncryptionMode, Columns, Datum, EvalError};

pub(crate) struct AesPreparation {
    function: &'static str,
    count: usize,
    mode: BlockEncryptionMode,
    next: usize,
    input: Option<Vec<u8>>,
    password: Option<Vec<u8>>,
}
impl AesPreparation {
    pub(crate) fn new(function: &'static str, count: usize, ctx: &dyn Columns) -> Self {
        Self {
            function,
            count,
            mode: ctx.block_encryption_mode(),
            next: 0,
            input: None,
            password: None,
        }
    }
    pub(crate) fn validate_arity(&self) -> Result<(), EvalError> {
        if if self.mode.iv_required() {
            self.count == 3
        } else {
            (2..=3).contains(&self.count)
        } {
            Ok(())
        } else {
            Err(EvalError::WrongParameterCount(self.function))
        }
    }
    pub(crate) fn next_argument(&self) -> usize {
        self.next
    }
    pub(crate) fn owned_payload_bytes(&self) -> Option<usize> {
        self.input
            .as_ref()
            .map_or(0, Vec::capacity)
            .checked_add(self.password.as_ref().map_or(0, Vec::capacity))
    }
    pub(crate) fn accept(
        &mut self,
        value: &Datum,
        ctx: &dyn Columns,
    ) -> Result<Option<(EvaluatedBytesOp, EvaluatedArgs)>, EvalError> {
        self.validate_arity()?;
        let bytes = if let Some(remaining) = tidb_query_expr::local::value_kernel_work_remaining() {
            let bytes = crate::tikv::crypto_value::input(value, false, remaining)?;
            bytes
                .map(|bytes| {
                    tidb_query_expr::local::charge_value_kernel_work(bytes.len()).map_err(
                        |error| {
                            EvalError::ExpressionRuntimeFailure(
                                crate::ExpressionRuntimeFailure::from_local_eval(error, None),
                            )
                        },
                    )?;
                    Ok(bytes.into_owned())
                })
                .transpose()?
        } else {
            sql_string_bytes(value)?
        };
        let Some(bytes) = bytes else {
            self.input = None;
            self.password = None;
            // This witness exists only after observing this genuine SQL NULL.
            return Ok(Some((
                EvaluatedBytesOp::AesNullNative,
                EvaluatedArgs::NullWitness(None),
            )));
        };
        if self.next == 0 {
            self.input = Some(bytes);
            self.next = 1;
            return Ok(None);
        }
        if self.next == 1 && self.mode.iv_required() {
            self.password = Some(bytes);
            self.next = 2;
            return Ok(None);
        }
        let args = if self.mode.iv_required() {
            EvaluatedArgs::Bytes3([self.input.take(), self.password.take(), Some(bytes)])
        } else {
            if self.count == 3 {
                ctx.append_warning(1618, "<IV> option ignored");
            }
            EvaluatedArgs::Bytes2(self.input.take(), Some(bytes))
        };
        Ok(Some((
            aes_operation(self.function == "aes_encrypt", self.mode),
            args,
        )))
    }
}
