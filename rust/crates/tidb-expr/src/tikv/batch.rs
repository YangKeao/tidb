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

//! Prepare once per worker, execute only through the official local RPN driver.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::tikv_compat::value::{from_scalar, ValueMetadata};
use tidb_datatype::{Datum, DatumKind, FieldType};
use tidb_query_datatype::{codec::data_type::VectorValue, expr::EvalContext, EvalType};
#[cfg(test)]
use tidb_query_expr::local::LocalRuntimeServices;
use tidb_query_expr::local::{
    compile_local, ExecutionLimits, LocalCompileContext, LocalError, LocalEvalState, LocalProgram,
};

use super::context::NativeInputs;
use super::lower::LoweredSpec;
use super::SeedResult;

pub(crate) struct PreparedIntControlSeed {
    spec: Arc<LoweredSpec>,
    program: LocalProgram,
    state: LocalEvalState,
}

impl PreparedIntControlSeed {
    pub(crate) fn compile(spec: Arc<LoweredSpec>, limits: ExecutionLimits) -> SeedResult<Self> {
        let program = compile_local(
            &spec.expr,
            &spec.schema,
            LocalCompileContext {
                limits: spec.limits,
            },
        )?;
        Ok(Self {
            spec,
            program,
            state: LocalEvalState::with_limits(limits),
        })
    }

    /// Selection is explicitly a sequence of physical row occurrences, not the
    /// filter API's mask/universe policy. No native values are read up front.
    pub(crate) fn eval_selected(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        selection: &[usize],
    ) -> SeedResult<Vec<Datum>> {
        let mut inputs = NativeInputs::new(&self.spec, chunk, row_schema, selection)?;
        let output = self.program.eval_with_bindings(
            &mut self.state,
            ctx,
            chunk.physical_rows(),
            selection,
            &mut inputs,
        )?;
        materialize(output, selection.len())
    }

    pub(crate) fn eval_one(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        physical_row: usize,
    ) -> SeedResult<Datum> {
        let mut values = self.eval_selected(ctx, chunk, row_schema, &[physical_row])?;
        values
            .pop()
            .ok_or_else(|| LocalError::BindingContract("missing singleton result".into()).into())
    }

    // Exercise the released C2a seam with poisoned/recording leaf services in
    // tests; not an alternate production route that skips native preflight.
    #[cfg(test)]
    pub(super) fn eval_test_services(
        &mut self,
        ctx: &mut EvalContext,
        physical_rows: usize,
        selection: &[usize],
        inputs: &mut dyn LocalRuntimeServices,
    ) -> SeedResult<Vec<Datum>> {
        let output = self.program.eval_with_bindings(
            &mut self.state,
            ctx,
            physical_rows,
            selection,
            inputs,
        )?;
        materialize(output, selection.len())
    }
}

fn materialize(output: VectorValue, expected_rows: usize) -> SeedResult<Vec<Datum>> {
    if output.eval_type() != EvalType::Int || output.len() != expected_rows {
        return Err(LocalError::BindingContract(
            "local output differs from IntControlSeed shape".into(),
        )
        .into());
    }
    // Every seed expression, including a passthrough, has the same signed Int
    // non-null identity. NULL comes from the nullable carrier. Mixed selected
    // Bytes/UInt/provenance reconstruction is deliberately not admitted here.
    let identity = ValueMetadata {
        kind: DatumKind::Int,
        string_collation: None,
        decimal_declared_shape: None,
    };
    (0..output.len())
        .map(|index| {
            from_scalar(output.get_scalar_ref(index), EvalType::Int, &identity).map_err(Into::into)
        })
        .collect()
}
