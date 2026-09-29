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

//! Invocation-local demanded row inputs. EvalContext is a disjoint caller-owned
//! value, not a borrowed member of a mutable Session. No Columns evaluator,
//! diagnostic sink, TLS guard, host task or native expression closure is stored.

use tidb_chunk::{chunk::Chunk, column::get_fixed_len};
use tidb_datatype::tikv_compat::value::to_scalar;
use tidb_datatype::{Datum, FieldType};
use tidb_query_datatype::{codec::data_type::VectorValue, expr::EvalContext, EvalType};
use tidb_query_expr::local::{InputRow, LocalError, LocalResult, LocalRuntimeServices};

use super::lower::LoweredSpec;

pub(super) struct NativeInputs<'a> {
    spec: &'a LoweredSpec,
    chunk: &'a Chunk,
    selection: &'a [usize],
    physical_rows: usize,
}

impl<'a> NativeInputs<'a> {
    /// Check declaration, layout and occurrence universe without importing any
    /// value. Chunk owns its valid byte buffers; this is not a raw-wire decoder.
    pub(super) fn new(
        spec: &'a LoweredSpec,
        chunk: &'a Chunk,
        row_schema: &[FieldType],
        selection: &'a [usize],
    ) -> LocalResult<Self> {
        if row_schema != spec.row_schema.as_ref() || chunk.num_cols() != row_schema.len() {
            return Err(LocalError::InvalidBatch(
                "native row schema/column count differs from preparation".into(),
            ));
        }
        let physical_rows = chunk.physical_rows();
        for (index, sql_type) in row_schema.iter().enumerate() {
            let column = chunk.column(index);
            if column.rows() != physical_rows || column.type_size() != get_fixed_len(sql_type) {
                return Err(LocalError::InvalidBatch(format!(
                    "native column {index} has incompatible length/layout"
                )));
            }
        }
        if selection.iter().any(|row| *row >= physical_rows) {
            return Err(LocalError::InvalidBatch(
                "native selection is outside the physical row universe".into(),
            ));
        }
        Ok(Self {
            spec,
            chunk,
            selection,
            physical_rows,
        })
    }

    /// One demanded native value, before any bridge erases its Datum kind.
    /// The D1 adapter below preserves its existing conversion and call order.
    pub(super) fn read_native_datum(
        &self,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        let binding = self
            .spec
            .bindings
            .get(slot)
            .ok_or_else(|| LocalError::BindingContract("unknown native input slot".into()))?;
        if self.spec.schema.get(slot) != Some(expected)
            || row.input_row >= self.physical_rows
            || self.selection.get(row.occurrence) != Some(&row.input_row)
        {
            return Err(LocalError::BindingContract(
                "native input type/occurrence differs from its contract".into(),
            ));
        }
        // input_row is already physical. get_row() would apply Chunk::Sel again.
        // The entrypoint admits the declaration; new() checks its native layout
        // before effects. This read preserves its actual Datum kind/collation;
        // each adapter must validate those before erasure. D1 below still uses
        // its original Int conversion. Drop every row/column borrow on return.
        Ok(self
            .chunk
            .physical_row(row.input_row)
            .get_datum(binding.index, &binding.sql_type))
    }
}

impl LocalRuntimeServices for NativeInputs<'_> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        &self.spec.schema
    }

    fn read_input(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        let value = self.read_native_datum(slot, row, expected)?;
        let (value, _) = to_scalar(&value, EvalType::Int)
            .map_err(|error| LocalError::BindingContract(error.to_string()))?;
        Ok(VectorValue::from_scalar(&value, 1))
    }
}
