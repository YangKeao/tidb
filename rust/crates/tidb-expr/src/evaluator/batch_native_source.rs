// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! The genuine public native candidate in the existing SDK batch frame.
use super::*;

// A public Chunk's selected coordinates retain their original demand-time
// cell behavior. No invented row universe or physical SQL schema is supplied.
pub(super) struct Coordinates<'a> {
    pub(super) inner: NativeChunk<'a>,
}
impl NativeDatumSource for Coordinates<'_> {
    fn schema(&self) -> &[tipb::FieldType] {
        self.inner.schema()
    }
    fn read(
        &mut self,
        _: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        if self.inner.source.schema.get(slot) != Some(expected)
            || self.inner.selection.get(row.occurrence) != Some(&row.input_row)
        {
            return Err(binding("native candidate occurrence/type differs"));
        }
        let ordinal = *self
            .inner
            .source
            .bindings
            .get(slot)
            .ok_or_else(|| binding("unknown native candidate slot"))?;
        read_source_row(
            self.inner.source,
            self.inner.native_values,
            self.inner.input.physical_row(row.input_row),
            ordinal,
            row,
        )
    }
    fn read_batch(
        &mut self,
        context: &mut EvalContext,
        rows_slot: usize,
        coordinates: &[usize],
        expected: &tipb::FieldType,
    ) -> LocalResult<Option<VectorValue>> {
        if let Some(ordinal) = self.inner.source.bindings.get(rows_slot).copied() {
            if let SourceKind::DecimalFastInput(value) = &self.inner.source.nodes[ordinal].kind {
                if self.inner.source.schema.get(rows_slot) != Some(expected)
                    || coordinates != self.inner.selection
                {
                    return Err(binding("native fast occurrence/type differs"));
                }
                return decimal_fast_source::read_batch(
                    value,
                    self.inner.input,
                    self.inner.native_values,
                    ordinal,
                    coordinates,
                )
                .map(Some);
            }
        }
        if coordinates
            .iter()
            .all(|row| *row < self.inner.input.physical_rows())
        {
            self.inner
                .read_batch(context, rows_slot, coordinates, expected)
        } else {
            // The actual demanded leaf retains the original panic/error or
            // succeeds without a physical read. Occurrence authentication stays.
            Ok(None)
        }
    }
}

pub(super) fn eval(
    candidate: crate::scalar_function::EligibleBatch<'_>,
    context: &dyn Columns,
    input: &Chunk,
) -> std::result::Result<Vec<Datum>, EvalError> {
    let (limits, execution, maximum) = public_scalar_limits();
    eval_with_policies(candidate, context, input, limits, execution, maximum)
}
pub(super) fn eval_with_policies(
    candidate: crate::scalar_function::EligibleBatch<'_>,
    context: &dyn Columns,
    input: &Chunk,
    limits: BatchSourceLimits,
    execution: ExecutionLimits,
    _maximum: usize,
) -> std::result::Result<Vec<Datum>, EvalError> {
    if input.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let root = candidate.expression();
    let scalar_rows = crate::scalar_function::native_scalar_row_batch(root);
    let view = if scalar_rows {
        NativeView::RootValue
    } else {
        NativeView::BulkRoot(candidate.target())
    };
    let source = lower_source_plan(
        root,
        SourceInputs::NativeBatch {
            width: input.num_cols(),
        },
        true,
        1,
        limits,
        Some(view),
        SourceConsumer::Scalar,
    )
    .map_err(|e| e.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))?;
    let mut program = compile_batch(
        &source.expr,
        &source.schema,
        LocalCompileContext {
            limits: limits.tree,
        },
        &source.facts,
    )
    .map_err(|e| {
        BatchFailure::from(e).into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare)
    })?;
    execute(
        &source,
        &mut program,
        context,
        input,
        execution,
        scalar_rows,
    )
    .map_err(|e| e.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke))
}
fn execute(
    source: &SourcePlan,
    program: &mut LocalBatchProgram,
    context: &dyn Columns,
    input: &Chunk,
    execution: ExecutionLimits,
    scalar_rows: bool,
) -> Result<Vec<Datum>> {
    if source.nodes[0].view.value_role() != Some(NativeValueRole::Output) {
        return Err(binding("native candidate output role differs").into());
    }
    let rows = input.num_rows();

    let mut selection = Vec::new();
    selection
        .try_reserve_exact(rows)
        .map_err(|_| resource("native candidate selection allocation failed"))?;
    if let Some(selected) = input.sel() {
        selection.extend_from_slice(selected);
    } else {
        selection.extend(0..rows);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(rows)
        .map_err(|_| resource("native candidate output allocation failed"))?;

    let store = RefCell::new(NativeValueStore::new()?);
    store.borrow().check_live()?;
    let policy = source.context_policy.as_deref();
    let mut hosts = source
        .hosts
        .clone()
        .map(|catalog| {
            NativeBatchHosts::new(catalog, &source.host_metadata, context)
                .with_native_values(Some(&store))
                .with_coordinate_input(input)
                .with_typed_cache_slots(policy.map_or(0, |p| p.typed_cache_slots))?
                .with_constant_probes(&source.host_probes)
                .with_constant_root_shortcut(source.constant_root_shortcut)
                .with_diagnostic_parents(policy.map_or(&[], |p| p.diagnostic_parents.as_ref()))?
                .with_numeric_parents(policy.map_or(&[], |p| p.numeric_parents.as_ref()))?
                .with_diagnostic_slots(policy.map_or(&[], |p| p.diagnostic_slots.as_ref()))
        })
        .transpose()?;
    let row_domain = scalar_rows || source.context_row_only;
    let whole = program.requires_whole_selection() && !row_domain;
    let window = if row_domain {
        1
    } else {
        program.max_batch_size()
    };
    let mut start = 0usize;
    while start < selection.len() {
        let end = if whole {
            selection.len()
        } else {
            selection.len().min(start.saturating_add(window))
        };
        let selected = &selection[start..end];
        store.borrow_mut().begin_window()?;
        let mut native = Coordinates {
            inner: NativeChunk {
                source,
                native_values: Some(&store),
                input,
                selection: selected,
            },
        };
        let mut bindings = CheckedInputs {
            source: &mut native,
            hosts: hosts.as_mut().map(|h| h as &mut dyn LocalHostServices),
        };
        let values = program.eval_coordinates_with_bindings(
            execution,
            &mut EvalContext::default(),
            selected,
            &mut bindings,
        );
        drop(bindings);
        if let Some(error) = hosts.as_mut().and_then(NativeBatchHosts::take_native_error) {
            return Err(error.into());
        }
        let VectorValue::Bytes(values) = values? else {
            return Err(binding("native candidate output carrier differs").into());
        };
        if values.len() != selected.len() {
            return Err(binding("native candidate output length differs").into());
        }

        let store = store.borrow();
        for (occurrence, &input_row) in selected.iter().enumerate() {
            let token = values
                .get(occurrence)
                .ok_or_else(|| binding("native candidate output is wire NULL"))?;
            store.get(
                token,
                NativeValueRole::Output,
                NativeValueOrigin {
                    node: 0,
                    row: InputRow {
                        occurrence,
                        input_row,
                    },
                },
            )?;
        }
        for (occurrence, &input_row) in selected.iter().enumerate() {
            let value = store.get(
                values.get(occurrence).expect("authenticated token"),
                NativeValueRole::Output,
                NativeValueOrigin {
                    node: 0,
                    row: InputRow {
                        occurrence,
                        input_row,
                    },
                },
            )?;

            output.push(value.clone());
        }
        start = end;
    }
    Ok(output)
}
