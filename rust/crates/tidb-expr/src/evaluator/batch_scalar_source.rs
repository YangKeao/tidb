// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Borrowed public-source proof, before retiring the original public oracle.
//! No expression, Suite, SQL declaration or physical Chunk is manufactured.
use super::*;
use tidb_chunk::row::Row;

pub(super) struct BorrowedExpression<'a> {
    // The lifetime pins the genuine root; no context or live argument value is
    // retained in its detached source plan.
    _root: &'a Expression,
    width: usize,
    source: SourcePlan,
    program: LocalBatchProgram,
    execution: ExecutionLimits,
    maximum: usize,
}
struct PhysicalRow<'a> {
    source: &'a SourcePlan,
    values: &'a RefCell<NativeValueStore>,
    native: Row<'a>,
}
impl NativeDatumSource for PhysicalRow<'_> {
    fn schema(&self) -> &[tipb::FieldType] {
        &self.source.schema
    }
    fn read(
        &mut self,
        _: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        if self.source.schema.get(slot) != Some(expected)
            || row.occurrence != 0
            || row.input_row != self.native.idx()
        {
            return Err(binding("scalar demanded occurrence/type differs"));
        }
        let ordinal = *self
            .source
            .bindings
            .get(slot)
            .ok_or_else(|| binding("unknown scalar slot"))?;
        read_source_row(self.source, Some(self.values), self.native, ordinal, row)
    }
}
impl<'a> BorrowedExpression<'a> {
    pub(super) fn compile(
        root: &'a Expression,
        width: usize,
        new_collation: bool,
        limits: BatchSourceLimits,
        execution: ExecutionLimits,
        maximum: usize,
    ) -> Result<Self> {
        let source = lower_source_plan(
            root,
            SourceInputs::PhysicalRow { width },
            new_collation,
            1,
            limits,
            Some(NativeView::RootValue),
            SourceConsumer::Scalar,
        )?;
        let program = compile_batch(
            &source.expr,
            &source.schema,
            LocalCompileContext {
                limits: limits.tree,
            },
            &source.facts,
        )?;
        Ok(Self {
            _root: root,
            width,
            source,
            program,
            execution,
            maximum,
        })
    }
    pub(super) fn eval(&mut self, context: &dyn Columns, row: Row<'_>) -> Result<Datum> {
        if self.width != row.len() {
            return Err(binding("scalar physical row width differs from preparation").into());
        }
        let store = RefCell::new(NativeValueStore::new(self.maximum)?);
        store.borrow_mut().begin_window()?;
        let source = &self.source;
        let policy = source.context_policy.as_deref();
        let mut hosts = source
            .hosts
            .clone()
            .map(|catalog| {
                let mut hosts =
                    NativeBatchHosts::new(catalog, &source.host_metadata, context, self.maximum)
                        .with_native_values(Some(&store))
                        .with_typed_cache_slots(policy.map_or(0, |p| p.typed_cache_slots))?
                        .with_constant_probes(&source.host_probes)
                        .with_diagnostic_parents(
                            policy.map_or(&[], |p| p.diagnostic_parents.as_ref()),
                        )?
                        .with_numeric_parents(policy.map_or(&[], |p| p.numeric_parents.as_ref()))?
                        .with_diagnostic_slots(
                            policy.map_or(&[], |p| p.diagnostic_slots.as_ref()),
                        )?;
                if let Some(input) = row.chunk() {
                    hosts = hosts.with_input(input);
                }
                Ok::<_, LocalError>(hosts)
            })
            .transpose()?;
        let mut input = PhysicalRow {
            source,
            values: &store,
            native: row,
        };
        let mut bindings = CheckedInputs {
            source: &mut input,
            hosts: hosts.as_mut().map(|h| h as &mut dyn LocalHostServices),
        };
        let mut sdk_context = EvalContext::default();
        // Authenticate this actual cursor, including a cursor into an empty
        // Chunk whose column branch is never demanded. The scalar frame's
        // coordinate extent does not assert that the Chunk contains a cell;
        // demanded leaves still use the original physical row primitive.
        let extent = row
            .idx()
            .checked_add(1)
            .ok_or_else(|| resource("scalar cursor coordinate overflow"))?;
        let result = self.program.eval_with_bindings(
            self.execution,
            &mut sdk_context,
            extent,
            &[row.idx()],
            &mut bindings,
        );
        drop(bindings);
        if let Some(error) = hosts.as_mut().and_then(NativeBatchHosts::take_native_error) {
            return Err(error.into());
        }
        let output = result?;
        let VectorValue::Bytes(values) = output else {
            return Err(binding("scalar output is not a native value token").into());
        };
        if values.len() != 1 {
            return Err(binding("scalar output count differs").into());
        }
        let token = values
            .get(0)
            .ok_or_else(|| binding("scalar output token is wire NULL"))?;
        let value = store
            .borrow()
            .get(
                token,
                NativeValueRole::Output,
                NativeValueOrigin {
                    node: 0,
                    row: InputRow {
                        occurrence: 0,
                        input_row: row.idx(),
                    },
                },
            )?
            .clone();
        Ok(value)
    }
}
