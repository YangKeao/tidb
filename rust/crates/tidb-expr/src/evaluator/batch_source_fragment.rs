// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine native source metadata for a caller's ONE composed SDK program.
//!
//! A fragment has no executable program and no eval method. Its original
//! Arc pins every native source identity. Only derived SDK host/input slots
//! and call ordinals are relocated; native metadata and value origins remain
//! unchanged. Native bindings borrow context/row for this invocation only.
use super::*;
use protobuf::Message as _;
use tidb_chunk::row::Row;
use tidb_query_expr::local::{
    HostArgFailure, HostArgReply, HostInvocation, HostStart, HostStep, HostTaskId,
};

/// A genuine Arc-owned Expression contributing metadata and native services.
/// Preparation is pure; the consumer decides when a preparation error is due.
pub struct NativeSourceFragment {
    source: SourcePlan,
    width: usize,
    // Metadata is dropped before the genuine address-bearing source owner.
    owner: Arc<Expression>,
}

/// The sole root program for a composed source. This owns the existing SDK
/// program and maps its actual preparation/invocation failures at that boundary.
/// Native child errors are moved by their demanding host via `take_failure`.
pub struct ComposedExpressionProgram {
    program: LocalBatchProgram,
    execution: ExecutionLimits,
}

impl ComposedExpressionProgram {
    /// Preserve an actual source compiler failure at the preparation boundary.
    /// This accepts the SDK's concrete contract/resource error; child invocation
    /// failures must still arrive through their sealed `HostArgFailure` receipt.
    pub fn source_preparation_failure(error: LocalError) -> EvalError {
        BatchFailure::from(error).into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare)
    }
    /// Move an actual child failure issued only by the canonical SDK driver.
    /// The sealed receipt prevents manufacturing an opaque native failure from
    /// an unrelated caller-created error or replacing its original cause.
    pub fn child_failure(failure: HostArgFailure) -> EvalError {
        BatchFailure::from(failure.into_error())
            .into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke)
    }
    /// Prepare one catalog from the source owner's exact checked signatures.
    pub fn catalog(
        entries: Vec<(SqlBatchHostOperation, HostSignature)>,
    ) -> std::result::Result<HostCatalog, EvalError> {
        HostCatalog::for_sql_batch(entries).map_err(|error| {
            BatchFailure::from(error)
                .into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare)
        })
    }

    /// Seal actual call identities and native input carriers before compilation.
    pub fn compile_with_hosts(
        graph: &LocalExpr,
        schema: &[tipb::FieldType],
        sites: Vec<OrdinaryCallSite>,
        catalog: &HostCatalog,
    ) -> std::result::Result<Self, EvalError> {
        let (limits, _) = public_scalar_limits();
        let facts =
            BatchFacts::sql_native_batch_with_hosts(graph, schema, sites, limits.tree, catalog)
                .map_err(|error| {
                    BatchFailure::from(error)
                        .into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare)
                })?;
        Self::compile(graph, schema, &facts)
    }

    /// Compile the complete owner-supplied graph and its checked native facts.
    /// Original public scalar limits apply; no retry or alternate evaluator.
    pub fn compile(
        graph: &LocalExpr,
        schema: &[tipb::FieldType],
        facts: &BatchFacts,
    ) -> std::result::Result<Self, EvalError> {
        let (limits, execution) = public_scalar_limits();
        let program = compile_batch(
            graph,
            schema,
            LocalCompileContext {
                limits: limits.tree,
            },
            facts,
        )
        .map_err(|error| {
            BatchFailure::from(error)
                .into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare)
        })?;
        Ok(Self { program, execution })
    }

    /// Run one real physical cursor through the canonical SDK frame driver.
    /// Services retain actual source/row ownership; no child evaluator is called.
    pub fn evaluate(
        &mut self,
        context: &mut EvalContext,
        physical: usize,
        services: &mut dyn LocalRuntimeServices,
    ) -> std::result::Result<VectorValue, EvalError> {
        self.program
            .eval_cursor_with_bindings(self.execution, context, physical, services)
            .map_err(|error| {
                BatchFailure::from(error)
                    .into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke)
            })
    }
}

/// Checked relocation into one particular immutable combined host catalog.
/// Private fields prevent replacing signatures with index-only dispatch.
pub struct NativeFragmentMount {
    owner: Arc<Expression>,
    catalog: HostCatalog,
    source_catalog: Option<HostCatalog>,
    host_base: usize,
    host_count: usize,
    input_base: usize,
    input_count: usize,
    width: usize,
}

/// Affine native values. Reset requires exclusive access, after bindings end.
/// Native handles keep their existing namespace/generation/role/row checks.
pub struct NativeFragmentState {
    owner: Arc<Expression>,
    width: usize,
    values: RefCell<NativeValueStore>,
    bound: std::cell::Cell<bool>,
}

/// Invocation-only services, never a nested expression evaluator. Suspended
/// native tasks retain their existing owned state and original local IDs.
/// A composite service must route these IDs through its own checked namespace.
pub struct NativeFragmentBindings<'a> {
    fragment: &'a NativeSourceFragment,
    mount: &'a NativeFragmentMount,
    state: &'a NativeFragmentState,
    row: Row<'a>,
    hosts: Option<NativeBatchHosts<'a>>,
}

impl NativeSourceFragment {
    /// Lower the actual owner with the same scalar source and default limits.
    /// No input row or context is read, and no replacement SQL type is attached.
    pub fn prepare(owner: Arc<Expression>, width: usize) -> std::result::Result<Self, EvalError> {
        let (limits, _) = public_scalar_limits();
        let source = lower_source_plan(
            owner.as_ref(),
            SourceInputs::PhysicalRow { width },
            true,
            Arc::as_ptr(&owner) as usize as u64,
            limits,
            Some(NativeView::RootValue),
            SourceConsumer::Scalar,
        )
        .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))?;
        Ok(Self {
            source,
            width,
            owner,
        })
    }

    /// Append these EXACT native signatures to a composition's catalog input.
    /// The returned start is later authenticated by mount, including operations.
    pub fn append_hosts(
        &self,
        entries: &mut Vec<(SqlBatchHostOperation, HostSignature)>,
    ) -> LocalResult<usize> {
        let start = entries.len();
        reserve(entries, self.source.host_metadata.len())?;
        if let Some(catalog) = &self.source.hosts {
            for index in 0..self.source.host_metadata.len() {
                let slot = catalog.slot(index)?;
                entries.push((
                    catalog.sql_batch_operation(slot)?,
                    catalog.signature(slot)?.clone(),
                ));
            }
        }
        Ok(start)
    }

    /// These are the original input carriers, not a declaration for the parent
    /// source. Native Bytes handles keep their private capability meaning.
    pub fn input_schema(&self) -> &[tipb::FieldType] {
        &self.source.schema
    }

    /// The original private native-value carrier for a parent host argument.
    /// This is not a SQL declaration for that parent's source expression.
    pub fn output_type(&self) -> &tipb::FieldType {
        self.source.expr.field_type()
    }

    /// Number of actual nodes in this already checked derived SDK graph.
    pub fn node_count(&self) -> usize {
        self.source.facts.node_count()
    }

    /// Original source preflight's conservative logical metadata observation.
    /// It imposes no allowance and does not measure allocator peaks or total heap.
    pub fn logical_metadata_bytes(&self) -> usize {
        self.source.logical_metadata_bytes
    }

    /// Authenticate a contiguous range of a combined catalog and input schema.
    pub fn mount(
        &self,
        catalog: &HostCatalog,
        host_base: usize,
        input_base: usize,
    ) -> LocalResult<NativeFragmentMount> {
        input_base
            .checked_add(self.source.schema.len())
            .ok_or_else(|| resource("fragment input offset overflow"))?;
        for index in 0..self.source.host_metadata.len() {
            let original = self
                .source
                .hosts
                .as_ref()
                .ok_or_else(|| binding("fragment catalog missing"))?;
            let old = original.slot(index)?;
            let new = catalog.slot(
                host_base
                    .checked_add(index)
                    .ok_or_else(|| resource("fragment host offset overflow"))?,
            )?;
            if !same_signature(original.signature(old)?, catalog.signature(new)?)
                || original.sql_batch_operation(old)? != catalog.sql_batch_operation(new)?
            {
                return Err(binding("fragment native host signature/operation differs"));
            }
        }
        Ok(NativeFragmentMount {
            owner: Arc::clone(&self.owner),
            catalog: catalog.clone(),
            source_catalog: self.source.hosts.clone(),
            host_base,
            host_count: self.source.host_metadata.len(),
            input_base,
            input_count: self.source.schema.len(),
            width: self.width,
        })
    }

    fn check_mount(&self, mount: &NativeFragmentMount) -> LocalResult<()> {
        if !Arc::ptr_eq(&self.owner, &mount.owner)
            || self.width != mount.width
            || self.source.hosts.as_ref().map(HostCatalog::key)
                != mount.source_catalog.as_ref().map(HostCatalog::key)
        {
            return Err(binding("fragment mount belongs to another preparation"));
        }
        Ok(())
    }

    /// Copy only the bounded derived SDK specification, iteratively. Native
    /// Expression owners are shared, never cloned or manufactured. All original
    /// source IDs survive; only preorder ordinals change in the combined graph.
    pub fn graph(
        &self,
        mount: &NativeFragmentMount,
        ordinal_base: usize,
    ) -> LocalResult<(LocalExpr, Vec<OrdinaryCallSite>)> {
        self.check_mount(mount)?;
        let count = self.source.facts.node_count();
        ordinal_base
            .checked_add(count)
            .ok_or_else(|| resource("fragment ordinal overflow"))?;
        let mut pending = Vec::new();
        let mut ordered = Vec::new();
        reserve(&mut pending, count)?;
        reserve(&mut ordered, count)?;
        pending.push(&self.source.expr);
        while let Some(node) = pending.pop() {
            if ordered.len() >= count {
                return Err(binding("fragment graph extent differs"));
            }
            ordered.push(node);
            if let LocalExpr::Call { args, .. } | LocalExpr::HostCall { args, .. } = node {
                pending.extend(args.iter().rev());
            }
        }
        if ordered.len() != count {
            return Err(binding("fragment graph lost source nodes"));
        }
        let mut built = Vec::new();
        reserve(&mut built, count)?;
        for node in ordered.into_iter().rev() {
            let mut children = |args: &[LocalExpr]| -> LocalResult<Box<[LocalExpr]>> {
                let mut output = Vec::new();
                reserve(&mut output, args.len())?;
                for _ in args {
                    output.push(
                        built
                            .pop()
                            .ok_or_else(|| binding("fragment child missing"))?,
                    );
                }
                Ok(output.into_boxed_slice())
            };
            let copy = match node {
                LocalExpr::Constant {
                    value,
                    field_type,
                    literal_kind,
                } => LocalExpr::Constant {
                    value: value.clone(),
                    field_type: field_type.clone(),
                    literal_kind: *literal_kind,
                },
                LocalExpr::InputSlot { slot, field_type } => LocalExpr::InputSlot {
                    slot: mount
                        .input_base
                        .checked_add(*slot)
                        .ok_or_else(|| resource("fragment input offset overflow"))?,
                    field_type: field_type.clone(),
                },
                LocalExpr::Call {
                    function,
                    args,
                    return_type,
                    metadata,
                } => LocalExpr::Call {
                    function: *function,
                    args: children(args)?,
                    return_type: return_type.clone(),
                    metadata: metadata.clone(),
                },
                LocalExpr::HostCall {
                    slot,
                    args,
                    return_type,
                } => LocalExpr::HostCall {
                    slot: mount.catalog.slot(
                        mount
                            .host_base
                            .checked_add(slot.index())
                            .ok_or_else(|| resource("fragment host offset overflow"))?,
                    )?,
                    args: children(args)?,
                    return_type: return_type.clone(),
                },
            };
            built.push(copy);
        }
        let mut sites = Vec::new();
        reserve(&mut sites, self.source.facts.call_sites().len())?;
        for site in self.source.facts.call_sites() {
            sites.push(OrdinaryCallSite::sql_native_batch(
                ordinal_base
                    .checked_add(site.ordinal())
                    .ok_or_else(|| resource("fragment ordinal overflow"))?,
                site.source(),
            ));
        }
        let graph = built
            .pop()
            .ok_or_else(|| binding("fragment graph is empty"))?;
        if !built.is_empty() {
            return Err(binding("fragment graph has extra roots"));
        }
        Ok((graph, sites))
    }

    /// Create a fresh native value namespace with the existing scalar allowance.
    pub fn new_state(&self) -> LocalResult<NativeFragmentState> {
        let mut values = NativeValueStore::new()?;
        values.begin_window()?;
        Ok(NativeFragmentState {
            owner: Arc::clone(&self.owner),
            width: self.width,
            values: RefCell::new(values),
            bound: std::cell::Cell::new(false),
        })
    }

    /// Borrow this invocation's actual physical Row and original native context.
    pub fn bind<'a>(
        &'a self,
        mount: &'a NativeFragmentMount,
        state: &'a NativeFragmentState,
        context: &'a dyn Columns,
        row: Row<'a>,
    ) -> LocalResult<NativeFragmentBindings<'a>> {
        self.check_mount(mount)?;
        if self.width != row.len()
            || state.width != self.width
            || !Arc::ptr_eq(&state.owner, &self.owner)
        {
            return Err(binding(
                "fragment physical row/state differs from preparation",
            ));
        }
        if state.bound.get() {
            return Err(binding("native fragment state already has live bindings"));
        }
        let source = &self.source;
        let policy = source.context_policy.as_deref();
        let hosts = source
            .hosts
            .clone()
            .map(|catalog| {
                let mut hosts = NativeBatchHosts::new(catalog, &source.host_metadata, context)
                    .with_native_values(Some(&state.values))
                    .with_typed_cache_slots(policy.map_or(0, |p| p.typed_cache_slots))?
                    .with_constant_probes(&source.host_probes)
                    .with_diagnostic_parents(policy.map_or(&[], |p| p.diagnostic_parents.as_ref()))
                    .with_numeric_parents(policy.map_or(&[], |p| p.numeric_parents.as_ref()))
                    .with_diagnostic_slots(policy.map_or(&[], |p| p.diagnostic_slots.as_ref()));
                if let Some(input) = row.chunk() {
                    hosts = hosts.with_input(input);
                }
                Ok::<_, LocalError>(hosts)
            })
            .transpose()?;
        state.bound.set(true);
        Ok(NativeFragmentBindings {
            fragment: self,
            mount,
            state,
            row,
            hosts,
        })
    }
}

impl NativeFragmentState {
    /// Exclusive reset after the previous SDK invocation and its consumers end.
    pub fn reset(&mut self) -> LocalResult<()> {
        self.values.get_mut().begin_window()
    }

    /// Drop a completed child's temporary value buffers before its parent
    /// demands the next child. Every binding must have ended first. This resets
    /// only native value ownership, never the SDK's global execution budget.
    pub fn release_values(&self) -> LocalResult<()> {
        if self.bound.get() {
            return Err(binding(
                "cannot release values while native bindings are live",
            ));
        }
        self.values.borrow_mut().release_window();
        Ok(())
    }
}

impl Drop for NativeFragmentBindings<'_> {
    fn drop(&mut self) {
        // Drop owned task/lease state before making the affine store reusable.
        self.hosts = None;
        self.state.bound.set(false);
    }
}

impl NativeFragmentMount {
    /// Resolve dispatch only after authenticating the combined catalog key.
    pub fn owns_host(&self, slot: tidb_query_expr::local::HostSlot) -> LocalResult<bool> {
        self.catalog.signature(slot)?;
        Ok(slot
            .index()
            .checked_sub(self.host_base)
            .is_some_and(|i| i < self.host_count))
    }

    /// Input dispatch is subsequently checked against the original schema, row
    /// coordinate and native value owner by `read_input`.
    pub fn owns_input(&self, slot: usize) -> bool {
        slot.checked_sub(self.input_base)
            .is_some_and(|i| i < self.input_count)
    }
}

impl NativeFragmentBindings<'_> {
    /// Read one relocated input through the original checked native binding.
    pub fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        let local = slot
            .checked_sub(self.mount.input_base)
            .ok_or_else(|| binding("fragment input belongs to another mount"))?;
        let mut native = scalar_source::PhysicalRow {
            source: &self.fragment.source,
            values: &self.state.values,
            native: self.row,
        };
        CheckedInputs {
            source: &mut native,
            hosts: None,
        }
        .read_input(ctx, local, row, expected)
    }

    /// Move an actual native callback failure immediately when its SDK child
    /// fails. This is the existing native side channel, with no cause cloning,
    /// classification by text, SQL folding or replacement error construction.
    pub fn take_native_error(&mut self) -> Option<EvalError> {
        self.hosts
            .as_mut()
            .and_then(NativeBatchHosts::take_native_error)
    }

    /// Move a failure from this actual demanded native fragment. Native causes
    /// take precedence over their SDK transport signal; other SDK failures keep
    /// their original class and cause at the invocation boundary.
    pub fn take_failure(&mut self, failure: HostArgFailure) -> EvalError {
        self.take_native_error()
            .unwrap_or_else(|| ComposedExpressionProgram::child_failure(failure))
    }

    /// Materialize only this fragment's root token at the original coordinate.
    pub fn value(&self, token: &[u8], row: InputRow) -> LocalResult<Datum> {
        if row.occurrence != 0 || row.input_row != self.row.idx() {
            return Err(binding("fragment output coordinate differs"));
        }
        self.state
            .values
            .borrow()
            .get(
                token,
                self.fragment.source.nodes[0]
                    .view
                    .value_role()
                    .ok_or_else(|| binding("fragment root role missing"))?,
                NativeValueOrigin { node: 0, row },
            )
            .cloned()
    }
}

impl LocalHostServices for NativeFragmentBindings<'_> {
    fn catalog_key(&self) -> &tidb_query_expr::local::HostCatalogKey {
        self.mount.catalog.key()
    }
    fn start(&mut self, ctx: &mut EvalContext, call: HostInvocation<'_>) -> LocalResult<HostStart> {
        let global = self.mount.catalog.signature(call.slot)?;
        if global.arg_types.as_ref() != call.arg_types
            || &global.return_type != call.return_type
            || call.row.occurrence != 0
            || call.row.input_row != self.row.idx()
        {
            return Err(binding("fragment host invocation differs"));
        }
        let local = call
            .slot
            .index()
            .checked_sub(self.mount.host_base)
            .ok_or_else(|| binding("fragment host belongs to another mount"))?;
        let original = self
            .mount
            .source_catalog
            .as_ref()
            .ok_or_else(|| binding("fragment has no native hosts"))?;
        let slot = original.slot(local)?;
        if !same_signature(original.signature(slot)?, global) {
            return Err(binding("fragment native signature differs"));
        }
        self.hosts
            .as_mut()
            .ok_or_else(|| binding("fragment native services missing"))?
            .start(ctx, HostInvocation { slot, ..call })
    }
    fn resume(
        &mut self,
        ctx: &mut EvalContext,
        task: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        self.hosts
            .as_mut()
            .ok_or_else(|| binding("fragment native services missing"))?
            .resume(ctx, task, reply)
    }
    fn cancel(&mut self, task: &HostTaskId) {
        if let Some(hosts) = self.hosts.as_mut() {
            hosts.cancel(task);
        }
    }
}

fn same_signature(left: &HostSignature, right: &HostSignature) -> bool {
    left.arg_types == right.arg_types && left.return_type == right.return_type
}

#[cfg(test)]
#[path = "batch_source_fragment_tests.rs"]
mod tests;
