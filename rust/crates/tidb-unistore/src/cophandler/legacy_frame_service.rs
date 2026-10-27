// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Owned host continuations. Only the SDK traverses child expressions.
use super::*;
use std::cell::OnceCell;
use tidb_expr::{
    NativeFragmentBindings, NativeFragmentState, ScopedReadyValueColumns, SelectedReadyValueScope,
};
use tidb_query_datatype::codec::data_type::ChunkedVec as _;
use tidb_query_expr::local::ArgMode;

fn contract(message: &'static str) -> LocalError {
    LocalError::HostContract(message.into())
}
fn resource() -> LocalError {
    LocalError::ResourceLimit("legacy source task storage allowance exceeded".into())
}
fn value_error(error: ValueError) -> LocalError {
    match error {
        ValueError::Contract => {
            LocalError::BindingContract("legacy source value capability differs".into())
        }
        ValueError::Resource => resource(),
    }
}
struct Invocation<'a> {
    row: OnceCell<tidb_chunk::mutrow::MutRow>,
    columns: Vec<OnceCell<ScopedReadyValueColumns<'a, 'a>>>,
    states: Vec<OnceCell<NativeFragmentState>>,
}
struct SourceTask {
    node: usize,
    next: usize,
    tokens: Vec<Vec<u8>>,
    raw_scope: Option<usize>,
    stage: Option<Box<Stage>>,
}
enum Stage {
    Date {
        machine: tidb_expr::LegacyDateArithmeticMachine,
    },
    In {
        machine: tidb_expr::LegacyInMachine,
        bytes: bool,
    },
    Substring {
        need_length: bool,
    },
    EpochFormat {
        time: Option<tidb_datatype::Time>,
    },
    JsonMember {
        target: Option<tidb_datatype::BinaryJSON>,
    },
    JsonModify {
        append: bool,
        document: Option<tidb_datatype::BinaryJSON>,
        paths: Vec<tidb_datatype::JSONPathExpression>,
        values: Vec<tidb_datatype::BinaryJSON>,
    },
}
enum Task {
    Source(SourceTask),
    Native {
        fragment: usize,
        original: HostTaskId,
    },
}
struct Slot {
    generation: u64,
    task: Option<Task>,
}
struct Services<'a, 'source> {
    evaluator: &'a LegacyEvaluator<'a>,
    nodes: &'a [Node<'source>],
    natives: &'a [Native<'source>],
    catalog: &'a HostCatalog,
    schema: &'a [sdk_tipb::FieldType],
    invocation: &'a Invocation<'a>,
    bindings: Vec<Option<NativeFragmentBindings<'a>>>,
    native_scopes: Vec<Option<SelectedReadyValueScope<'a>>>,
    raw_scopes: Vec<Option<SelectedReadyValueScope<'a>>>,
    slots: Vec<Slot>,
    generation: u64,
    values: Values,
}

impl<'a, 'source: 'a> Services<'a, 'source> {
    fn origin(&self, node: usize) -> Origin {
        Origin {
            node,
            view: self.nodes[node].view,
            occurrence: 0,
            physical: 0,
        }
    }
    fn check(&self) -> LocalResult<()> {
        let mut bytes =
            self.slots
                .capacity()
                .saturating_mul(std::mem::size_of::<Slot>())
                .saturating_add(
                    self.raw_scopes
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Option<SelectedReadyValueScope<'a>>>()),
                )
                .saturating_add(
                    self.native_scopes
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Option<SelectedReadyValueScope<'a>>>()),
                )
                .saturating_add(
                    self.bindings
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Option<NativeFragmentBindings<'a>>>()),
                )
                .saturating_add(self.invocation.columns.capacity().saturating_mul(
                    std::mem::size_of::<OnceCell<ScopedReadyValueColumns<'a, 'a>>>(),
                ))
                .saturating_add(
                    self.invocation
                        .states
                        .capacity()
                        .saturating_mul(std::mem::size_of::<OnceCell<NativeFragmentState>>()),
                );
        for slot in &self.slots {
            if let Some(Task::Source(task)) = &slot.task {
                bytes = bytes.saturating_add(
                    task.tokens
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Vec<u8>>()),
                );
                for token in &task.tokens {
                    bytes = bytes.saturating_add(token.capacity());
                }
                if let Some(stage) = task.stage.as_deref() {
                    bytes = bytes.saturating_add(std::mem::size_of::<Stage>());
                    match stage {
                        Stage::Date { machine, .. } => {
                            bytes =
                                bytes.saturating_add(machine.retained_bytes().ok_or_else(resource)?)
                        }
                        Stage::In { machine, .. } => {
                            bytes = bytes.saturating_add(machine.retained_bytes())
                        }
                        Stage::Substring { .. } => {}
                        Stage::EpochFormat { .. } => {}
                        Stage::JsonMember { target } => {
                            bytes = bytes.saturating_add(
                                target
                                    .as_ref()
                                    .map_or(0, |v| v.value().len().saturating_add(1)),
                            );
                        }
                        Stage::JsonModify {
                            document,
                            paths,
                            values,
                            ..
                        } => {
                            bytes = bytes
                                .saturating_add(
                                    document
                                        .as_ref()
                                        .map_or(0, |v| v.value().len().saturating_add(1)),
                                )
                                .saturating_add(paths.capacity().saturating_mul(
                                    std::mem::size_of::<tidb_datatype::JSONPathExpression>(),
                                ))
                                .saturating_add(values.capacity().saturating_mul(
                                    std::mem::size_of::<tidb_datatype::BinaryJSON>(),
                                ));
                            for path in paths {
                                bytes = bytes.saturating_add(path.retained_bytes());
                            }
                            for value in values {
                                bytes = bytes.saturating_add(value.value().len().saturating_add(1));
                            }
                        }
                    }
                }
            }
        }
        self.values.check_external(bytes).map_err(value_error)
    }
    fn allocate(&mut self, task: Task) -> LocalResult<HostTaskId> {
        self.generation = match self.generation.checked_add(1) {
            Some(next) => next,
            None => {
                self.clean_owned(&task);
                return Err(resource());
            }
        };
        let slot = match self.slots.iter().position(|s| s.task.is_none()) {
            Some(slot) => slot,
            None => {
                if self.slots.try_reserve_exact(1).is_err() {
                    self.clean_owned(&task);
                    return Err(resource());
                }
                self.slots.push(Slot {
                    generation: 0,
                    task: None,
                });
                self.slots.len() - 1
            }
        };
        self.slots[slot] = Slot {
            generation: self.generation,
            task: Some(task),
        };
        let id = HostTaskId {
            slot,
            generation: self.generation,
        };
        if let Err(error) = self.check() {
            self.cancel(&id);
            return Err(error);
        }
        Ok(id)
    }
    fn take_task(&mut self, id: &HostTaskId) -> LocalResult<Task> {
        self.slots
            .get_mut(id.slot)
            .filter(|s| s.generation == id.generation)
            .and_then(|s| s.task.take())
            .ok_or_else(|| contract("stale legacy source task"))
    }
    fn enter_int_scope(&mut self, node: usize) -> LocalResult<Option<usize>> {
        if self.nodes[node].view != View::Int || self.raw_scopes.iter().rev().any(Option::is_some) {
            return Ok(None);
        }
        if self.evaluator.raw_columns.ready_value_cache().is_some() {
            return Ok(None);
        }
        let scope = self
            .evaluator
            .ready_values
            .select_columns_scope(self.evaluator.raw_columns);
        self.raw_scopes
            .try_reserve_exact(1)
            .map_err(|_| resource())?;
        let index = self.raw_scopes.len();
        self.raw_scopes.push(Some(scope));
        if let Err(error) = self.check() {
            self.raw_scopes[index] = None;
            return Err(error);
        }
        Ok(Some(index))
    }
    fn clean_source(&mut self, task: &SourceTask) {
        for (token, child) in task.tokens.iter().zip(&self.nodes[task.node].children) {
            let origin = self.origin(*child);
            let _ = self.values.take(token, origin);
        }
        if let Recipe::Shared(index) = self.nodes[task.node].recipe {
            if self.native_scopes[index].is_some() || self.bindings[index].is_some() {
                self.bindings[index] = None;
                if let Some(state) = self.invocation.states[index].get() {
                    let _ = state.release_values();
                }
                self.native_scopes[index] = None;
            }
        }
        if let Some(scope) = task.raw_scope {
            self.raw_scopes[scope] = None;
        }
    }
    fn clean_owned(&mut self, task: &Task) {
        match task {
            Task::Source(task) => self.clean_source(task),
            Task::Native { fragment, original } => {
                if let Some(binding) = self.bindings[*fragment].as_mut() {
                    binding.cancel(original);
                }
            }
        }
    }
    fn emit(&mut self, node: usize, outcome: LegacyResult<Value>) -> LocalResult<VectorValue> {
        let token = self
            .values
            .insert(self.origin(node), outcome)
            .map_err(value_error)?;
        if let Err(error) = self.check() {
            let _ = self.values.take(&token, self.origin(node));
            return Err(error);
        }
        Ok(VectorValue::from_scalar(
            &ScalarValue::Bytes(Some(token)),
            1,
        ))
    }
    fn finish(&mut self, task: &SourceTask) -> LocalResult<HostStep> {
        let columns = self
            .raw_scopes
            .iter()
            .rev()
            .find_map(|s| s.as_ref())
            .map(|scope| scope.bind_columns(self.evaluator.raw_columns));
        let columns: &dyn tidb_expr::Columns =
            columns.as_ref().map_or(self.evaluator.raw_columns, |c| c);
        let outcome = kernel(
            &self.nodes[task.node],
            &task.tokens,
            self.nodes,
            &mut self.values,
            self.evaluator,
            columns,
            match task.stage.as_deref() {
                Some(Stage::Substring { need_length }) => Some(*need_length),
                _ => None,
            },
        );
        self.clean_source(&task);
        self.emit(task.node, outcome?).map(HostStep::Ready)
    }
    fn prepare_native(&mut self, node: usize, native: usize) -> LocalResult<()> {
        // This is the actual old order: physical row, then Shared scope, then
        // native preparation/invocation. Pure preparation errors stay late.
        let invocation = self.invocation;
        let _row = invocation
            .row
            .get_or_init(|| tidb_chunk::mutrow::MutRow::from_datums(self.evaluator.row));
        let context: &dyn tidb_expr::Columns = self.natives[native].original.context.as_ref();
        #[cfg(test)]
        let context = self.evaluator.shared_override.unwrap_or(context);
        let scope = self.evaluator.ready_values.select_columns_scope(context);
        invocation.columns[native]
            .set(scope.bind_columns(context))
            .map_err(|_| contract("legacy native occurrence replayed"))?;
        self.native_scopes[native] = Some(scope);
        if let Ok(fragment) = &self.natives[native].fragment {
            invocation.states[native]
                .set(fragment.new_state()?)
                .map_err(|_| contract("legacy native state replayed"))?;
            let binding = fragment.bind(
                self.natives[native]
                    .mount
                    .as_ref()
                    .ok_or_else(|| contract("legacy native mount missing"))?,
                invocation.states[native].get().unwrap(),
                invocation.columns[native].get().unwrap(),
                invocation.row.get().unwrap().to_row(),
            )?;
            self.bindings[native] = Some(binding);
        }
        let _ = node;
        self.check()
    }
    fn start_source(&mut self, node: usize) -> LocalResult<HostStart> {
        let raw_scope = self.enter_int_scope(node)?;
        let mut task = SourceTask {
            node,
            next: 0,
            tokens: vec![],
            raw_scope,
            stage: None,
        };
        match self.nodes[node].recipe {
            Recipe::JsonModify { append } => {
                task.stage = Some(Box::new(Stage::JsonModify {
                    append,
                    document: None,
                    paths: vec![],
                    values: vec![],
                }));
                if self.nodes[node].children.is_empty() {
                    let result =
                        tidb_expr::eval_legacy_json_output_none_in(self.evaluator.raw_columns)
                            .map(Value::Json)
                            .map_err(Into::into);
                    self.clean_source(&task);
                    return self.emit(node, result).map(HostStart::Ready);
                }
            }
            Recipe::EpochFormat => {
                if self.nodes[node].children.is_empty() {
                    panic!("wire shape");
                }
                task.stage = Some(Box::new(Stage::EpochFormat { time: None }));
            }
            Recipe::JsonMember => {
                task.stage = Some(Box::new(Stage::JsonMember { target: None }));
                if self.nodes[node].children.is_empty() {
                    let result = tidb_expr::eval_legacy_json_member_of_in(
                        tidb_expr::LegacyBinaryArgs::Missing,
                        self.evaluator.raw_columns,
                    )
                    .map(Value::Int)
                    .map_err(Into::into);
                    self.clean_source(&task);
                    return self.emit(node, result).map(HostStart::Ready);
                }
            }
            _ => {}
        }
        if let Recipe::In { collation } = self.nodes[node].recipe {
            let mut machine = match collation {
                Some(collation) => match tidb_expr::LegacyInMachine::bytes(
                    self.nodes[node].original_arity,
                    collation,
                ) {
                    Ok(machine) => machine,
                    Err(error) => {
                        self.clean_source(&task);
                        return self.emit(node, Err(error.into())).map(HostStart::Ready);
                    }
                },
                None => tidb_expr::LegacyInMachine::integer(self.nodes[node].original_arity),
            };
            let step = machine.begin();
            task.stage = Some(Box::new(Stage::In {
                machine,
                bytes: collation.is_some(),
            }));
            return match self.in_step(&mut task, step) {
                Ok(HostStep::NeedArg(request)) => self
                    .allocate(Task::Source(task))
                    .map(|task| HostStart::Pending { task, request }),
                Ok(HostStep::Ready(value)) => Ok(HostStart::Ready(value)),
                Ok(_) => {
                    self.clean_source(&task);
                    Err(contract("legacy IN source requested a window"))
                }
                Err(error) => {
                    self.clean_source(&task);
                    Err(error)
                }
            };
        }
        if let Recipe::Date { metadata, output } = self.nodes[node].recipe {
            let mut machine = match tidb_expr::LegacyDateArithmeticMachine::new(output, metadata) {
                Ok(machine) => machine,
                Err(error) => {
                    self.clean_source(&task);
                    return self.emit(node, Err(error.into())).map(HostStart::Ready);
                }
            };
            let step = machine.begin(self.evaluator.time_zone);
            task.stage = Some(Box::new(Stage::Date { machine }));
            let result = self.date_step(&mut task, step);
            return match result {
                Ok(HostStep::NeedArg(request)) => self
                    .allocate(Task::Source(task))
                    .map(|task| HostStart::Pending { task, request }),
                Ok(HostStep::Ready(value)) => Ok(HostStart::Ready(value)),
                Ok(_) => {
                    self.clean_source(&task);
                    Err(contract("date source requested a window"))
                }
                Err(error) => {
                    self.clean_source(&task);
                    Err(error)
                }
            };
        }
        if let Recipe::Shared(native) = self.nodes[node].recipe {
            if let Err(error) = self.prepare_native(node, native) {
                self.clean_source(&task);
                return Err(error);
            }
            if let Err(error) = &self.natives[native].fragment {
                let outcome = shared_value(
                    self.natives[native].original,
                    self.nodes[node].view,
                    Err(LegacyEvalError::from(error.clone())),
                );
                self.clean_source(&task);
                return self.emit(node, outcome).map(HostStart::Ready);
            }
            let id = self.allocate(Task::Source(task))?;
            return Ok(HostStart::Pending {
                task: id,
                request: HostArgRequest {
                    index: 0,
                    mode: ArgMode::FreshResult,
                },
            });
        }
        if self.nodes[node].children.is_empty() {
            return self.finish(&task).and_then(|step| match step {
                HostStep::Ready(value) => Ok(HostStart::Ready(value)),
                _ => Err(contract("leaf requested a child")),
            });
        }
        let id = self.allocate(Task::Source(task))?;
        Ok(HostStart::Pending {
            task: id,
            request: HostArgRequest {
                index: 0,
                mode: ArgMode::FreshResult,
            },
        })
    }
    fn consume_reply(
        &mut self,
        task: &mut SourceTask,
        index: usize,
        values: &VectorValue,
    ) -> LocalResult<HostStep> {
        if index != task.next {
            self.clean_source(&task);
            return Err(contract("legacy source child index differs"));
        }
        let VectorValue::Bytes(values) = values else {
            self.clean_source(&task);
            return Err(contract("legacy source reply is not a capability"));
        };
        if values.len() != 1 {
            self.clean_source(&task);
            return Err(contract("legacy source reply width differs"));
        }
        let token = values
            .get(0)
            .ok_or_else(|| contract("legacy source reply is wire NULL"))?;
        if matches!(
            task.stage.as_deref(),
            Some(Stage::EpochFormat { .. } | Stage::JsonMember { .. } | Stage::JsonModify { .. })
        ) {
            return self.consume_staged_value(task, token);
        }
        if matches!(task.stage.as_deref(), Some(Stage::Date { .. })) {
            let child = self.nodes[task.node].children[index];
            let outcome = self
                .values
                .take(token, self.origin(child))
                .map_err(value_error)?;
            let value = match outcome {
                Ok(value) => date_value(value),
                Err(error) => Err(error),
            };
            let value = match value {
                Ok(value) => value,
                Err(error) => {
                    task.stage = None;
                    self.clean_source(task);
                    return self.emit(task.node, Err(error)).map(HostStep::Ready);
                }
            };
            let Some(Stage::Date { machine, .. }) = task.stage.as_deref_mut() else {
                unreachable!()
            };
            let step = machine.resume_source(value, self.evaluator.time_zone);
            return self.date_step(task, step);
        }
        if matches!(task.stage.as_deref(), Some(Stage::In { .. })) {
            let child = self.nodes[task.node].children[index];
            let outcome = self
                .values
                .take(token, self.origin(child))
                .map_err(value_error)?;
            let value = match outcome {
                Ok(value) => value,
                Err(error) => {
                    task.stage = None;
                    self.clean_source(task);
                    return self.emit(task.node, Err(error)).map(HostStep::Ready);
                }
            };
            let Some(Stage::In { machine, bytes }) = task.stage.as_deref_mut() else {
                unreachable!()
            };
            let step = match (value, *bytes) {
                (Value::Int(value), false) => machine.resume_integer(value),
                (Value::Bytes(value), true) => machine.resume_bytes(value),
                _ => {
                    task.stage = None;
                    self.clean_source(task);
                    return Err(contract("legacy IN source channel differs"));
                }
            };
            return self.in_step(task, step);
        }
        if let Recipe::Shared(native) = self.nodes[task.node].recipe {
            let value = self.bindings[native]
                .as_ref()
                .ok_or_else(|| contract("native bindings missing"))?
                .value(
                    token,
                    InputRow {
                        occurrence: 0,
                        input_row: 0,
                    },
                );
            match value {
                Ok(value) => {
                    let outcome = shared_value(
                        self.natives[native].original,
                        self.nodes[task.node].view,
                        Ok(value),
                    );
                    self.clean_source(&task);
                    return self.emit(task.node, outcome).map(HostStep::Ready);
                }
                Err(error) => {
                    self.clean_source(&task);
                    return Err(error);
                }
            }
        }
        let child = self.nodes[task.node].children[task.next];
        if let Err(error) = self.values.peek(token, self.origin(child)) {
            self.clean_source(&task);
            return Err(value_error(error));
        }
        if task.tokens.try_reserve_exact(1).is_err() {
            let _ = self.values.take(token, self.origin(child));
            return Err(resource());
        }
        task.tokens.push(token.to_vec());
        task.next += 1;
        let result = self
            .values
            .peek(task.tokens.last().unwrap(), self.origin(child))
            .map_err(value_error)?;
        let eager = matches!(
            self.nodes[task.node].recipe,
            Recipe::IntBinary {
                eager_error: true,
                ..
            }
        );
        let null_stop = match self.nodes[task.node].recipe {
            Recipe::Typed {
                nulls: NullDemand::First,
                ..
            } => task.next == 1,
            Recipe::Typed {
                nulls: NullDemand::Any,
                ..
            } => true,
            _ => false,
        };
        let mut stop =
            (!eager && result.is_err()) || (null_stop && result.as_ref().is_ok_and(Value::is_null));
        if !stop && task.next == 2 {
            if let Recipe::Typed { signature, .. } = self.nodes[task.node].recipe {
                match signature {
                    SimpleSig::Conv => {
                        stop =
                            !matches!(result, Ok(Value::Int(Some(v))) if i64::try_from(*v).is_ok());
                    }
                    SimpleSig::Substring3Args | SimpleSig::Substring3ArgsUtf8 => {
                        let first = self.nodes[task.node].children[0];
                        let text = self
                            .values
                            .peek(&task.tokens[0], self.origin(first))
                            .map_err(value_error)?;
                        let need_length = match (text, result) {
                            (Ok(Value::Bytes(Some(text))), Ok(Value::Int(Some(pos)))) => {
                                tidb_expr::raw_substring_needs_len(
                                    text,
                                    *pos,
                                    signature == SimpleSig::Substring3ArgsUtf8,
                                )
                            }
                            _ => false,
                        };
                        task.stage = Some(Box::new(Stage::Substring { need_length }));
                        stop = !need_length;
                    }
                    _ => {}
                }
            }
        }
        if stop || task.next >= self.nodes[task.node].children.len() {
            return self.finish(task);
        }
        let index = task.next;
        Ok(HostStep::NeedArg(HostArgRequest {
            index,
            mode: ArgMode::FreshResult,
        }))
    }

    fn consume_staged_value(
        &mut self,
        task: &mut SourceTask,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let child = self.nodes[task.node].children[task.next];
        let outcome = self
            .values
            .take(token, self.origin(child))
            .map_err(value_error)?;
        let columns = self
            .raw_scopes
            .iter()
            .rev()
            .find_map(|s| s.as_ref())
            .map(|scope| scope.bind_columns(self.evaluator.raw_columns));
        let columns: &dyn tidb_expr::Columns =
            columns.as_ref().map_or(self.evaluator.raw_columns, |c| c);
        let result: LegacyResult<Option<Value>> = (|| {
            let value = outcome?;
            match task.stage.as_deref_mut() {
                Some(Stage::JsonModify {
                    append,
                    document,
                    paths,
                    values,
                }) => {
                    let none = || {
                        tidb_expr::eval_legacy_json_output_none_in(columns)
                            .map(Value::Json)
                            .map(Some)
                            .map_err(Into::into)
                    };
                    if task.next == 0 {
                        let Value::Json(value) = value else {
                            return Err(LegacyEvalError::InvalidResult(
                                "JSON document channel differs",
                            ));
                        };
                        if value.is_none() {
                            return none();
                        }
                        *document = value;
                    } else if task.next % 2 == 1 {
                        let Value::Bytes(value) = value else {
                            return Err(LegacyEvalError::InvalidResult(
                                "JSON path channel differs",
                            ));
                        };
                        let Some(value) = value else {
                            return none();
                        };
                        let Ok(path) =
                            tidb_datatype::parse_json_path_expr(&String::from_utf8_lossy(&value))
                        else {
                            return none();
                        };
                        paths.push(path);
                        return Ok(None);
                    } else {
                        let Value::Json(value) = value else {
                            return Err(LegacyEvalError::InvalidResult(
                                "JSON value channel differs",
                            ));
                        };
                        let Some(value) = value else {
                            return none();
                        };
                        if *append {
                            let path = paths.pop().expect("prepared JSON path");
                            *document = tidb_expr::eval_legacy_json_array_append_step_in(
                                document.as_ref().unwrap(),
                                Some((&path, &value)),
                                columns,
                            )?;
                            if document.is_none() {
                                return Ok(Some(Value::Json(None)));
                            }
                        } else {
                            values.push(value);
                        }
                    }
                    if task.next + 1 < self.nodes[task.node].children.len() {
                        return Ok(None);
                    }
                    let document = document.take().unwrap();
                    Ok(Some(Value::Json(if *append {
                        if task.next == 0 {
                            tidb_expr::eval_legacy_json_array_append_step_in(
                                &document, None, columns,
                            )?
                        } else {
                            Some(document)
                        }
                    } else {
                        tidb_expr::eval_legacy_json_replace_in(&document, paths, values, columns)?
                    })))
                }
                Some(Stage::EpochFormat { time }) => {
                    let layout = if task.next == 0 {
                        let Value::Decimal(value) = value else {
                            return Err(LegacyEvalError::InvalidResult(
                                "epoch source channel differs",
                            ));
                        };
                        *time = tidb_expr::eval_from_unixtime_legacy_scoped_in(
                            value,
                            self.evaluator.time_zone,
                            columns,
                            |time, _| time,
                        )?;
                        if time.is_none() {
                            return Ok(Some(Value::Bytes(None)));
                        }
                        if self.nodes[task.node].children.len() > 1 {
                            return Ok(None);
                        }
                        None
                    } else {
                        let Value::Bytes(value) = value else {
                            return Err(LegacyEvalError::InvalidResult(
                                "epoch format channel differs",
                            ));
                        };
                        value.map(|v| String::from_utf8_lossy(&v).into_owned())
                    };
                    Ok(Some(Value::Bytes(tidb_expr::eval_legacy_date_format_in(
                        Some((time.as_ref().unwrap().core_time(), layout.as_deref())),
                        columns,
                    )?)))
                }
                Some(Stage::JsonMember { target }) => {
                    use tidb_expr::LegacyBinaryArgs as Args;
                    if task.next == 0 {
                        let Value::Datum(datum) = value else {
                            return Err(LegacyEvalError::InvalidResult(
                                "member target channel differs",
                            ));
                        };
                        if datum.is_null() {
                            return Ok(Some(Value::Int(tidb_expr::eval_legacy_json_member_of_in(
                                Args::NullWitness(None),
                                columns,
                            )?)));
                        }
                        let value = datum_to_json_value(&datum).ok_or_else(|| {
                            LegacyEvalError::Sql(
                                "this MEMBER OF target kind is a later course".into(),
                            )
                        })?;
                        *target = Some(
                            tidb_datatype::BinaryJSON::from_typed_value(&value)
                                .map_err(|e| LegacyEvalError::Sql(e.to_string()))?,
                        );
                        if self.nodes[task.node].children.len() > 1 {
                            return Ok(None);
                        }
                        return Ok(Some(Value::Int(tidb_expr::eval_legacy_json_member_of_in(
                            Args::Missing,
                            columns,
                        )?)));
                    }
                    let Value::Json(document) = value else {
                        return Err(LegacyEvalError::InvalidResult(
                            "member document channel differs",
                        ));
                    };
                    let args = match document {
                        Some(document) => {
                            if document.type_code() == tidb_datatype::JSON_TYPE_CODE_ARRAY {
                                document.element_count().map_err(|_| {
                                    LegacyEvalError::Sql("invalid json array".into())
                                })?;
                            }
                            Args::Values(target.take().unwrap(), document)
                        }
                        None => Args::NullWitness(None),
                    };
                    Ok(Some(Value::Int(tidb_expr::eval_legacy_json_member_of_in(
                        args, columns,
                    )?)))
                }
                _ => Err(LegacyEvalError::InvalidResult(
                    "legacy staged source state differs",
                )),
            }
        })();
        match result {
            Ok(None) => {
                task.next += 1;
                self.check()?;
                Ok(HostStep::NeedArg(HostArgRequest {
                    index: task.next,
                    mode: ArgMode::FreshResult,
                }))
            }
            result => {
                let result = result.map(|v| v.expect("completed staged value"));
                task.stage = None;
                self.clean_source(task);
                self.emit(task.node, result).map(HostStep::Ready)
            }
        }
    }

    fn consume_failure(
        &mut self,
        task: &mut SourceTask,
        failure: HostArgFailure,
    ) -> LocalResult<HostStep> {
        if failure.index() != task.next {
            return Err(contract("legacy failed child index differs"));
        }
        let error = if let Recipe::Shared(native) = self.nodes[task.node].recipe {
            match self.bindings[native].as_mut() {
                Some(binding) => binding.take_failure(failure),
                None => ComposedExpressionProgram::child_failure(failure),
            }
        } else {
            ComposedExpressionProgram::child_failure(failure)
        };
        if task.stage.is_some() {
            task.stage = None;
            self.clean_source(task);
            return self.emit(task.node, Err(error.into())).map(HostStep::Ready);
        }
        if let Recipe::Shared(native) = self.nodes[task.node].recipe {
            let outcome = shared_value(
                self.natives[native].original,
                self.nodes[task.node].view,
                Err(LegacyEvalError::from(error)),
            );
            self.clean_source(task);
            return self.emit(task.node, outcome).map(HostStep::Ready);
        }
        let child = self.nodes[task.node].children[task.next];
        let token = self
            .values
            .insert(self.origin(child), Err(LegacyEvalError::from(error)))
            .map_err(value_error)?;
        if task.tokens.try_reserve_exact(1).is_err() {
            let _ = self.values.take(&token, self.origin(child));
            return Err(resource());
        }
        task.tokens.push(token);
        task.next += 1;
        let eager = matches!(
            self.nodes[task.node].recipe,
            Recipe::IntBinary {
                eager_error: true,
                ..
            }
        );
        if eager && task.next < self.nodes[task.node].children.len() {
            Ok(HostStep::NeedArg(HostArgRequest {
                index: task.next,
                mode: ArgMode::FreshResult,
            }))
        } else {
            self.finish(task)
        }
    }

    fn date_step(
        &mut self,
        task: &mut SourceTask,
        mut step: Result<tidb_expr::LegacyDateArithmeticDemand, tidb_expr::EvalError>,
    ) -> LocalResult<HostStep> {
        use tidb_expr::LegacyDateArithmeticDemand as Demand;
        loop {
            match step {
                Err(error) => {
                    task.stage = None;
                    self.clean_source(task);
                    return self.emit(task.node, Err(error.into())).map(HostStep::Ready);
                }
                Ok(Demand::Ready { value, presence }) => {
                    let value = if self.nodes[task.node].view == View::Int {
                        Value::Int(Some(presence))
                    } else {
                        match value {
                            tidb_expr::LegacyDateArithmeticValue::Bytes(v) => Value::Bytes(v),
                            tidb_expr::LegacyDateArithmeticValue::Time(v) => Value::Time(v),
                            tidb_expr::LegacyDateArithmeticValue::Duration(v) => Value::Duration(v),
                            _ => {
                                task.stage = None;
                                self.clean_source(task);
                                return Err(contract("date source result channel differs"));
                            }
                        }
                    };
                    task.stage = None;
                    self.clean_source(task);
                    return self.emit(task.node, Ok(value)).map(HostStep::Ready);
                }
                Ok(Demand::Source { index, channel }) => {
                    let Some(Stage::Date { machine }) = task.stage.as_deref_mut() else {
                        return Err(contract("date source state missing"));
                    };
                    task.next = index;
                    if index < self.nodes[task.node].children.len() {
                        return Ok(HostStep::NeedArg(HostArgRequest {
                            index,
                            mode: ArgMode::FreshResult,
                        }));
                    }
                    // An absent child is the original nullable callback result;
                    // it does not create a source node or query an Int scope.
                    step = machine.resume_source(date_missing(channel), self.evaluator.time_zone);
                }
            }
        }
    }

    fn in_step(
        &mut self,
        task: &mut SourceTask,
        mut step: Result<tidb_expr::LegacyInDemand, tidb_expr::EvalError>,
    ) -> LocalResult<HostStep> {
        loop {
            match step {
                Err(error) => {
                    task.stage = None;
                    self.clean_source(task);
                    return self.emit(task.node, Err(error.into())).map(HostStep::Ready);
                }
                Ok(tidb_expr::LegacyInDemand::Ready(value)) => {
                    task.stage = None;
                    self.clean_source(task);
                    return self
                        .emit(task.node, Ok(Value::Int(value)))
                        .map(HostStep::Ready);
                }
                Ok(tidb_expr::LegacyInDemand::Source { index }) => {
                    task.next = index;
                    if index < self.nodes[task.node].children.len() {
                        return Ok(HostStep::NeedArg(HostArgRequest {
                            index,
                            mode: ArgMode::FreshResult,
                        }));
                    }
                    let Some(Stage::In { machine, bytes }) = task.stage.as_deref_mut() else {
                        return Err(contract("legacy IN state missing"));
                    };
                    step = if *bytes {
                        machine.resume_bytes(None)
                    } else {
                        machine.resume_integer(None)
                    };
                }
            }
        }
    }
}

fn date_missing(
    channel: tidb_expr::LegacyDateArithmeticChannel,
) -> tidb_expr::LegacyDateArithmeticValue {
    use tidb_expr::{LegacyDateArithmeticChannel as C, LegacyDateArithmeticValue as V};
    match channel {
        C::Bytes => V::Bytes(None),
        C::FoldedInt => V::Int(None),
        C::Real => V::Real(None),
        C::Decimal => V::Decimal(None),
        C::Time => V::Time(None),
        C::Duration => V::Duration(None),
    }
}
fn date_value(value: Value) -> LegacyResult<tidb_expr::LegacyDateArithmeticValue> {
    use tidb_expr::LegacyDateArithmeticValue as V;
    Ok(match value {
        Value::Bytes(v) => V::Bytes(v),
        Value::Int(v) => V::Int(v),
        Value::Real(v) => V::Real(v),
        Value::Decimal(v) => V::Decimal(v),
        Value::Time(v) => V::Time(v),
        Value::Duration(v) => V::Duration(v),
        _ => {
            return Err(LegacyEvalError::InvalidResult(
                "date source input channel differs",
            ))
        }
    })
}

impl LocalRuntimeServices for Services<'_, '_> {
    fn binding_schema(&self) -> &[sdk_tipb::FieldType] {
        self.schema
    }
    fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> {
        Some(self)
    }
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &sdk_tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        let native = self
            .natives
            .iter()
            .position(|n| n.mount.as_ref().is_some_and(|m| m.owns_input(slot)))
            .ok_or_else(|| contract("unknown legacy native input"))?;
        self.bindings[native]
            .as_mut()
            .ok_or_else(|| contract("native input before Shared demand"))?
            .read_input(ctx, slot, row, expected)
    }
}
impl LocalHostServices for Services<'_, '_> {
    fn catalog_key(&self) -> &HostCatalogKey {
        self.catalog.key()
    }
    fn start(
        &mut self,
        ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let signature = self.catalog.signature(invocation.slot)?;
        if invocation.row
            != (InputRow {
                occurrence: 0,
                input_row: 0,
            })
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
        {
            return Err(contract("legacy source invocation differs"));
        }
        if invocation.slot.index() < self.nodes.len() {
            return self.start_source(invocation.slot.index());
        }
        let native = self
            .natives
            .iter()
            .position(|n| {
                n.mount
                    .as_ref()
                    .is_some_and(|m| m.owns_host(invocation.slot).unwrap_or(false))
            })
            .ok_or_else(|| contract("unknown legacy native host"))?;
        let start = self.bindings[native]
            .as_mut()
            .ok_or_else(|| contract("native host before Shared demand"))?
            .start(ctx, invocation)?;
        match start {
            HostStart::Ready(value) => Ok(HostStart::Ready(value)),
            HostStart::Pending { task, request } => {
                match self.allocate(Task::Native {
                    fragment: native,
                    original: task,
                }) {
                    Ok(id) => Ok(HostStart::Pending { task: id, request }),
                    Err(error) => {
                        self.bindings[native].as_mut().unwrap().cancel(&task);
                        Err(error)
                    }
                }
            }
        }
    }
    fn resume(
        &mut self,
        ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.take_task(id)?;
        match task {
            Task::Native { fragment, original } => {
                let result = self.bindings[fragment]
                    .as_mut()
                    .ok_or_else(|| contract("native task bindings disappeared"))?
                    .resume(ctx, &original, reply);
                if !matches!(result, Ok(HostStep::Ready(_))) {
                    self.slots[id.slot].task = Some(Task::Native { fragment, original });
                }
                result
            }
            Task::Source(mut task) => {
                let result = self.consume_reply(&mut task, reply.index, reply.values);
                if matches!(result, Ok(HostStep::NeedArg(_))) {
                    self.slots[id.slot].task = Some(Task::Source(task));
                } else {
                    self.clean_source(&task);
                }
                self.check()?;
                result
            }
        }
    }
    fn resume_error(
        &mut self,
        _: &mut EvalContext,
        id: &HostTaskId,
        failure: HostArgFailure,
    ) -> LocalResult<HostStep> {
        match self.take_task(id)? {
            Task::Source(mut task) => {
                let result = self.consume_failure(&mut task, failure);
                if matches!(result, Ok(HostStep::NeedArg(_))) {
                    self.slots[id.slot].task = Some(Task::Source(task));
                } else {
                    self.clean_source(&task);
                }
                self.check()?;
                result
            }
            task @ Task::Native { .. } => {
                self.slots[id.slot].task = Some(task);
                Err(failure.into_error())
            }
        }
    }
    fn cancel(&mut self, id: &HostTaskId) {
        if let Ok(task) = self.take_task(id) {
            match task {
                Task::Source(task) => self.clean_source(&task),
                Task::Native { fragment, original } => {
                    if let Some(binding) = self.bindings[fragment].as_mut() {
                        binding.cancel(&original);
                    }
                }
            }
        }
    }
}

impl Plan<'_> {
    pub(in super::super) fn evaluate(
        &mut self,
        evaluator: &LegacyEvaluator<'_>,
    ) -> LegacyResult<Value> {
        if self.width != evaluator.row.len() {
            return Err(LegacyEvalError::InvalidResult(
                "legacy physical row width differs",
            ));
        }
        let invocation = Invocation {
            row: OnceCell::new(),
            columns: (0..self.natives.len()).map(|_| OnceCell::new()).collect(),
            states: (0..self.natives.len()).map(|_| OnceCell::new()).collect(),
        };
        let mut services = Services {
            evaluator,
            nodes: &self.nodes,
            natives: &self.natives,
            catalog: &self.catalog,
            schema: &self.schema,
            invocation: &invocation,
            bindings: (0..self.natives.len()).map(|_| None).collect(),
            native_scopes: (0..self.natives.len()).map(|_| None).collect(),
            raw_scopes: vec![],
            slots: vec![],
            generation: 0,
            values: Values::new(ExecutionLimits::default().max_retained_bytes)
                .map_err(|_| PrepareError::Local(resource()).into_legacy())?,
        };
        let output = self
            .program
            .evaluate(&mut EvalContext::default(), 0, &mut services)
            .map_err(LegacyEvalError::from)?;
        let VectorValue::Bytes(values) = output else {
            return Err(LegacyEvalError::InvalidResult(
                "legacy root output is not a capability",
            ));
        };
        let token = values.get(0).ok_or(LegacyEvalError::InvalidResult(
            "legacy root output is wire NULL",
        ))?;
        services
            .values
            .take(token, services.origin(0))
            .map_err(|_| LegacyEvalError::InvalidResult("legacy root capability differs"))?
    }
}

fn shared_value(
    shared: &SharedExpression,
    view: View,
    result: LegacyResult<tidb_datatype::Datum>,
) -> LegacyResult<Value> {
    use tidb_datatype::Datum;
    if view == View::Datum {
        return result.map(Value::Datum);
    }
    if view == View::Int {
        return result.and_then(|value| {
            Ok(Value::Int(match value {
                Datum::Int(v) => Some(i128::from(v)),
                Datum::UInt(v) => Some(i128::from(v)),
                Datum::Null => None,
                other => Some(shared.context.condition_value(&other)?),
            }))
        });
    }
    let value = fold_legacy_sql(result)?;
    Ok(match view {
        View::Decimal => Value::Decimal(match value {
            Some(Datum::Decimal(v)) => Some(v),
            _ => None,
        }),
        View::Json => Value::Json(match value {
            Some(Datum::Json(v)) => Some(v),
            _ => None,
        }),
        View::Real => Value::Real(match value {
            Some(Datum::Real(v) | Datum::Float32(v)) => Some(v),
            _ => None,
        }),
        View::Duration => Value::Duration(match value {
            Some(Datum::Duration(v)) => Some(v),
            _ => None,
        }),
        View::Time => Value::Time(match value {
            Some(Datum::Time(v)) => Some(v),
            _ => None,
        }),
        View::Bytes => Value::Bytes(match value {
            Some(Datum::Bytes(v)) => Some(v),
            Some(Datum::String(v)) => Some(v.bytes().to_vec()),
            _ => None,
        }),
        _ => {
            return Err(LegacyEvalError::InvalidResult(
                "unknown legacy Shared consumer",
            ))
        }
    })
}

fn leaf(node: &Node<'_>, evaluator: &LegacyEvaluator<'_>) -> LegacyResult<Value> {
    use tidb_datatype::Datum;
    let column = match node.original {
        SimpleExpr::Column(index) => evaluator.row.get(*index),
        _ => None,
    };
    Ok(match node.view {
        View::Int => Value::Int(match node.original {
            SimpleExpr::Int(v) => Some(i128::from(*v)),
            _ => match column {
                Some(Datum::Int(v)) => Some(i128::from(*v)),
                Some(Datum::UInt(v)) => Some(i128::from(*v)),
                _ => None,
            },
        }),
        View::Decimal => Value::Decimal(match node.original {
            SimpleExpr::Decimal(v) => Some(v.clone()),
            _ => match column {
                Some(Datum::Decimal(v)) => Some(v.clone()),
                _ => None,
            },
        }),
        View::Json => Value::Json(match node.original {
            SimpleExpr::Json(v) => Some(v.clone()),
            _ => match column {
                Some(Datum::Json(v)) => Some(v.clone()),
                _ => None,
            },
        }),
        View::Real => Value::Real(match node.original {
            SimpleExpr::Real(v) => Some(*v),
            _ => match column {
                Some(Datum::Real(v)) => Some(*v),
                _ => None,
            },
        }),
        View::Duration => Value::Duration(match column {
            Some(Datum::Duration(v)) => Some(*v),
            Some(Datum::Time(v)) => v.to_duration().ok(),
            _ => None,
        }),
        View::Time => Value::Time(match node.original {
            SimpleExpr::Time(v) => Some(*v),
            _ => match column {
                Some(Datum::Time(v)) => Some(*v),
                _ => None,
            },
        }),
        View::Bytes => Value::Bytes(match node.original {
            SimpleExpr::Bytes(v) => Some(v.clone()),
            _ => match column {
                Some(Datum::Bytes(v)) => Some(v.clone()),
                Some(Datum::String(v)) => Some(v.bytes().to_vec()),
                _ => None,
            },
        }),
        View::Datum => Value::Datum(match node.original {
            SimpleExpr::Column(_) => column.cloned().ok_or_else(|| {
                LegacyEvalError::Sql("aggregate input is outside the scanned row".into())
            })?,
            SimpleExpr::Int(v) => Datum::Int(*v),
            SimpleExpr::Null => Datum::Null,
            SimpleExpr::Bytes(v) => Datum::Bytes(v.clone()),
            SimpleExpr::Decimal(v) => Datum::Decimal(v.clone()),
            SimpleExpr::Json(v) => Datum::Json(v.clone()),
            SimpleExpr::Real(v) => Datum::Real(*v),
            SimpleExpr::Time(v) => Datum::Time(*v),
            SimpleExpr::Func(..) => {
                return Err("a computed aggregate argument is a later course"
                    .to_owned()
                    .into())
            }
            _ => {
                return Err(LegacyEvalError::InvalidResult(
                    "unexpected legacy datum leaf",
                ))
            }
        }),
        _ => {
            return Err(LegacyEvalError::InvalidResult(
                "unexpected legacy leaf view",
            ))
        }
    })
}

fn kernel(
    node: &Node<'_>,
    tokens: &[Vec<u8>],
    nodes: &[Node<'_>],
    values: &mut Values,
    evaluator: &LegacyEvaluator<'_>,
    columns: &dyn tidb_expr::Columns,
    need_length: Option<bool>,
) -> LocalResult<LegacyResult<Value>> {
    if matches!(node.recipe, Recipe::Leaf) {
        return Ok(leaf(node, evaluator));
    }
    if matches!(node.recipe, Recipe::Null) {
        return Ok(Ok(typed::null(node.view)));
    }
    if matches!(node.recipe, Recipe::JsonLiteralNull) {
        return Ok(Ok(Value::Json(
            tidb_datatype::BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Null).ok(),
        )));
    }
    if let Recipe::ConstantInt(value) = node.recipe {
        return Ok(Ok(Value::Int(Some(value))));
    }
    let mut args = Vec::new();
    for (token, child) in tokens.iter().zip(&node.children) {
        args.push(
            values
                .take(
                    token,
                    Origin {
                        node: *child,
                        view: nodes[*child].view,
                        occurrence: 0,
                        physical: 0,
                    },
                )
                .map_err(value_error)?,
        );
    }
    let mut args = args.into_iter();
    match node.recipe {
        Recipe::Typed { .. } => {
            return Ok(typed::evaluate(node, args, evaluator, columns, need_length))
        }
        Recipe::Condition(view) => return Ok(typed::condition_value(view, args)),
        _ => {}
    }
    let int = |value: Option<LegacyResult<Value>>| -> LegacyResult<Option<i128>> {
        match value {
            None => Ok(None),
            Some(Ok(Value::Int(v))) => Ok(v),
            Some(Err(error)) => Err(error),
            _ => Err(LegacyEvalError::InvalidResult(
                "legacy integer port view differs",
            )),
        }
    };
    let outcome = (|| -> LegacyResult<Value> {
        if matches!(node.recipe, Recipe::FoldedInt) {
            return Ok(Value::Int(fold_legacy_sql(int(args.next()))?.flatten()));
        }
        let left = int(args.next());
        let answer = match node.recipe {
            Recipe::IntUnary(signature) => {
                let left = left?;
                match signature {
                    SimpleSig::CastIntAsInt => left,
                    SimpleSig::RoundInt => tidb_expr::eval_legacy_round_int_in(left, columns)?,
                    SimpleSig::UnaryNot => eval_boolean_ready(
                        tidb_expr::BooleanFunction::UnaryNot,
                        left.map(|v| v != 0),
                        columns,
                    )?,
                    SimpleSig::IntIsNull => eval_boolean_ready(
                        tidb_expr::BooleanFunction::IsNull,
                        left.map(|_| false),
                        columns,
                    )?,
                    _ => {
                        return Err(LegacyEvalError::InvalidResult(
                            "unknown integer unary source",
                        ))
                    }
                }
            }
            Recipe::IntBinary {
                signature,
                eager_error,
            } => {
                let right = int(args.next());
                let (left, right) = if eager_error {
                    (
                        fold_legacy_sql(left)?.flatten(),
                        fold_legacy_sql(right)?.flatten(),
                    )
                } else {
                    (left?, right?)
                };
                match signature {
                    SimpleSig::LogicalAnd | SimpleSig::LogicalOr => eval_logical_ready(
                        if signature == SimpleSig::LogicalAnd {
                            tidb_expr::LogicalFunction::And
                        } else {
                            tidb_expr::LogicalFunction::Or
                        },
                        left.map(|v| v != 0),
                        right.map(|v| v != 0),
                        columns,
                    )?,
                    SimpleSig::LtInt
                    | SimpleSig::LeInt
                    | SimpleSig::GtInt
                    | SimpleSig::GeInt
                    | SimpleSig::EqInt
                    | SimpleSig::NeInt => {
                        let op = match signature {
                            SimpleSig::LtInt => tidb_expr::ComparisonOp::Lt,
                            SimpleSig::LeInt => tidb_expr::ComparisonOp::Le,
                            SimpleSig::GtInt => tidb_expr::ComparisonOp::Gt,
                            SimpleSig::GeInt => tidb_expr::ComparisonOp::Ge,
                            SimpleSig::EqInt => tidb_expr::ComparisonOp::Eq,
                            _ => tidb_expr::ComparisonOp::Ne,
                        };
                        tidb_expr::eval_legacy_integer_comparison_in(
                            op,
                            legacy_comparison_args(node.original_arity, left, right),
                            columns,
                        )?
                    }
                    _ => {
                        use tidb_expr::LegacyIntegerArithmetic as P;
                        use SimpleSig::*;
                        let profile = match signature {
                            PlusInt => P::AddSigned,
                            PlusIntUnsignedUnsigned | PlusIntSignedSigned => P::AddUnsigned,
                            PlusIntUnsignedSigned => P::AddRejectRight,
                            PlusIntSignedUnsigned => P::AddRejectLeft,
                            MinusInt => P::SubSigned,
                            MinusIntUnsignedSigned | MinusIntForcedUnsignedSigned => {
                                P::SubRejectRight
                            }
                            MinusIntForcedSignedUnsigned => P::SubRejectLeft,
                            MinusIntUnsignedUnsigned
                            | MinusIntSignedUnsigned
                            | MinusIntSignedSigned
                            | MinusIntForcedUnsignedUnsigned => P::SubUnsigned,
                            MultiplyInt => P::MulSigned,
                            MultiplyIntUnsigned => P::MulUnsigned,
                            ModIntUnsignedUnsigned
                            | ModIntUnsignedSigned
                            | ModIntSignedUnsigned
                            | ModIntSignedSigned => P::Modulo,
                            IntDivideInt
                            | IntDivideIntUnsignedUnsigned
                            | IntDivideIntUnsignedSigned
                            | IntDivideIntSignedSigned
                            | IntDivideIntSignedUnsigned => P::IntDivide,
                            _ => {
                                return Err(LegacyEvalError::InvalidResult(
                                    "unknown integer binary source",
                                ))
                            }
                        };
                        tidb_expr::eval_legacy_integer_arithmetic_in(
                            profile,
                            legacy_comparison_args(node.original_arity, left, right),
                            columns,
                        )
                        .map_err(|error| match error {
                            tidb_expr::EvalError::DataOutOfRange { value, expression }
                                if eager_error =>
                            {
                                LegacyEvalError::Sql(format!(
                                    "{value} value is out of range in '{expression}'"
                                ))
                            }
                            error => LegacyEvalError::from(error),
                        })?
                    }
                }
            }
            _ => {
                return Err(LegacyEvalError::InvalidResult(
                    "unexpected legacy kernel recipe",
                ))
            }
        };
        Ok(Value::Int(answer))
    })();
    Ok(outcome)
}
