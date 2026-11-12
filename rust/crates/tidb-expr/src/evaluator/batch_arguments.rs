// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Shared eager child collection; each kernel retains its original value phases.
use super::*;

pub(in super::super) enum NativeArgumentNodes {
    Empty,
    One(usize),
    Three([usize; 3]),
    Two([usize; 2]),
    Many(Box<[usize]>),
}
impl From<usize> for NativeArgumentNodes {
    fn from(node: usize) -> Self {
        Self::One(node)
    }
}
impl From<Box<[usize]>> for NativeArgumentNodes {
    fn from(nodes: Box<[usize]>) -> Self {
        Self::Many(nodes)
    }
}
impl From<Option<usize>> for NativeArgumentNodes {
    fn from(node: Option<usize>) -> Self {
        node.map_or(Self::Empty, Self::One)
    }
}
impl From<[usize; 3]> for NativeArgumentNodes {
    fn from(nodes: [usize; 3]) -> Self {
        Self::Three(nodes)
    }
}
impl From<[usize; 2]> for NativeArgumentNodes {
    fn from(nodes: [usize; 2]) -> Self {
        Self::Two(nodes)
    }
}
impl std::ops::Deref for NativeArgumentNodes {
    type Target = [usize];
    fn deref(&self) -> &[usize] {
        match self {
            Self::Empty => &[],
            Self::Three(nodes) => nodes,
            Self::Two(nodes) => nodes,
            Self::One(node) => std::slice::from_ref(node),
            Self::Many(nodes) => nodes,
        }
    }
}
pub(in super::super) enum NativeArgumentKernel {
    Extract {
        source_type: FieldType,
        unit: Option<Box<str>>,
    },
    BoundedStaleness {
        argument_types: [FieldType; 2],
    },
    StrToDate {
        output: tidb_query_expr::local::SqlTemporalOutput,
    },
    DurationConstruction {
        operation: tidb_query_expr::NativeDurationConstructionOperation,
        argument_types: Box<[FieldType]>,
    },
    Calendar {
        operation: tidb_query_expr::NativeCalendarOperation,
        source_type: FieldType,
    },
    Clock {
        operation: tidb_query_expr::NativeClockOperation,
    },
    Tso {
        operation: tidb_query_expr::NativeTsoOperation,
        argument_type: Option<FieldType>,
    },
    Epoch {
        from: bool,
    },
    TimestampDiff {
        stop_on_null: bool,
        argument_types: [FieldType; 3],
    },
    Crypto {
        operation: tidb_query_expr::NativeCryptoOperation,
        probe: Option<Datum>,
    },
    Compression {
        operation: tidb_query_expr::NativeCompressionOperation,
        probe: Option<Datum>,
    },
    ByteUnary {
        operation: tidb_query_expr::NativeByteUnaryOperation,
        probe: Option<Datum>,
    },
    Misc {
        operation: tidb_query_expr::NativeMiscOperation,
        probe: Option<Datum>,
    },
    Identity {
        name_const: bool,
        probe: Option<Datum>,
    },
    Grouping(Option<std::sync::Arc<crate::grouping::GroupingMetadata>>),
    Vector(tidb_query_expr::NativeVectorOperation),
}
pub(in super::super) struct NativeArguments {
    pub arguments: NativeArgumentNodes,
    pub kernel: NativeArgumentKernel,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: Option<NativeValueRole>,
}
impl NativeArguments {
    pub(super) fn probe(&self) -> Option<&Datum> {
        // Only these kernels capture a probe; keeping it here avoids enlarging
        // every host descriptor around the largest temporal payload.
        match &self.kernel {
            NativeArgumentKernel::Crypto { probe, .. }
            | NativeArgumentKernel::Compression { probe, .. }
            | NativeArgumentKernel::ByteUnary { probe, .. }
            | NativeArgumentKernel::Misc { probe, .. }
            | NativeArgumentKernel::Identity { probe, .. } => probe.as_ref(),
            _ => None,
        }
    }
    pub(super) fn accepts(&self, operation: SqlBatchHostOperation, arity: usize) -> bool {
        if arity != self.arguments.len()
            || arity > 3
            || (!matches!(self.kernel, NativeArgumentKernel::Grouping(_))
                && self.result_type.is_none())
            || (!matches!(
                self.kernel,
                NativeArgumentKernel::TimestampDiff { .. }
                    | NativeArgumentKernel::Extract { .. }
                    | NativeArgumentKernel::BoundedStaleness { .. }
                    | NativeArgumentKernel::StrToDate { .. }
            ) && self.output_role.is_none())
        {
            return false;
        }
        match (&self.kernel, operation) {
            (
                NativeArgumentKernel::Crypto { operation: a, .. },
                SqlBatchHostOperation::CryptoNative(b),
            ) => *a == b && arity == a.arity(),
            (
                NativeArgumentKernel::Compression { operation: a, .. },
                SqlBatchHostOperation::CompressionNative(b),
            ) => *a == b && arity == 1,
            (
                NativeArgumentKernel::ByteUnary { operation: a, .. },
                SqlBatchHostOperation::ByteUnaryNative(b),
            ) => *a == b && arity == 1,
            (
                NativeArgumentKernel::Misc { operation: a, .. },
                SqlBatchHostOperation::MiscNative {
                    operation: b,
                    arity: n,
                },
            ) => *a == b && arity == usize::from(n) && a.accepts_arity(arity),
            (
                NativeArgumentKernel::Identity { name_const: a, .. },
                SqlBatchHostOperation::ValueIdentityNative { name_const: b },
            ) => *a == b && arity == 1 + usize::from(b),
            (NativeArgumentKernel::Grouping(_), SqlBatchHostOperation::GroupingNative) => {
                arity == 1
            }
            (NativeArgumentKernel::Vector(a), SqlBatchHostOperation::VectorNative(b)) => {
                *a == b && arity == a.arity()
            }
            (
                NativeArgumentKernel::DurationConstruction {
                    operation: a,
                    argument_types,
                },
                SqlBatchHostOperation::DurationConstructionNative(b),
            ) => *a == b && arity == a.arity() && argument_types.len() == arity,
            (
                NativeArgumentKernel::Calendar { operation: a, .. },
                SqlBatchHostOperation::CalendarNative {
                    operation: b,
                    arity: n,
                },
            ) => *a == b && arity == usize::from(n) && a.accepts_arity(arity),
            (
                NativeArgumentKernel::Clock { operation: a },
                SqlBatchHostOperation::ClockNative {
                    operation: b,
                    arity: n,
                },
            ) => *a == b && arity == usize::from(n) && a.accepts_arity(arity),
            (
                NativeArgumentKernel::Tso {
                    operation: a,
                    argument_type,
                },
                SqlBatchHostOperation::TsoNative(b),
            ) => *a == b && arity == a.arity() && argument_type.is_some() == (arity != 0),
            (
                NativeArgumentKernel::Epoch { from: a },
                SqlBatchHostOperation::EpochNative { from: b, arity: n },
            ) => *a == b && arity == usize::from(n),
            (
                NativeArgumentKernel::TimestampDiff {
                    stop_on_null: a, ..
                },
                SqlBatchHostOperation::TimestampDiffNative {
                    typed,
                    stop_on_null: b,
                },
            ) => *a == b && arity == 3 && typed == self.output_role.is_none(),
            (
                NativeArgumentKernel::Extract { unit, .. },
                SqlBatchHostOperation::ExtractNative { typed, composite },
            ) => {
                arity == if composite { 1 } else { 2 }
                    && unit.is_some() == composite
                    && typed == self.output_role.is_none()
            }
            (
                NativeArgumentKernel::BoundedStaleness { .. },
                SqlBatchHostOperation::BoundedStalenessNative { typed },
            ) => arity == 2 && typed == self.output_role.is_none(),
            (
                NativeArgumentKernel::StrToDate { output: a },
                SqlBatchHostOperation::StrToDateNative { output: b },
            ) => {
                arity == 2
                    && *a == b
                    && (b == tidb_query_expr::local::SqlTemporalOutput::Value)
                        == self.output_role.is_some()
            }
            _ => false,
        }
    }
    fn name(&self) -> &'static str {
        match self.kernel {
            NativeArgumentKernel::Crypto { operation: op, .. } => op.name(),
            NativeArgumentKernel::Compression { operation: op, .. } => op.name(),
            NativeArgumentKernel::ByteUnary { operation: op, .. } => op.name(),
            NativeArgumentKernel::Misc { operation: op, .. } => op.name(),
            NativeArgumentKernel::Identity {
                name_const: true, ..
            } => "name_const",
            NativeArgumentKernel::Identity {
                name_const: false, ..
            } => "any_value",
            NativeArgumentKernel::Grouping(_) => "grouping",
            NativeArgumentKernel::Vector(op) => op.name(),
            NativeArgumentKernel::TimestampDiff { .. } => "timestampdiff",
            NativeArgumentKernel::Extract { .. } => "extract",
            NativeArgumentKernel::BoundedStaleness { .. } => "tidb_bounded_staleness",
            NativeArgumentKernel::StrToDate { .. } => "str_to_date",
            NativeArgumentKernel::DurationConstruction { operation, .. } => operation.name(),
            NativeArgumentKernel::Calendar { operation, .. } => operation.name(),
            NativeArgumentKernel::Clock { operation } => operation.name(),
            NativeArgumentKernel::Tso { operation, .. } => operation.name(),
            NativeArgumentKernel::Epoch { from: true } => "from_unixtime",
            NativeArgumentKernel::Epoch { from: false } => "unix_timestamp",
        }
    }
}
impl NativeBatchHosts<'_> {
    // The caller validates the reply and task once; no callback separates that
    // check from consuming this phase. Callback-dependent value reads stay below.
    pub(super) fn resume_arguments(
        &mut self,
        id: &HostTaskId,
        slot: HostSlot,
        row: InputRow,
        next: usize,
        mut tokens: [Option<native_values::NativeValueHandle>; 3],
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let Some(NativeHostMetadata::Arguments(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("native argument metadata absent"));
        };
        let arguments = &meta.arguments;
        let arity = arguments.len();
        if next >= arity.max(1) || token.is_some() != (arity != 0) {
            return Err(binding("native argument task consumed"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("native argument store absent"))?;
        let origin = |i: usize| NativeValueOrigin {
            node: arguments[i],
            row,
        };
        let observed_null = if let Some(token) = token {
            let value = store.borrow();
            let is_null = value
                .get(token, NativeValueRole::CharInput, origin(next))?
                .is_null();
            tokens[next] = Some(
                token
                    .try_into()
                    .map_err(|_| binding("native argument capability width differs"))?,
            );
            if matches!(meta.kernel, NativeArgumentKernel::TimestampDiff { .. }) {
                for i in 0..=next {
                    value.get(
                        tokens[i].as_ref().expect("collected prefix"),
                        NativeValueRole::CharInput,
                        origin(i),
                    )?;
                }
            }
            is_null
        } else {
            false
        };
        let stop_on_null = observed_null
            && matches!(
                meta.kernel,
                NativeArgumentKernel::TimestampDiff {
                    stop_on_null: true,
                    ..
                }
            );
        // Close the stage before requesting another child or invoking any body.
        self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated argument task")
            .demand = SqlDemand::Arguments {
            next: if stop_on_null { arity } else { next + 1 },
            tokens,
        };
        if next + 1 < arity && !stop_on_null {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        let token_for = |i: usize| {
            tokens[i]
                .as_ref()
                .map(|v| v.as_slice())
                .ok_or_else(|| binding("native argument capability lost"))
        };
        let inputs = ArgumentValues {
            store,
            nodes: arguments,
            tokens: &tokens,
            row,
        };
        // NAME_CONST does not retain or read the ignored name after demanding it.
        if matches!(
            meta.kernel,
            NativeArgumentKernel::Crypto { operation: _, .. }
                | NativeArgumentKernel::Misc { operation: _, .. }
                | NativeArgumentKernel::Vector(_)
                | NativeArgumentKernel::Calendar { .. }
                | NativeArgumentKernel::Epoch { .. }
                | NativeArgumentKernel::DurationConstruction { .. }
                | NativeArgumentKernel::Extract { .. }
                | NativeArgumentKernel::BoundedStaleness { .. }
                | NativeArgumentKernel::StrToDate { .. }
        ) {
            let values = store.borrow();
            for i in 0..arity {
                inputs.get(&values, i)?;
            }
        }
        let maximum = usize::MAX;
        let result = if stop_on_null {
            Ok(Datum::Null)
        } else {
            match &meta.kernel {
                NativeArgumentKernel::Extract { source_type, unit } => {
                    let null_unit = arity == 2 && inputs.get(&store.borrow(), 0)?.is_null();
                    if null_unit {
                        Ok(Datum::Null)
                    } else {
                        let owned = inputs.copy(0)?;
                        let second = if arity == 2 {
                            Some(inputs.copy(1)?)
                        } else {
                            None
                        };
                        match unit {
                            Some(unit) => crate::tikv::eval_extract_composite_values_bounded(
                                self.context,
                                unit,
                                std::slice::from_ref(&owned),
                                maximum,
                            ),
                            None => crate::tikv::eval_extract_scalar_values_bounded(
                                self.context,
                                &owned,
                                second.as_ref().expect("two extract arguments"),
                                Some(source_type),
                                maximum,
                            ),
                        }
                    }
                }
                NativeArgumentKernel::BoundedStaleness { argument_types } => {
                    let mut owned = [inputs.copy(0)?, inputs.copy(1)?];
                    crate::tikv::bounded_staleness_cast_arguments(
                        self.context,
                        &mut owned,
                        argument_types,
                        maximum,
                    )
                    .and_then(|()| {
                        crate::tikv::bounded_staleness_bounded(self.context, &owned, maximum)
                    })
                }
                NativeArgumentKernel::StrToDate { .. } => {
                    let field = meta.result_type.as_ref().expect("checked temporal result");
                    let prepared = {
                        let values = store.borrow();
                        crate::tikv::prepare_str_to_date_value(
                            inputs.get(&values, 0)?,
                            inputs.get(&values, 1)?,
                            maximum,
                        )
                    };
                    prepared
                        .and_then(|prepared| {
                            crate::tikv::compute_str_to_date_value(
                                self.context,
                                prepared.as_ref(),
                                Some(field.code()),
                                maximum,
                            )
                        })
                        .and_then(|value| {
                            crate::scalar_function::finish_native_temporal_result(
                                Some(field),
                                value,
                                self.context,
                            )
                        })
                }
                NativeArgumentKernel::DurationConstruction {
                    operation,
                    argument_types,
                } => {
                    use crate::tikv::duration_construction_value as values;
                    use tidb_query_expr::NativeDurationConstructionOperation as O;
                    let result_type = meta
                        .result_type
                        .as_ref()
                        .expect("checked temporal result type");
                    let maximum = usize::MAX;
                    let outcome = if *operation == O::GetFormat {
                        let source = store.borrow();
                        values::get_format_source(
                            [inputs.get(&source, 0)?, inputs.get(&source, 1)?],
                            maximum,
                        )
                    } else if *operation == O::DateFormat {
                        let original = inputs.copy(0)?;
                        let cast = crate::tikv::calendar_value::cast_source(
                            tidb_query_expr::NativeCalendarOperation::Date,
                            &original,
                            &argument_types[0],
                            self.context,
                            usize::MAX,
                        );
                        drop(original);
                        match cast {
                            Err(e) => Err(e),
                            Ok(date) => {
                                // This owner performs only strict pure text coercion and
                                // formatting. No context callback spans this format borrow;
                                // both text conversions remain demanded even after NULL.
                                let source = store.borrow();
                                let format = inputs.get(&source, 1)?;
                                crate::tikv::eval_date_format_values_bounded(
                                    self.context,
                                    &date,
                                    format,
                                    usize::MAX,
                                )
                            }
                        }
                    } else if *operation == O::DateDiff {
                        // Both original ETDatetime casts precede signature text conversion.
                        // A failing left cast must not clone or convert the right payload.
                        let mut converted = Vec::new();
                        converted
                            .try_reserve_exact(2)
                            .map_err(|_| resource("date diff conversion allocation failed"))?;

                        let mut error = None;
                        for index in 0..2 {
                            let original = inputs.copy(index)?;
                            let cast = crate::tikv::calendar_value::cast_source(
                                tidb_query_expr::NativeCalendarOperation::Date,
                                &original,
                                &argument_types[index],
                                self.context,
                                usize::MAX,
                            );
                            drop(original);
                            match cast {
                                Ok(v) => {
                                    converted.push(v);
                                }
                                Err(e) => {
                                    error = Some(e);
                                    break;
                                }
                            }
                        }
                        match error {
                            Some(e) => Err(e),
                            None => values::eval_bounded(
                                *operation,
                                &converted,
                                self.context,
                                usize::MAX,
                            ),
                        }
                    } else {
                        // TIMESTAMPADD's original ETDatetime cast of argument two precedes
                        // body conversion of unit/amount, even after a NULL prefix. Keep
                        // its actual converted value without copying either prefix early.
                        let date = if *operation == O::TimestampAdd {
                            let original = inputs.copy(2)?;
                            let cast = crate::tikv::calendar_value::cast_source(
                                tidb_query_expr::NativeCalendarOperation::Date,
                                &original,
                                &argument_types[2],
                                self.context,
                                usize::MAX,
                            );
                            drop(original);
                            match cast {
                                Ok(value) => Some(value),
                                Err(e) => return self.native_failure(e),
                            }
                        } else {
                            None
                        };

                        let maximum = usize::MAX;
                        let start_calls_context = {
                            let source = store.borrow();
                            let value = inputs.get(&source, 0)?;
                            values::start_calls_context(*operation, value)
                        };
                        let first = if *operation == O::ConvertTz {
                            // Original first ETDatetime cast precedes all signature text
                            // coercions, carrying YEAR and other complete source metadata.
                            let original = inputs.copy(0)?;
                            let cast = crate::tikv::calendar_value::cast_source(
                                tidb_query_expr::NativeCalendarOperation::Date,
                                &original,
                                &argument_types[0],
                                self.context,
                                usize::MAX,
                            );
                            drop(original);
                            match cast {
                                Err(e) => Err(e),
                                Ok(value) => {
                                    values::start(*operation, &value, self.context, usize::MAX)
                                }
                            }
                        } else if start_calls_context {
                            let original = inputs.copy(0)?;
                            values::start(*operation, &original, self.context, usize::MAX)
                        } else {
                            let source = store.borrow();
                            let original = inputs.get(&source, 0)?;
                            values::start(*operation, original, self.context, maximum)
                        };
                        match first {
                            Err(e) => Err(e),
                            Ok(mut prepared) => loop {
                                let Some((index, callback)) = prepared.argument() else {
                                    break prepared.into_value();
                                };
                                if index >= arity {
                                    return Err(binding(
                                        "duration value stage exceeds original arity",
                                    ));
                                }

                                let next = if index == 2 && date.is_some() {
                                    prepared.advance(
                                        date.as_ref().expect("original date cast"),
                                        self.context,
                                        usize::MAX,
                                    )
                                } else if callback {
                                    let original = inputs.copy(index)?;
                                    prepared.advance(&original, self.context, usize::MAX)
                                } else {
                                    let source = store.borrow();
                                    let original = inputs.get(&source, index)?;
                                    prepared.advance(original, self.context, usize::MAX)
                                };
                                match next {
                                    Ok(v) => prepared = v,
                                    Err(e) => break Err(e),
                                }
                            },
                        }
                    };
                    let result = match outcome {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };

                    let result = match values::finish_source(
                        *operation,
                        result,
                        result_type,
                        self.context,
                        usize::MAX,
                    ) {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    Ok(result)
                }
                NativeArgumentKernel::Calendar {
                    operation,
                    source_type,
                } => {
                    let result_type = meta
                        .result_type
                        .as_ref()
                        .expect("checked temporal result type");
                    let owned = if arity > 0 {
                        let values = store.borrow();
                        let origin = NativeValueOrigin {
                            node: arguments[0],
                            row,
                        };
                        values.get(token_for(0)?, NativeValueRole::CharInput, origin)?;

                        Some(values.clone_for_kernel(
                            token_for(0)?,
                            NativeValueRole::CharInput,
                            origin,
                        )?)
                    } else {
                        None
                    };

                    let remaining = usize::MAX;
                    let converted = if operation.datetime_argument() {
                        match crate::tikv::calendar_value::cast_source(
                            *operation,
                            owned
                                .as_ref()
                                .ok_or_else(|| binding("calendar source lost"))?,
                            source_type,
                            self.context,
                            remaining,
                        ) {
                            Ok(v) => Some(v),
                            Err(e) => return self.native_failure(e),
                        }
                    } else {
                        None
                    };
                    // Eager session getter follows the original cast, with no store borrow.
                    let default = if *operation == tidb_query_expr::NativeCalendarOperation::Week {
                        self.context.default_week_format()
                    } else {
                        0
                    };
                    let value = converted.as_ref().unwrap_or(
                        owned
                            .as_ref()
                            .ok_or_else(|| binding("calendar source lost"))?,
                    );
                    let head = crate::tikv::calendar_value::head(
                        *operation,
                        value,
                        self.context,
                        remaining,
                    );
                    drop(converted);
                    drop(owned);
                    let result = match head {
                        Err(e) => return self.native_failure(e),
                        Ok(crate::tikv::calendar_value::Head::Value(v)) => v,
                        Ok(crate::tikv::calendar_value::Head::Week(v)) => {
                            let mode = if arity == 2 {
                                let values = store.borrow();
                                let value = inputs.get(&values, 1)?;
                                crate::tikv::calendar_value::mode(value, remaining)
                            } else {
                                Ok(Some(default))
                            };
                            let mode = match mode {
                                Ok(v) => v,
                                Err(e) => return self.native_failure(e),
                            };
                            match crate::tikv::calendar_value::finish_week(v, mode, remaining) {
                                Ok(v) => v,
                                Err(e) => return self.native_failure(e),
                            }
                        }
                    };

                    let result = match crate::tikv::calendar_value::finish_source(
                        *operation,
                        result,
                        result_type,
                        self.context,
                        usize::MAX,
                    ) {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    Ok(result)
                }
                NativeArgumentKernel::Clock { operation } => {
                    let argument = arguments.first().copied();
                    let result_type = meta
                        .result_type
                        .as_ref()
                        .expect("checked temporal result type");
                    let origin = argument.map(|node| NativeValueOrigin { node, row });
                    if let Some(origin) = origin {
                        store.borrow().get(
                            token.ok_or_else(|| binding("clock source lost"))?,
                            NativeValueRole::CharInput,
                            origin,
                        )?;
                    }
                    // SYSDATE's flag precedes precision validation. No borrow spans it.
                    let resolved = crate::tikv::clock_value::resolve(*operation, self.context);
                    let prepared = {
                        let values = store.borrow();
                        let input = match origin {
                            Some(origin) => std::slice::from_ref(values.get(
                                token.ok_or_else(|| binding("clock source lost"))?,
                                NativeValueRole::CharInput,
                                origin,
                            )?),
                            None => &[],
                        };
                        crate::tikv::clock_value::prepare(resolved, input, maximum)
                    };
                    let prepared = match prepared {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    let result =
                        match crate::tikv::clock_value::finish(prepared, self.context, maximum) {
                            Ok(v) => v,
                            Err(e) => return self.native_failure(e),
                        };

                    let result = match crate::tikv::clock_value::finish_source(
                        *operation,
                        result,
                        result_type,
                        self.context,
                        usize::MAX,
                    ) {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    Ok(result)
                }
                NativeArgumentKernel::Tso {
                    operation,
                    argument_type,
                } => {
                    let argument = arguments.first();
                    let result_type = meta
                        .result_type
                        .as_ref()
                        .expect("checked temporal result type");
                    let owned = if let Some(node) = argument {
                        let token = token.ok_or_else(|| binding("TSO source lost"))?;
                        let origin = NativeValueOrigin { node: *node, row };
                        let values = store.borrow();
                        values.get(token, NativeValueRole::CharInput, origin)?;

                        Some(values.clone_for_kernel(token, NativeValueRole::CharInput, origin)?)
                    } else {
                        None
                    };

                    let remaining = usize::MAX;
                    // The clone, not a store borrow, crosses the original zone/transaction
                    // getter. These callbacks may exclusively access the frame-owned store.
                    let result = crate::tikv::tso_value::eval_source_bounded(
                        *operation,
                        owned.as_ref().map_or(&[] as &[Datum], std::slice::from_ref),
                        argument_type.as_ref(),
                        self.context,
                        remaining,
                    );
                    drop(owned);
                    let result = match result {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };

                    let result = match crate::tikv::coerce_result_bounded(
                        operation.name(),
                        result_type,
                        result,
                    ) {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    Ok(result)
                }
                NativeArgumentKernel::Epoch { from } => {
                    let result_type = meta
                        .result_type
                        .as_ref()
                        .expect("checked temporal result type");
                    let owned = if arity > 0 {
                        let values = store.borrow();
                        let origin = NativeValueOrigin {
                            node: arguments[0],
                            row,
                        };
                        values.get(token_for(0)?, NativeValueRole::CharInput, origin)?;

                        Some(values.clone_for_kernel(
                            token_for(0)?,
                            NativeValueRole::CharInput,
                            origin,
                        )?)
                    } else {
                        None
                    };

                    let remaining = usize::MAX;
                    let result = if *from {
                        crate::tikv::eval_from_unixtime_head_values_bounded(
                            self.context,
                            owned
                                .as_ref()
                                .ok_or_else(|| binding("epoch conversion source lost"))?,
                            remaining,
                        )
                    } else {
                        crate::tikv::eval_unix_timestamp_values_bounded(
                            self.context,
                            owned.as_ref().map_or(&[] as &[Datum], std::slice::from_ref),
                            remaining,
                        )
                    };
                    drop(owned);
                    let mut result = match result {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    if *from && arity == 2 && !result.is_null() {
                        // The frame owns the source window throughout callbacks. Re-check
                        // its original capability when the format is actually consumed;
                        // no store borrow spans warning, zone, clock or truncate callbacks.

                        let format = {
                            let values = store.borrow();
                            let origin = NativeValueOrigin {
                                node: arguments[1],
                                row,
                            };
                            values.get(token_for(1)?, NativeValueRole::CharInput, origin)?;

                            values.clone_for_kernel(
                                token_for(1)?,
                                NativeValueRole::CharInput,
                                origin,
                            )?
                        };
                        let value = crate::tikv::eval_date_format_values_bounded(
                            self.context,
                            &result,
                            &format,
                            usize::MAX,
                        );
                        drop(format);
                        drop(result);
                        result = match value {
                            Ok(v) => v,
                            Err(e) => return self.native_failure(e),
                        };
                    }
                    if *from {
                        result = match crate::tikv::finish_from_unixtime_values_bounded(
                            self.context,
                            result,
                            result_type,
                            usize::MAX,
                        ) {
                            Ok(v) => v,
                            Err(e) => return self.native_failure(e),
                        };
                    }

                    result = match crate::tikv::finish_epoch_result_bounded(
                        result,
                        result_type,
                        *from,
                    ) {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    Ok(result)
                }
                NativeArgumentKernel::TimestampDiff {
                    stop_on_null,
                    argument_types,
                } => {
                    let mut owned = {
                        let source = store.borrow();
                        let get = |i| {
                            source.clone_for_kernel(
                                token_for(i)?,
                                NativeValueRole::CharInput,
                                origin(i),
                            )
                        };
                        [get(0)?, get(1)?, get(2)?]
                    };
                    let cast = if *stop_on_null {
                        Ok(())
                    } else {
                        crate::tikv::timestamp_diff_cast_arguments_bounded(
                            self.context,
                            &mut owned,
                            argument_types,
                            usize::MAX,
                        )
                    };
                    cast.and_then(|()| {
                        crate::tikv::timestamp_diff_values_bounded(
                            &owned[0],
                            &owned[1],
                            &owned[2],
                            usize::MAX,
                        )
                    })
                }
                NativeArgumentKernel::Crypto {
                    operation: operation,
                    ..
                } => {
                    // PASSWORD warns before all coercion and NULL handling.
                    crate::tikv::crypto_value::before_body(*operation, self.context);
                    let values = store.borrow();
                    let first = inputs.get(&values, 0)?;
                    if arity == 1 {
                        crate::tikv::crypto_value::body(*operation, &[first], maximum)
                    } else {
                        let second = inputs.get(&values, 1)?;
                        crate::tikv::crypto_value::body(*operation, &[first, second], maximum)
                    }
                }
                NativeArgumentKernel::Compression {
                    operation: operation,
                    ..
                } => {
                    let report = {
                        let values = store.borrow();
                        crate::tikv::compression_value::body(
                            *operation,
                            inputs.get(&values, 0)?,
                            maximum,
                        )
                    };
                    let report = match report {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    report.warn(self.context);
                    store
                        .borrow()
                        .get(token_for(0)?, NativeValueRole::CharInput, origin(0))?;
                    report.value
                }
                NativeArgumentKernel::ByteUnary {
                    operation: operation,
                    ..
                } => {
                    let values = store.borrow();
                    crate::tikv::byte_unary_value::scalar_body(
                        *operation,
                        inputs.get(&values, 0)?,
                        maximum,
                    )
                }
                NativeArgumentKernel::Identity { name_const: _, .. } => {
                    let values = store.borrow();
                    crate::tikv::identity_actual::copy(inputs.get(&values, next)?)
                }
                NativeArgumentKernel::Grouping(metadata) => {
                    let values = store.borrow();
                    crate::tikv::grouping_value::eval(
                        inputs.get(&values, 0)?,
                        metadata.as_deref(),
                        maximum,
                    )
                }
                NativeArgumentKernel::Vector(operation) => {
                    let source = store.borrow();
                    let mut values = Vec::new();
                    reserve(&mut values, arity)?;
                    for i in 0..arity {
                        values.push(inputs.get(&source, i)?);
                    }
                    crate::tikv::vector_value::eval_source(*operation, &values, maximum)
                }
                NativeArgumentKernel::Misc {
                    operation: operation,
                    ..
                } => {
                    use tidb_query_expr::NativeMiscOperation as O;
                    match operation {
                        O::TidbShard | O::VitessHash => {
                            let borrowed = {
                                let values = store.borrow();
                                crate::tikv::misc_value::prepare_borrowed_hash(
                                    inputs.get(&values, 0)?,
                                    maximum,
                                )
                            };
                            let borrowed = match borrowed {
                                Ok(v) => v,
                                Err(e) => return self.native_failure(e),
                            };
                            if borrowed {
                                // Both original warning stages are empty for these actual
                                // kinds. Read the genuine zone in its original phase with
                                // NO store borrow, then reauthenticate before pure conversion.
                                let zone = self.context.time_zone();
                                let values = store.borrow();
                                crate::tikv::misc_value::hash_after_zone(
                                    *operation,
                                    inputs.get(&values, 0)?,
                                    &zone,
                                    maximum,
                                )
                            } else {
                                let owned = {
                                    let values = store.borrow();
                                    crate::tikv::identity_actual::copy(inputs.get(&values, 0)?)
                                };
                                let owned = match owned {
                                    Ok(v) => v,
                                    Err(e) => return self.native_failure(e),
                                };
                                crate::tikv::misc_value::hash(
                                    *operation,
                                    &owned,
                                    self.context,
                                    maximum,
                                )
                            }
                        }
                        O::BinToUuid => {
                            let callback_flag = if arity == 2 {
                                let values = store.borrow();
                                matches!(
                                    inputs.get(&values, 1)?,
                                    Datum::String(_) | Datum::Bytes(_)
                                )
                            } else {
                                false
                            };
                            let flag = if callback_flag {
                                let owned = {
                                    let values = store.borrow();
                                    crate::tikv::identity_actual::copy(inputs.get(&values, 1)?)
                                };
                                let owned = match owned {
                                    Ok(v) => v,
                                    Err(e) => return self.native_failure(e),
                                };
                                // Only string/bytes flags call Columns. Share actual string
                                // headers and detach demanded byte ownership before callbacks.
                                crate::tikv::misc_value::flag(
                                    *operation,
                                    Some(&owned),
                                    self.context,
                                    maximum,
                                )
                            } else if arity == 2 {
                                let values = store.borrow();
                                crate::tikv::misc_value::flag(
                                    *operation,
                                    Some(inputs.get(&values, 1)?),
                                    self.context,
                                    maximum,
                                )
                            } else {
                                crate::tikv::misc_value::flag(
                                    *operation,
                                    None,
                                    self.context,
                                    maximum,
                                )
                            };
                            let flag = match flag {
                                Ok(v) => v,
                                Err(e) => return self.native_failure(e),
                            };
                            // Reauthenticate the original payload after any flag callbacks.
                            let values = store.borrow();
                            crate::tikv::misc_value::binary(inputs.get(&values, 0)?, flag, maximum)
                        }
                        O::UuidToBin => {
                            let parsed = {
                                let values = store.borrow();
                                crate::tikv::misc_value::parse(inputs.get(&values, 0)?, maximum)
                            };
                            match parsed {
                                Err(e) => Err(e),
                                Ok(None) => Ok(Datum::Null),
                                Ok(Some(parsed)) => {
                                    let flag = if arity == 2 {
                                        let values = store.borrow();
                                        crate::tikv::misc_value::flag(
                                            *operation,
                                            Some(inputs.get(&values, 1)?),
                                            self.context,
                                            maximum,
                                        )
                                    } else {
                                        crate::tikv::misc_value::flag(
                                            *operation,
                                            None,
                                            self.context,
                                            maximum,
                                        )
                                    };
                                    flag.and_then(|flag| {
                                        crate::tikv::misc_value::swap(&parsed, flag, maximum)
                                    })
                                }
                            }
                        }
                        _ => {
                            let values = store.borrow();
                            crate::tikv::misc_value::unary(
                                *operation,
                                inputs.get(&values, 0)?,
                                maximum,
                            )
                        }
                    }
                }
            }
        };
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        let result = match result {
            Datum::Int(value)
                if matches!(
                    meta.kernel,
                    NativeArgumentKernel::TimestampDiff {
                        stop_on_null: true,
                        ..
                    }
                ) && meta
                    .result_type
                    .as_ref()
                    .is_some_and(FieldType::is_unsigned) =>
            {
                Datum::UInt(value as u64)
            }
            value => value,
        };
        let body_coerced = matches!(
            meta.kernel,
            NativeArgumentKernel::DurationConstruction { .. }
                | NativeArgumentKernel::Calendar { .. }
                | NativeArgumentKernel::Clock { .. }
                | NativeArgumentKernel::Epoch { .. }
                | NativeArgumentKernel::Tso { .. }
        );
        let result = if body_coerced {
            result
        } else if let Some(field) = &meta.result_type {
            match crate::tikv::coerce_result_bounded(meta.name(), field, result) {
                Ok(v) => v,
                Err(e) => return self.native_failure(e),
            }
        } else {
            result
        };
        if let Some(role) = meta.output_role {
            return self
                .finish_native_value(id, result, meta.output_node, role, row)
                .map(HostStep::Ready);
        }
        let output = match &meta.kernel {
            NativeArgumentKernel::TimestampDiff { .. } => {
                let result = crate::expression::apply_enum_set_integer_view(
                    result,
                    meta.result_type.as_ref(),
                );
                let bits = match crate::arg_eval_type::eval_int(&result) {
                    Ok(v) => v.map(|v| v as i64),
                    Err(e) => return self.native_failure(e),
                };
                VectorValue::from_scalar(&ScalarValue::Int(bits), 1)
            }
            NativeArgumentKernel::Extract { .. } => {
                let value = match result {
                    Datum::Null => None,
                    Datum::Int(v) => Some(v),
                    _ => return Err(binding("EXTRACT result differs from integer carrier")),
                };
                VectorValue::from_scalar(&ScalarValue::Int(value), 1)
            }
            NativeArgumentKernel::BoundedStaleness { .. } => {
                let value = match result {
                    Datum::Null => None,
                    Datum::Time(v) => Some(TikvDateTime::from_go_raw_like_go(v.go_raw())),
                    _ => return Err(binding("bounded result differs from temporal carrier")),
                };
                VectorValue::from_scalar(&ScalarValue::DateTime(value), 1)
            }
            NativeArgumentKernel::StrToDate { output } => {
                use tidb_query_expr::local::SqlTemporalOutput as Output;
                let value = result;
                let value = crate::expression::apply_enum_set_integer_view(
                    value,
                    meta.result_type.as_ref(),
                );
                match output {
                    Output::Value => {
                        return Err(binding("STR_TO_DATE native carrier lost output role"));
                    }
                    Output::Bytes => {
                        let bytes = match crate::arg_eval_type::eval_string_ref(&value) {
                            Ok(bytes) => bytes,
                            Err(error) => return self.native_failure(error),
                        };
                        let length = bytes.map_or(0, <[u8]>::len);

                        let mut output = ChunkedVecBytes::try_with_capacities(1, length)
                            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
                        output.push_ref(bytes);

                        VectorValue::Bytes(output)
                    }
                    Output::DateTime => {
                        let value = match value {
                            Datum::Null => None,
                            Datum::Time(value) => {
                                Some(TikvDateTime::from_go_raw_like_go(value.go_raw()))
                            }
                            _ => {
                                return Err(binding(
                                    "STR_TO_DATE temporal result differs from its carrier",
                                ));
                            }
                        };
                        VectorValue::from_scalar(&ScalarValue::DateTime(value), 1)
                    }
                    Output::Duration => {
                        let value = match value {
                            Datum::Null => None,
                            Datum::Duration(value) => Some(
                                TikvDuration::from_native_raw_parts(
                                    value.nanoseconds(),
                                    i8::try_from(value.fsp()).map_err(|_| {
                                        binding("date duration FSP is not representable")
                                    })?,
                                )
                                .map_err(|_| binding("date duration is not representable"))?,
                            ),
                            _ => {
                                return Err(binding(
                                    "STR_TO_DATE duration result differs from its carrier",
                                ));
                            }
                        };
                        VectorValue::from_scalar(&ScalarValue::Duration(value), 1)
                    }
                }
            }
            _ => return Err(binding("native argument result lost output role")),
        };
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(output))
    }
}

/// Only stores argument identities. Each read authenticates against the current
/// store, so no borrow or stale value survives a host callback.
struct ArgumentValues<'a> {
    store: &'a RefCell<NativeValueStore>,
    nodes: &'a [usize],
    tokens: &'a [Option<native_values::NativeValueHandle>; 3],
    row: InputRow,
}
impl ArgumentValues<'_> {
    fn get<'v>(&self, values: &'v NativeValueStore, index: usize) -> LocalResult<&'v Datum> {
        values.get(
            self.tokens[index]
                .as_ref()
                .ok_or_else(|| binding("native argument capability lost"))?,
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: self.nodes[index],
                row: self.row,
            },
        )
    }
    fn copy(&self, index: usize) -> LocalResult<Datum> {
        self.store.borrow().clone_for_kernel(
            self.tokens[index]
                .as_ref()
                .ok_or_else(|| binding("native argument capability lost"))?,
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: self.nodes[index],
                row: self.row,
            },
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_arguments_reject_wrong_kernel_and_do_not_replay_completed_body() {
        let field = projected(&native_bytes_type(), true).unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let store = RefCell::new(NativeValueStore::new().unwrap());
        store.borrow_mut().begin_window().unwrap();
        let token = store
            .borrow_mut()
            .insert_owned(
                Datum::Bytes(b"abc".to_vec()),
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 1, row },
            )
            .unwrap();
        for wrong_kernel in [true, false] {
            let catalog = HostCatalog::for_sql_batch(vec![(
                SqlBatchHostOperation::CryptoNative(tidb_query_expr::NativeCryptoOperation::Md5),
                HostSignature {
                    arg_types: vec![field.clone()].into_boxed_slice(),
                    return_type: field.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let metadata = [NativeHostMetadata::Arguments(NativeArguments {
                arguments: 1usize.into(),
                kernel: if wrong_kernel {
                    NativeArgumentKernel::ByteUnary {
                        operation: tidb_query_expr::NativeByteUnaryOperation::LTrim,
                        probe: None,
                    }
                } else {
                    NativeArgumentKernel::Crypto {
                        operation: tidb_query_expr::NativeCryptoOperation::Md5,
                        probe: None,
                    }
                },
                result_type: Some(native_bytes_type()),
                output_node: 0,
                output_role: Some(NativeValueRole::Output),
            })];
            let mut hosts = NativeBatchHosts::new(catalog, &metadata, &crate::NoColumns)
                .with_native_values(Some(&store));
            let mut context = EvalContext::default();
            let start = hosts.start(
                &mut context,
                HostInvocation {
                    slot,
                    row,
                    arg_types: std::slice::from_ref(&field),
                    return_type: &field,
                },
            );
            if wrong_kernel {
                assert!(start.is_err());
                continue;
            }
            let HostStart::Pending { task, .. } = start.unwrap() else {
                panic!("MD5 demands its input");
            };
            let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.to_vec())), 1);
            let reply = HostArgReply {
                index: 0,
                field_type: &field,
                values: &input,
            };
            let HostStep::Ready(output) = hosts.resume(&mut context, &task, reply).unwrap() else {
                panic!("one input completes MD5");
            };
            let ScalarValueRef::Bytes(Some(handle)) = output.get_scalar_ref(0) else {
                panic!("native result handle");
            };
            assert_eq!(
                store
                    .borrow()
                    .get(
                        handle,
                        NativeValueRole::Output,
                        NativeValueOrigin { node: 0, row }
                    )
                    .unwrap()
                    .go_bytes(),
                b"900150983cd24fb0d6963f7d28e17f72"
            );
            assert!(hosts.resume(&mut context, &task, reply).is_err());
        }
    }
}
