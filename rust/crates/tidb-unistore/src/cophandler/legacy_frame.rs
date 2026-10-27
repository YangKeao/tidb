// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine SimpleExpr consumers and native Shared children in one SDK program.
//! Source preparation and evaluation failures are terminal; there is no retry.
use super::legacy_frame_value::{Origin, Value, ValueError, Values, View};
use super::*;
use tidb_expr::{ComposedExpressionProgram, NativeFragmentMount, NativeSourceFragment};
use tidb_query_datatype::codec::data_type::{ScalarValue, VectorValue};
use tidb_query_datatype::expr::EvalContext;
use tidb_query_expr::local::{
    CompileLimits, ExecutionLimits, HostArgFailure, HostArgReply, HostArgRequest, HostCatalog,
    HostCatalogKey, HostInvocation, HostSignature, HostStart, HostStep, HostTaskId, InputRow,
    LocalError, LocalExpr, LocalHostServices, LocalResult, LocalRuntimeServices, OrdinaryCallSite,
    OrdinarySourceId, SqlBatchHostOperation,
};

#[derive(Debug)]
pub(super) enum PrepareError {
    Contract(&'static str),
    Native(tidb_expr::EvalError),
    Local(LocalError),
    Limit,
}
impl From<tidb_expr::EvalError> for PrepareError {
    fn from(error: tidb_expr::EvalError) -> Self {
        Self::Native(error)
    }
}
impl From<LocalError> for PrepareError {
    fn from(error: LocalError) -> Self {
        Self::Local(error)
    }
}
impl PrepareError {
    #[cfg(test)]
    fn message(&self) -> String {
        match self {
            Self::Contract(message) => (*message).into(),
            Self::Native(error) => format!("{error:?}"),
            Self::Local(error) => format!("{error}"),
            Self::Limit => "legacy source metadata limit".into(),
        }
    }
}

#[derive(Clone, Copy)]
enum Recipe {
    Leaf,
    FoldedInt,
    Shared(usize),
    IntBinary {
        signature: SimpleSig,
        eager_error: bool,
    },
    IntUnary(SimpleSig),
    Typed {
        signature: SimpleSig,
        nulls: NullDemand,
    },
    Condition(View),
    Null,
    ConstantInt(i128),
    EpochFormat,
    JsonMember,
    JsonModify {
        append: bool,
    },
    JsonLiteralNull,
    Date {
        metadata: tidb_expr::LegacyDateArithmeticMetadata,
        output: tidb_expr::LegacyDateArithmeticOutputKind,
    },
    In {
        collation: Option<i32>,
    },
}
#[derive(Clone, Copy)]
enum NullDemand {
    Never,
    First,
    Any,
}
enum Ports {
    Children(&'static [View]),
    All(View),
    SelfView(View),
    Dynamic([View; 3]),
    JsonPairs,
}
struct Node<'a> {
    original: &'a SimpleExpr,
    view: View,
    recipe: Recipe,
    children: Vec<usize>,
    original_arity: usize,
}
struct Native<'a> {
    original: &'a SharedExpression,
    fragment: Result<NativeSourceFragment, tidb_expr::EvalError>,
    mount: Option<NativeFragmentMount>,
    host_start: usize,
    input_start: usize,
}
pub(super) struct Plan<'a> {
    nodes: Vec<Node<'a>>,
    natives: Vec<Native<'a>>,
    catalog: HostCatalog,
    schema: Vec<sdk_tipb::FieldType>,
    program: ComposedExpressionProgram,
    width: usize,
}

fn frame_type() -> sdk_tipb::FieldType {
    tidb_datatype::tikv_compat::value::project_field_type(
        &tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Blob)
            .with_collation(tidb_datatype::Collation::Binary),
        -63,
    )
    .expect("closed private native-value carrier")
}

fn integer_recipe(signature: SimpleSig) -> Option<Recipe> {
    use SimpleSig::*;
    let eager_error = match signature {
        PlusInt
        | PlusIntUnsignedUnsigned
        | PlusIntUnsignedSigned
        | PlusIntSignedUnsigned
        | PlusIntSignedSigned
        | MinusInt
        | MinusIntUnsignedUnsigned
        | MinusIntUnsignedSigned
        | MinusIntSignedUnsigned
        | MinusIntSignedSigned
        | MinusIntForcedUnsignedUnsigned
        | MinusIntForcedUnsignedSigned
        | MinusIntForcedSignedUnsigned
        | MultiplyInt
        | MultiplyIntUnsigned
        | LtInt
        | LeInt
        | GtInt
        | GeInt
        | EqInt
        | NeInt => true,
        ModIntUnsignedUnsigned
        | ModIntUnsignedSigned
        | ModIntSignedUnsigned
        | ModIntSignedSigned
        | IntDivideInt
        | IntDivideIntUnsignedUnsigned
        | IntDivideIntUnsignedSigned
        | IntDivideIntSignedSigned
        | IntDivideIntSignedUnsigned
        | LogicalAnd
        | LogicalOr => false,
        CastIntAsInt | UnaryNot | IntIsNull | RoundInt => return Some(Recipe::IntUnary(signature)),
        _ => return None,
    };
    Some(Recipe::IntBinary {
        signature,
        eager_error,
    })
}

struct Graph {
    expression: LocalExpr,
    sites: Vec<OrdinaryCallSite>,
    count: usize,
}
impl<'a> Plan<'a> {
    pub(super) fn prepare(
        original: &'a SimpleExpr,
        view: View,
        width: usize,
    ) -> Result<Self, PrepareError> {
        let limits = CompileLimits::default();
        let maximum = ExecutionLimits::default().max_retained_bytes;
        let mut nodes: Vec<Node<'a>> = vec![];
        let mut natives = vec![];
        let mut pending = vec![(original, view, None::<usize>, 1usize)];
        let mut graph_nodes = 0usize;
        let mut metadata = 0usize;
        while let Some((original, view, parent, depth)) = pending.pop() {
            if depth > limits.max_depth || graph_nodes >= limits.max_nodes {
                return Err(PrepareError::Limit);
            }
            graph_nodes += 1;
            metadata = metadata
                .checked_add(std::mem::size_of::<Node<'a>>() + 4 * std::mem::size_of::<LocalExpr>())
                .filter(|n| *n <= maximum)
                .ok_or(PrepareError::Limit)?;
            let index = nodes.len();
            let original_arity = match original {
                SimpleExpr::Func(_, args) => args.len(),
                _ => 0,
            };
            let mut children = Vec::new();
            let recipe = if view == View::FoldedInt {
                pending.push((original, View::Int, Some(index), depth + 1));
                Recipe::FoldedInt
            } else {
                match original {
                    SimpleExpr::Shared(shared) => {
                        let native = natives.len();
                        let fragment =
                            NativeSourceFragment::prepare(Arc::clone(&shared.expression), width);
                        if let Ok(fragment) = &fragment {
                            graph_nodes = graph_nodes
                                .checked_add(fragment.node_count())
                                .filter(|n| *n <= limits.max_nodes)
                                .ok_or(PrepareError::Limit)?;
                            metadata = metadata
                                .checked_add(fragment.logical_metadata_bytes())
                                .filter(|n| *n <= maximum)
                                .ok_or(PrepareError::Limit)?;
                        }
                        natives.push(Native {
                            original: shared.as_ref(),
                            fragment,
                            mount: None,
                            host_start: 0,
                            input_start: 0,
                        });
                        Recipe::Shared(native)
                    }
                    SimpleExpr::Func(_, _) if view == View::Datum => Recipe::Leaf,
                    SimpleExpr::Func(signature, args) => {
                        let (recipe, ports) = match (view, integer_recipe(*signature)) {
                            (View::Int, Some(recipe)) => (
                                recipe,
                                Ports::Children(match recipe {
                                    Recipe::IntUnary(_) => &[View::Int],
                                    _ => &[View::Int, View::Int],
                                }),
                            ),
                            _ => typed::ports(*signature, view).ok_or(PrepareError::Contract(
                                "legacy source consumer recipe differs",
                            ))?,
                        };
                        match ports {
                            Ports::JsonPairs => {
                                let count = if args.is_empty() {
                                    0
                                } else {
                                    1 + (args.len() - 1) / 2 * 2
                                };
                                if count > limits.max_nodes.saturating_sub(graph_nodes) {
                                    return Err(PrepareError::Limit);
                                }
                                for (offset, child) in args.iter().take(count).enumerate().rev() {
                                    pending.push((
                                        child,
                                        if offset % 2 == 1 {
                                            View::Bytes
                                        } else {
                                            View::Json
                                        },
                                        Some(index),
                                        depth + 1,
                                    ));
                                }
                                children
                                    .try_reserve_exact(count)
                                    .map_err(|_| PrepareError::Limit)?;
                            }
                            Ports::Children(views) => {
                                for (child, view) in args.iter().zip(views).rev() {
                                    pending.push((child, *view, Some(index), depth + 1));
                                }
                                children
                                    .try_reserve_exact(views.len().min(args.len()))
                                    .map_err(|_| PrepareError::Limit)?;
                            }
                            Ports::All(view) => {
                                if args.len() > limits.max_nodes.saturating_sub(graph_nodes) {
                                    return Err(PrepareError::Limit);
                                }
                                for child in args.iter().rev() {
                                    pending.push((child, view, Some(index), depth + 1));
                                }
                                children
                                    .try_reserve_exact(args.len())
                                    .map_err(|_| PrepareError::Limit)?;
                            }
                            Ports::SelfView(view) => {
                                pending.push((original, view, Some(index), depth + 1))
                            }
                            Ports::Dynamic(views) => {
                                for (child, view) in args.iter().zip(views).rev() {
                                    pending.push((child, view, Some(index), depth + 1));
                                }
                                children
                                    .try_reserve_exact(3.min(args.len()))
                                    .map_err(|_| PrepareError::Limit)?;
                            }
                        }
                        recipe
                    }
                    SimpleExpr::Null
                        if view == View::Json
                            && parent.is_some_and(|p| {
                                matches!(nodes[p].recipe, Recipe::JsonModify { .. })
                                    && !nodes[p].children.is_empty()
                            }) =>
                    {
                        Recipe::JsonLiteralNull
                    }
                    _ => Recipe::Leaf,
                }
            };
            if let Some(parent) = parent {
                nodes[parent].children.push(index);
            }
            nodes.push(Node {
                original,
                view,
                recipe,
                children,
                original_arity,
            });
        }
        let carrier = frame_type();
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(nodes.len())
            .map_err(|_| PrepareError::Limit)?;
        for node in &nodes {
            let arity = match node.recipe {
                Recipe::Shared(index) => usize::from(natives[index].fragment.is_ok()),
                _ => node.children.len(),
            };
            entries.push((
                SqlBatchHostOperation::LegacyNative,
                HostSignature {
                    arg_types: vec![carrier.clone(); arity].into_boxed_slice(),
                    return_type: carrier.clone(),
                },
            ));
        }
        let mut schema = vec![];
        for native in &mut natives {
            if let Ok(fragment) = &native.fragment {
                if fragment.output_type() != &carrier {
                    return Err(PrepareError::Contract(
                        "native fragment result carrier differs",
                    ));
                }
                native.host_start = fragment.append_hosts(&mut entries)?;
                native.input_start = schema.len();
                schema.extend_from_slice(fragment.input_schema());
            }
        }
        let catalog = ComposedExpressionProgram::catalog(entries)?;
        for native in &mut natives {
            if let Ok(fragment) = &native.fragment {
                native.mount =
                    Some(fragment.mount(&catalog, native.host_start, native.input_start)?);
            }
        }
        let mut stack: Vec<Graph> = Vec::new();
        stack
            .try_reserve_exact(nodes.len())
            .map_err(|_| PrepareError::Limit)?;
        for (index, node) in nodes.iter().enumerate().rev() {
            let mut arguments = Vec::new();
            let mut sites = vec![OrdinaryCallSite::sql_native_batch(
                0,
                OrdinarySourceId::new(node.original as *const _ as usize as u64, node.view as u64),
            )];
            let mut count = 1usize;
            if let Recipe::Shared(native) = node.recipe {
                if let Ok(fragment) = &natives[native].fragment {
                    let (expression, nested) =
                        fragment.graph(natives[native].mount.as_ref().unwrap(), count)?;
                    sites.extend(nested);
                    count += fragment.node_count();
                    arguments.push(expression);
                }
            } else {
                for _ in &node.children {
                    let child = stack.pop().ok_or(PrepareError::Limit)?;
                    for site in child.sites {
                        sites.push(OrdinaryCallSite::sql_native_batch(
                            count + site.ordinal(),
                            site.source(),
                        ));
                    }
                    count += child.count;
                    arguments.push(child.expression);
                }
            }
            stack.push(Graph {
                expression: LocalExpr::HostCall {
                    slot: catalog.slot(index)?,
                    args: arguments.into_boxed_slice(),
                    return_type: carrier.clone(),
                },
                sites,
                count,
            });
        }
        let graph = stack.pop().ok_or(PrepareError::Limit)?;
        if !stack.is_empty() || graph.count != graph_nodes {
            return Err(PrepareError::Limit);
        }
        let program = ComposedExpressionProgram::compile_with_hosts(
            &graph.expression,
            &schema,
            graph.sites,
            &catalog,
        )?;
        Ok(Self {
            nodes,
            natives,
            catalog,
            schema,
            program,
            width,
        })
    }
}

#[path = "legacy_frame_service.rs"]
mod service;
#[cfg(test)]
#[path = "legacy_frame_tests.rs"]
mod tests;
#[path = "legacy_frame_typed.rs"]
mod typed;

/// One real borrowed root retained by a request across physical rows. The plan
/// is taken out before callbacks, so reentry never holds a RefCell borrow or
/// aliases a live SDK program. At most one idle plan is cached for this owner.
pub(super) struct PreparedRoot<'a> {
    source: &'a SimpleExpr,
    view: View,
    idle: std::cell::RefCell<Option<Plan<'a>>>,
}
struct ReturnPlan<'a, 'slot> {
    idle: &'slot std::cell::RefCell<Option<Plan<'a>>>,
    plan: Option<Plan<'a>>,
}
impl Drop for ReturnPlan<'_, '_> {
    fn drop(&mut self) {
        let mut idle = self.idle.borrow_mut();
        if idle.is_none() {
            *idle = self.plan.take();
        }
    }
}
impl<'a> PreparedRoot<'a> {
    pub(super) fn new(source: &'a SimpleExpr, view: View) -> Self {
        Self {
            source,
            view,
            idle: std::cell::RefCell::new(None),
        }
    }
    fn value(&self, evaluator: &LegacyEvaluator<'_>) -> LegacyResult<Value> {
        let idle = self.idle.borrow_mut().take();
        let plan = match idle {
            Some(plan) if plan.width == evaluator.row.len() => plan,
            _ => Plan::prepare(self.source, self.view, evaluator.row.len())
                .map_err(PrepareError::into_legacy)?,
        };
        let mut guard = ReturnPlan {
            idle: &self.idle,
            plan: Some(plan),
        };
        guard.plan.as_mut().unwrap().evaluate(evaluator)
    }
    pub(super) fn integer(&self, evaluator: &LegacyEvaluator<'_>) -> LegacyResult<Option<i128>> {
        match self.value(evaluator)? {
            Value::Int(value) => Ok(value),
            _ => Err(LegacyEvalError::InvalidResult(
                "prepared legacy integer view differs",
            )),
        }
    }
    pub(super) fn datum(
        &self,
        evaluator: &LegacyEvaluator<'_>,
    ) -> LegacyResult<tidb_datatype::Datum> {
        match self.value(evaluator)? {
            Value::Datum(value) => Ok(value),
            _ => Err(LegacyEvalError::InvalidResult(
                "prepared legacy datum view differs",
            )),
        }
    }
}
