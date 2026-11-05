// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine borrowed AST metadata lowered into the existing SDK batch program.
//! No source tree is cloned and no host evaluates an expression child.
use super::*;
use tidb_ast::Expr;

#[path = "batch_ast_demand.rs"]
pub(super) mod demand;
#[path = "batch_ast_value.rs"]
mod value;
pub(super) use demand::AstTask;

pub(crate) struct BorrowedAst<'a> {
    nodes: Vec<&'a Expr>,
    metadata: Vec<NativeHostMetadata>,
    catalog: HostCatalog,
    program: LocalBatchProgram,
    execution: ExecutionLimits,
}

/// Actual AST child positions. A ROW is flattened only for its real comparison
/// or IN consumer; sequence names and interval units remain parent metadata.
fn children(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Binary(_, l, r) if matches!((&**l, &**r), (Expr::Row(_), Expr::Row(_))) => {
            let (Expr::Row(l), Expr::Row(r)) = (&**l, &**r) else {
                unreachable!()
            };
            l.iter().chain(r).collect()
        }
        Expr::In { expr, list, .. } if matches!(&**expr, Expr::Row(_)) => {
            let Expr::Row(left) = &**expr else {
                unreachable!()
            };
            left.iter()
                .chain(list.iter().flat_map(|e| match e {
                    Expr::Row(items) => items.as_slice(),
                    _ => &[],
                }))
                .collect()
        }
        Expr::In { expr, list, .. } => std::iter::once(&**expr).chain(list).collect(),
        Expr::Func { name, args, .. } => {
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE"
            ) {
                if let [date, Expr::Interval { value, .. }] = args.as_slice() {
                    return vec![date, value];
                }
            }
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "NEXTVAL" | "LASTVAL" | "SETVAL"
            ) {
                return args.get(1).into_iter().collect();
            }
            args.iter().collect()
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => value
            .iter()
            .map(|e| &**e)
            .chain(when_clauses.iter().flat_map(|(a, b)| [a, b]))
            .chain(else_clause.iter().map(|e| &**e))
            .collect(),
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        Expr::Position { substr, str } => vec![substr, str],
        Expr::Trim { expr, remstr, .. } => std::iter::once(&**expr)
            .chain(remstr.iter().map(|e| &**e))
            .collect(),
        Expr::Binary(_, l, r) => vec![l, r],
        Expr::Like { expr, pattern, .. } | Expr::Regexp { expr, pattern, .. } => {
            vec![expr, pattern]
        }
        Expr::TimestampAdd { interval, expr, .. } => vec![interval, expr],
        Expr::CharsetBinary { value, .. }
        | Expr::Assign { value, .. }
        | Expr::Extract { value, .. } => vec![value],
        Expr::Unary(_, expr)
        | Expr::Paren(expr)
        | Expr::ConvertUsing { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Is { expr, .. }
        | Expr::GetFormat { expr, .. }
        | Expr::WeightString { expr, .. } => vec![expr],
        Expr::Cast(cast) => vec![&cast.expr],
        _ => Vec::new(),
    }
}
impl<'a> BorrowedAst<'a> {
    #[cfg(test)]
    pub(super) fn host_parts(&self) -> (&HostCatalog, &[NativeHostMetadata], &[&'a Expr]) {
        (&self.catalog, &self.metadata, &self.nodes)
    }

    pub(crate) fn compile(root: &'a Expr) -> std::result::Result<Self, EvalError> {
        Self::compile_with_limits(root, public_scalar_limits())
            .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))
    }
    fn compile_with_limits(
        root: &'a Expr,
        (limits, execution, _maximum): (BatchSourceLimits, ExecutionLimits, usize),
    ) -> Result<Self> {
        let mut nodes = Vec::new();
        let mut arguments: Vec<Vec<usize>> = Vec::new();
        let mut raw_strings = Vec::new();
        let mut pending = vec![(root, None::<usize>, false)];
        while let Some((expr, parent, raw_string)) = pending.pop() {
            let ordinal = nodes.len();
            // Check genuine combined arity overflow before materializing children.
            let _count = match expr {
                Expr::Func { args, .. } => args.len(),
                Expr::In { expr, list, .. } => list
                    .iter()
                    .try_fold(
                        match &**expr {
                            Expr::Row(v) => v.len(),
                            _ => 1,
                        },
                        |n, e| {
                            n.checked_add(match e {
                                Expr::Row(v) if matches!(&**expr, Expr::Row(_)) => v.len(),
                                _ => 1,
                            })
                        },
                    )
                    .ok_or_else(|| resource("AST IN width overflow"))?,
                Expr::Case {
                    value,
                    when_clauses,
                    else_clause,
                } => when_clauses
                    .len()
                    .checked_mul(2)
                    .and_then(|n| {
                        n.checked_add(
                            usize::from(value.is_some()) + usize::from(else_clause.is_some()),
                        )
                    })
                    .ok_or_else(|| resource("AST CASE width overflow"))?,
                Expr::Binary(_, l, r) => match (&**l, &**r) {
                    (Expr::Row(l), Expr::Row(r)) => l
                        .len()
                        .checked_add(r.len())
                        .ok_or_else(|| resource("AST ROW width overflow"))?,
                    _ => 2,
                },
                Expr::Between { .. } => 3,
                Expr::Position { .. }
                | Expr::Like { .. }
                | Expr::Regexp { .. }
                | Expr::TimestampAdd { .. } => 2,
                Expr::Trim { remstr, .. } => 1 + usize::from(remstr.is_some()),
                Expr::CharsetBinary { .. }
                | Expr::Assign { .. }
                | Expr::Extract { .. }
                | Expr::Unary(..)
                | Expr::Paren(_)
                | Expr::ConvertUsing { .. }
                | Expr::Collate { .. }
                | Expr::Is { .. }
                | Expr::GetFormat { .. }
                | Expr::WeightString { .. }
                | Expr::Cast(_) => 1,
                _ => 0,
            };

            let children = children(expr);

            nodes
                .try_reserve(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            arguments
                .try_reserve(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            raw_strings
                .try_reserve(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            raw_strings.push(raw_string);
            nodes.push(expr);
            arguments.push(Vec::new());
            if let Some(parent) = parent {
                arguments[parent].push(ordinal);
            }
            pending
                .try_reserve(children.len())
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            for (position, child) in children.into_iter().enumerate().rev() {
                let raw_string = matches!(expr,Expr::Func { name,args,.. } if name.eq_ignore_ascii_case("CHAR_FUNC") && position+1==args.len() && matches!(child,Expr::RawString(_)));
                pending.push((child, Some(ordinal), raw_string));
            }
        }
        // This Bytes declaration is the private capability ABI, never a SQL type
        // attached to a parsed AST node or a named column.
        let carrier = projected(&native_bytes_type(), true)?;
        let catalog = HostCatalog::for_sql_batch(
            arguments
                .iter()
                .map(|args| {
                    (
                        SqlBatchHostOperation::AstNative,
                        HostSignature {
                            arg_types: vec![carrier.clone(); args.len()].into_boxed_slice(),
                            return_type: carrier.clone(),
                        },
                    )
                })
                .collect(),
        )?;
        let mut graphs: Vec<Option<LocalExpr>> = (0..nodes.len()).map(|_| None).collect();
        for node in (0..nodes.len()).rev() {
            graphs[node] = Some(LocalExpr::HostCall {
                slot: catalog.slot(node)?,
                args: arguments[node]
                    .iter()
                    .map(|&child| graphs[child].take().expect("postorder genuine AST child"))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                return_type: carrier.clone(),
            });
        }
        let graph = graphs[0].take().expect("genuine root");
        let facts = BatchFacts::sql_native_batch_with_hosts(
            &graph,
            &[],
            nodes
                .iter()
                .enumerate()
                .map(|(ordinal, node)| {
                    OrdinaryCallSite::sql_native_batch(
                        ordinal,
                        OrdinarySourceId::new(
                            root as *const Expr as u64,
                            *node as *const Expr as u64,
                        ),
                    )
                })
                .collect(),
            limits.tree,
            &catalog,
        )?;
        let program = compile_batch(
            &graph,
            &[],
            LocalCompileContext {
                limits: limits.tree,
            },
            &facts,
        )?;
        let metadata = arguments
            .into_iter()
            .enumerate()
            .map(|(node, arguments)| NativeHostMetadata::Ast {
                node,
                arguments: arguments.into_boxed_slice(),
                raw_string: raw_strings[node],
            })
            .collect();
        Ok(Self {
            nodes,
            metadata,
            catalog,
            program,
            execution,
        })
    }
    pub(crate) fn eval(&mut self, context: &dyn Columns) -> std::result::Result<Datum, EvalError> {
        self.execute(context)
            .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke))
    }
    fn execute(&mut self, context: &dyn Columns) -> Result<Datum> {
        let store = RefCell::new(NativeValueStore::new()?);
        store.borrow_mut().begin_window()?;
        let mut hosts = NativeBatchHosts::new(self.catalog.clone(), &self.metadata, context)
            .with_ast_nodes(&self.nodes)
            .with_native_values(Some(&store));
        struct NoPhysicalSource;
        impl NativeDatumSource for NoPhysicalSource {
            fn schema(&self) -> &[tipb::FieldType] {
                &[]
            }
            fn read(
                &mut self,
                _: &mut EvalContext,
                _: usize,
                _: InputRow,
                _: &tipb::FieldType,
            ) -> LocalResult<Datum> {
                Err(binding("AST has no physical input slots"))
            }
        }
        let mut source = NoPhysicalSource;
        let mut bindings = CheckedInputs {
            source: &mut source,
            hosts: Some(&mut hosts),
        };
        let result = self.program.eval_cursor_with_bindings(
            self.execution,
            &mut EvalContext::default(),
            0,
            &mut bindings,
        );
        drop(bindings);
        if let Some(error) = hosts.take_native_error() {
            return Err(error.into());
        }
        let VectorValue::Bytes(output) = result? else {
            return Err(binding("AST output carrier differs").into());
        };
        if output.len() != 1 {
            return Err(binding("AST output occurrence count differs").into());
        }
        let value = store
            .borrow()
            .get(
                output
                    .get(0)
                    .ok_or_else(|| binding("AST output is wire NULL"))?,
                NativeValueRole::Output,
                NativeValueOrigin {
                    node: 0,
                    row: InputRow {
                        input_row: 0,
                        occurrence: 0,
                    },
                },
            )?
            .clone();
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn genuine_ast_compile_limits_precede_effects_and_keep_dead_sql_errors_late() {
        for sql in ["7", "IF(1,7,unregistered_ast_function())"] {
            let root = crate::ast_source_tests::parsed(sql);
            let mut plan = BorrowedAst::compile_with_limits(
                &root,
                (
                    BatchSourceLimits {
                        tree: CompileLimits {
                            max_nodes: 0,
                            max_depth: 0,
                        },
                        max_metadata_bytes: 0,
                    },
                    ExecutionLimits {
                        max_steps: 0,
                        max_frame_depth: 0,
                        max_active_tasks: 0,
                        max_retained_bytes: 0,
                    },
                    0,
                ),
            )
            .unwrap();
            assert_eq!(plan.eval(&crate::NoColumns).unwrap(), Datum::Int(7));
        }
        let root = tidb_ast::Expr::String("x".repeat(2048));
        let (mut limits, execution, _) = public_scalar_limits();
        limits.max_metadata_bytes = 0;
        let mut plan = BorrowedAst::compile_with_limits(&root, (limits, execution, 0)).unwrap();
        assert_eq!(
            plan.eval(&crate::NoColumns).unwrap().sql_string().unwrap(),
            "x".repeat(2048)
        );
    }
    #[test]
    fn genuine_ast_execution_resource_refusal_is_terminal_and_owner_can_be_reused() {
        let root = crate::ast_source_tests::parsed("7");
        let mut plan = BorrowedAst::compile(&root).unwrap();
        plan.execution = ExecutionLimits {
            max_steps: 0,
            max_frame_depth: 0,
            max_active_tasks: 0,
            max_retained_bytes: 0,
        };
        for _ in 0..2 {
            assert_eq!(plan.eval(&crate::NoColumns).unwrap(), Datum::Int(7));
        }
    }
    #[test]
    fn genuine_ast_metadata_only_rows_names_and_raw_charset_obey_source_budgets() {
        let cases = [
            (
                "row wrappers",
                Expr::In {
                    expr: Box::new(Expr::Row(vec![])),
                    list: (0..4).map(|_| Expr::Row(vec![])).collect(),
                    not: false,
                },
                Some(3),
            ),
            (
                "raw charset",
                Expr::Func {
                    name: "CHAR_FUNC".into(),
                    args: vec![Expr::Int("1".into()), Expr::RawString("x".repeat(8192))],
                    origin_position: 0,
                },
                None,
            ),
            (
                "sequence path",
                Expr::Func {
                    name: "NEXTVAL".into(),
                    args: vec![Expr::Column(vec!["x".repeat(8192)])],
                    origin_position: 0,
                },
                None,
            ),
        ];
        for (label, root, nodes) in cases {
            let (mut source, execution, maximum) = public_scalar_limits();
            if let Some(nodes) = nodes {
                source.tree.max_nodes = nodes;
            } else {
                source.max_metadata_bytes = 4096;
            }
            assert!(
                BorrowedAst::compile_with_limits(&root, (source, execution, maximum)).is_ok(),
                "{label}"
            );
        }
    }
    #[test]
    fn genuine_ast_char_charset_is_a_real_sdk_child_with_its_original_consumer() {
        let root = crate::ast_source_tests::parsed("CHAR(65 USING latin1)");
        let mut plan = BorrowedAst::compile(&root).unwrap();
        let Expr::Func { args, .. } = &root else {
            panic!("actual parsed function");
        };
        let NativeHostMetadata::Ast { arguments, .. } = &plan.metadata[0] else {
            unreachable!()
        };
        assert_eq!(arguments.len(), args.len());
        let child = *arguments.last().unwrap();
        assert!(std::ptr::eq(plan.nodes[child], args.last().unwrap()));
        assert!(matches!(
            plan.metadata[child],
            NativeHostMetadata::Ast {
                raw_string: true,
                ..
            }
        ));
        let old = (|| crate::eval_in(&root, &crate::NoColumns))().unwrap();

        assert_eq!(plan.eval(&crate::NoColumns).unwrap(), old);
        let root = Expr::RawString("latin1".into());
        let mut plan = BorrowedAst::compile(&root).unwrap();
        assert!(matches!(
            plan.metadata[0],
            NativeHostMetadata::Ast {
                raw_string: false,
                ..
            }
        ));
        assert_eq!(
            plan.eval(&crate::NoColumns).unwrap_err(),
            EvalError::Unsupported("unsupported expression")
        );
    }
}
