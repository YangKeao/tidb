# Original-baseline coverage and cost inventory

The adjacent JSON contains exactly245 original family identities, including the five explicit service/compatibility exceptions. The generator reads original-base source declarations and records candidates, not proof of compatibility. A family lacking a match may have a generic or differently named implementation. No previous experiment's functional credit transfers to this worktree. The ledger identity is frozen, but final classification requires each family's demand, coercion, result shape, collation, warnings and errors to be reviewed as it migrates.

## Entry points and retirement boundaries

| Original entry | Shared replacement | Retirement condition |
| --- | --- | --- |
| tidb-expr Expression::eval / ScalarFunction::eval | compile native structure into RPN; invoke shared evaluator | full native type/signature coverage; keep structural expression APIs |
| tidb-expr lib.rs eval_in / AST branches | AST builder lowers into the same program | AST-specific casts and syntax validated; delete evaluator branches |
| tidb-expr scalar_function.rs vector methods / evaluator.rs | shared multi-root and row/column group execution | scalar vs vector demand and effects preserved |
| simple_expr.rs / pb_predicate.rs / distsql_builtin.rs | protobuf/native build adapters into the same program | preserve distinct original PB eager behavior |
| executor projection/filter/sort/aggregate and Unistore | compile once; provide rows, context and output | callers and selection/effects tests pass |
| TiKV RpnExpressionBuilder / expr_eval.rs | extend original representation and loop | protobuf default tests preserve original contract |
| native scalar functions / builtin_ext / math_fn / string_fn / time_fn | existing SDK typed functions with semantic changes or necessary moved algorithms | remove only replaced expression algorithms, preserve storage/type APIs |

## File-level work budget (estimates, not measured diffs)

The numbers below are ranges of added/deleted lines, not net growth. Tests are included separately. They apply across both repositories, not per file. Algorithm migration and later deletions are additional to the core budget; unchanged algorithm movement counts both deletion and addition. No claim of a hard10K total cap is made.

| Files / scope | Add | Delete | Basis / remaining uncertainty |
| --- | ---: | ---: | --- |
| TiKV types/expr.rs, expr_builder.rs, expr_eval.rs and typed demand/state registration | 1800–3000 | 300–700 | flat region/call scheduling, retaining vector kernels; replace nested control scheduling |
| TiKV datatype scalar/vector bridge + exact Decimal accessors | 400–900 | 50–150 | full-domain floating carrier and exact parts, not storage migration |
| TiDB shared_kernel module, native/PB builders and input/context adapters | 2500–4500 | 0–300 | function mapping, input/context/output only; no host task protocol |
| TiDB evaluator/caller integration and deletion of superseded scheduling | 500–1000 | 2000–3500 | native AST/scalar/vector paths and execution groups; requires full coverage |
| Targeted shared integration tests in both repositories | 1800–3000 | 0–200 | boundary matrix plus original tests, no generated copies of existing suites |
| Build/config and maintenance documentation | 100–250 | 0–100 | docs counted separately in actual accounting |
| Necessary algorithm movement and extra semantic differences | not yet bounded | not yet bounded | inspect unmatched/generic families before using this budget as a full estimate |

Per-family classification is intentionally provisional: `direct_reuse_candidate` currently selects MD5 only; `semantic_revision_candidate` says a matching original SDK function is available, not that every overload is equivalent or the revision is known small. `algorithm_migration_candidate` is the inspection queue, not an assertion of absence. Five exceptions remain explicit. File-level retirement cannot be promised before this queue is resolved; whole-file native algorithm moves are not authorized by a missing name match.

M1 proceeds with MD5, Decimal/NaN, lazy complex values, RAND/variables/BENCHMARK. A runnable checkpoint is not final full coverage. Report actual raw additions/deletions for each repository and production/tests/docs independently at every checkpoint.
