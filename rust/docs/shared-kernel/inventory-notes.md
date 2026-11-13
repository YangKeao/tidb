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
| Core implementation/test contingency (to reconcile the design 13–22K envelope) | 3650–4750 | 0 | previous table omitted this reserve; re-estimate as semantic scope is proven |
| Build/config, generated inventory and documentation | measured separately below | measured separately below | original 100–250 estimate withdrawn; these are part of total changes |
| Necessary algorithm movement and extra semantic differences | not yet bounded | not yet bounded | inspect unmatched/generic families before using this budget as a full estimate |

Per-family classification is intentionally provisional: `direct_reuse_candidate` currently selects MD5 only; `semantic_revision_candidate` says a matching original SDK function is available, not that every overload is equivalent or the revision is known small. `algorithm_migration_candidate` is the inspection queue, not an assertion of absence. Five exceptions remain explicit. File-level retirement cannot be promised before this queue is resolved; whole-file native algorithm moves are not authorized by a missing name match.

M1 proceeds with MD5, Decimal/NaN, lazy complex values, RAND/variables/BENCHMARK. A runnable checkpoint is not final full coverage. Report actual raw additions/deletions for each repository and production/tests/docs independently at every checkpoint.

## Checkpoint 1 measured overhead and revised estimate

The fixed checkpoint contains Cargo/build +1866/-91 (1957 changed lines), generated JSON +6569, generator +89 and prose/maintenance +152. Together these are +8676/-91 (8767 changed lines), separate from the 818 source/test lines. The previous +100–250 documentation/build estimate was wrong. Reserve **9–12K** changed lines for build, inventory and documentation, in addition to the design's **13–22K** core/integration/basic-test envelope: **22–34K** before additional algorithm movement, semantic extensions and retirement deletions. This is an estimate, not a demonstrated full-project bound. The unbounded algorithm/semantic queue must be priced before M2.

The generator now scans complete fixed-base files, conservatively retaining inline-test candidates rather than truncating production after the first test attribute. Candidate anchors can be tests or registrations and require human source review; they do not claim algorithm ownership. BETWEEN's production AST anchor is retained. The JSON records SHA256 of both the exact family-ledger input and the sorted family identities.

Reproduce from this repository root:

```sh
python3 rust/docs/shared-kernel/inventory.py \
  --families /home/agent/tidb/expression-unification/tidb/rust/docs/evidence/expression-unification-final-coverage.json \
  --tikv ../tikv --output rust/docs/shared-kernel/coverage-candidates.json
```

| Scope | Original budget (changed lines) | Revised budget | Checkpoint1 actual additions/deletions |
| --- | ---: | ---: | ---: |
| Listed production/integration/tests | 9350–17250 | 9350–17250 | +802/-16 |
| Core contingency omitted from original table | implicit in design13–22K | 3650–4750 | not independently spent |
| Core subtotal including contingency | design13–22K | 13–22K | +802/-16 |
| Build/config/maintenance, inventory included | 100–350 | 9–12K | +8676/-91 |
| Known combined scope | not reconciled | 22–34K | +9478/-107 |
| Algorithm moves, extra compatibility, retirement | unbounded | unbounded until M2 source audit | not credited as complete |

The contingency is inside13–22K, not an additional charge above it. The9–12K overhead is outside that core subtotal. Actuals refer only to the fixed first checkpoint; subsequent checkpoint reports record new totals.
