# Reuse the unchanged TiKV expression engine in Rust TiDB

## Purpose

Rust TiDB should bind SQL expressions to the same RPN engine and existing protobuf kernels that TiKV uses. The TiKV checkout remains at `548812e1ef57aef077a2062a9cc356640a6347f5`. The previous dual-repository candidate remains the behavior and performance comparison.

## Progress

- [x] Establish separate worktrees and verify the upstream dependency pins.
- [x] Compile a local projection through the upstream builder and `eval_decoded`.
- [x] Verify SQL values, short-circuit demand, error order, and execution state.
- [ ] Migrate reusable typed signatures through the catalog and separate TiDB-only callbacks from remote pushdown.
- [ ] Verify normal build features, built-source provenance, regression and performance comparisons.

## Surprises

The protobuf-to-callback mapper is private, while the protobuf builder can select the same callbacks publicly. The TiKV dependency supplies a global allocator even with jemalloc disabled. TiDB must use that allocator for binaries that link this dependency.

## Decision Log

- The program uses the upstream node types and evaluator. Short-circuit callbacks reenter that evaluator with the selected physical rows.
- The niladic RAND callback shares the statement's existing session RNG handle. The statement owns one generator per literal-seeded RAND occurrence, shared across its projection workers. Both avoid borrowing an ambient context through a pointer.

## Outcomes & Retrospective

A 97-row MySQL protocol witness matched 40 observations to the original Rust node. Both vector settings and initial chunk sizes 1 and 32 were read back. Source breakpoints confirmed the TiDB IF and RAND callbacks and the untouched TiKV HOUR and EXP callbacks. The witness caught and verified the repair for a seeded RAND reset at a projection worker batch boundary.

## Context and Orientation

`tidb-expr` owns local SQL expression binding, `tidb-executor` owns statement projection and session effect handles, and `tidb-unistore` consumes protobuf expressions in the local store. A remote TiKV server has its own upstream coprocessor. SQL-only callbacks and fixes bound within TiDB apply to the TiDB execution route.

## Plan of Work

Start with nested TIME and DOUBLE expressions on the normal projection path. Compare seeded and session RAND, dead errors, and order through multiple chunks. Use the existing typed catalog for the remaining applicable protobuf signatures and reuse TiKV validators and callbacks. Keep local semantic decisions narrowly tied to confirmed behavior. Cover the existing high-traffic local and in-process storage entry points.

## Concrete Steps

Run `cargo check --locked -p tidb-expr -p tidb-executor` from `rust`. Build `tidb-server` with and without default features. Run the real SQL witness on otherwise equivalent local instances and capture launch settings and results. Resolve Cargo's graph and compare the selected TiKV paths to the pinned Git tree.

## Validation and Acceptance

The same typed expressions return identical rows and diagnostics on the two comparison builds. Results identify actual chunk configuration, program reuse, and executed callback paths. Verify that the untouched TiKV still supplies the existing protobuf kernels and that the Rust TiDB diff stays under `rust/`. Benchmark matched workloads and toolchain configurations.

## Idempotence and Recovery

Work occurs in a separate branch. Rebuild and rerun the local SQL fixtures in new temporary work directories. Keep the previous candidate and recorded evidence available.

## Artifacts and Notes

The evidence directory holds command records, source patches, frozen-binary hashes and SQL observations.

## Interfaces and Dependencies

The compiler uses `RpnExpressionBuilder::build_from_expr_tree` for protobuf function selection. `RpnExpression::eval_decoded` is the interpreter, including within `ShortCircuitFnMeta` callbacks.
