# Private caller checkpoint: caller-private-pool-02

This validates private foundations, not SQL activation or a complete migrated family. The frozen denominator remains245 and completed families remain0. TiKV product sources are unchanged from84cf2396; its paired commit updates only the Plan.

## Included changes

- Private `tikv/ready_value.rs` and its tests: real C4 workers, root-stable bounded reservation ledger, explicit scopes and63 existing Columns forwarders. No general evaluator hook, native fallback, capability discovery or native ASCII deletion.
- Two independently identified correctness defects, both actually RED before the repair: inconsistent epoch/debt observation and cached reuse after an accounting-mutex poison that had not yet reached the separate atomic flag. Parent changed only `check_epoch` for the repair; subsequent source edits only clarified two accounting comments.
- Private `tikv/runtime_failure.rs`: original LocalError moved into opaque Arc, identity equality, native-only fixed six-class diagnostics and explicitly optional phase. No public EvalError/SQL renderer wiring; pool/bridge/frontend errors retain separate origins.
- Source-only GNU allocation observer and bounded Python runner. Rust's unsafe-code prohibition and inherited allocator remain unchanged.

## Actual validation

Commands below ran from `/home/agent/tidb/expression-unification/tidb/rust`; the absolute wrapper selects the Aug2026 compiler and separate TEST artifacts:

```sh
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib tikv::evaluated_ascii::tests::structural_ -- --test-threads=1 --nocapture
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib tikv::evaluated_ascii::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib tikv::runtime_failure::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

- Structural filter BEFORE repair:1 passed,2 new failures,1435 filtered, exit101. Cached NULL actually entered eval_one and advanced the official worker counter1→2. The inconsistent state was actually accepted. These are structural caller-state regressions using real workers, not natural C4 corruption.
- Same filter AFTER repair:3 passed,1435 filtered, exit0. The poison refusal reaches no eval_one call site; destroyed workers have no observable getter, not a fabricated zero counter.
- Caller suite:30 passed,1 ignored,1407 filtered. Opaque carrier:7 passed,1438 filtered.
- Final combined cohort:1347 passed,4 original failures,94 ignored, exit101. Complete failure bodies equal the preceding K baseline after normalizing only numeric thread IDs. The ignored allocation fixture was separately executed by the observer, but the other93 ignored tests remain unverified.
- Scoped pinned rustfmt --check passed. Independent A review closed both caller findings and found no additional source issue in the opaque carrier. No build/test is attributed to reviewers.

## Final actual Arc request observation

Observer source SHA: eff83df6af640b0ce21789f2ff4d2c86b61151d0e898f5ec8e428fad1deee18e.
Final TEST executable SHA: ddb196e81f054e5fff33659c7d0a0e135bf8e37fe94f11ddafb5bc62c54b7a9b.
SO SHA: e6fd53441eaac42c80c2927617150e2da882797276c2eaf514fd65f72f5705d7.

From `/home/agent/tidb/expression-unification`:

```sh
/usr/bin/gcc-14 -std=gnu11 -O2 -Wall -Wextra -Werror -fPIC -shared -fno-builtin -fno-stack-protector -ftls-model=initial-exec -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -Wl,-z,now -Wl,-z,defs -o tools/pool-arc-observer.so tools/pool-arc-observer.c
python3 tools/pool-arc-runner.py --binary target-tidb/debug/build/tidb-expr/1a66296e036585b2/out/tidb_expr-1a66296e036585b2 --observer tools/pool-arc-observer.so --output logs/tidb-pool-arc-final
```

All eight fresh processes actually ran the one exact fixture and exited0. Each yielded14 markers/16 events: seven inherited-Rust positive-control events before and after, zero empty/64-clone-window traffic, one real production-owner malloc192 and its matching final free. No gap/foreign events were observed. The observer does not parse or compare the proxy size. Each positive group observed malloc73, realloc149, matching free, calloc91×1, matching free, posix_memalign64/320 and matching free. The320 bytes include the safe aligned Box's padding.

The same final ELF's non-fixture control actually passed one normal test but supplied no markers: observer INVALID/UNPROVEN and exit86, not false zero-allocation success. A20-second runner deadline uses separate process groups and kill/reap; timeout is inconclusive. All actual runs finished normally. `logs/tidb-pool-arc-final/{receipt,cohort}.json` bind final source, runner, executable, SO and resolved dynamic-library hashes. C independently checked the final receipts without modifying or executing anything.

This accepts192 requested bytes for this exact GNU/Linux x86-64/glibc2.43/Aug2026 TEST cohort. Malloc has no explicit alignment argument; pointer residues are observations, not a requested-alignment proof. This does not establish portable Arc ABI, usable bytes, factory transient high-water, physical-OOM recovery or a whole-process heap bound. The snapshot's external-revalidation obligation remains true.

## Still open

Public Columns capability propagation, actual native panic-catcher placement, phase-aware EvalError/SQL1105 wiring, complete AST/SQL/PB/vector/fold/default/DML/join/sort/aggregate/window/helper/unistore activation, native deletion, baseline differential/source checks and release performance gates. Whole-workspace lint/clippy and final M6 acceptance have not passed. No PR is created and no family receives coverage credit.
