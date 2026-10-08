# M6 full-clippy compatibility closure

`m6-full-clippy-213` closes the workspace gate without editing vendored dependencies.

The reproducible compatibility environment uses GCC 14, force-includes `cstdint`, and wraps CMake 4 so only configure invocations receive `-DCMAKE_POLICY_VERSION_MINIMUM=3.5`. This lets old c-ares, RocksDB and Abseil build while retaining the pinned Rust toolchain. The wrapper dispatches `--build`, `--install`, `--version`, and `-E` unchanged; other calls execute `/usr/bin/cmake "$@" -DCMAKE_POLICY_VERSION_MINIMUM=3.5`. The root command sets `CARGO_TARGET_DIR=<experiment>/target-tikv-gcc14 CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE=<wrapper>` plus the pinned `CARGO_HOME`/`RUSTUP_HOME`, then runs `make clippy`. Full repository clippy exits 0 after all policy gates and workspace targets.

The first complete Rust lint exposed migration-owned debt rather than an environment error. Test-only crate attributes preserve compatibility fixture bodies that intentionally assert only success/error class; production targets retain the repository deny. Function-local allows preserve the exact Go EXP/LOG10 constants instead of replacing them with different rounded Rust constants.

Clippy also found a material flate transcription bug: `find_match` computed a Go chain budget but never decremented it. The fix restores the decrement. A cyclic-chain regression starts one test and times out without the fix, then passes after restoration; COMPRESS and exact Go math goldens remain GREEN.

M6 now has both the diagnostic performance record and a GREEN full-workspace clippy receipt. Overall goal completion is withheld until final invariants and an independent readiness review are rerun. [Receipts](../logs/m6-full-clippy-summary.txt).
