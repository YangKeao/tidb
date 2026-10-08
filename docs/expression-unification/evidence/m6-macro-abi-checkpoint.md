# Downstream RPN macro ABI

`m6-macro-abi-210` closes the executor test-compilation failure disclosed at the previous checkpoint.

`rpn_fn` expands inside downstream crates and must construct `RpnFnMeta` plus invoke generated shape validators. The shared descriptor refactor had made that macro surface crate-private. The minimum required symbols—`CallShape`, `CallArg`, `CallBuild`, their read-only shape accessors, validation helpers, and the two `RpnFnMeta` construction fields—are now `doc(hidden)` public. Metadata contents, mutation APIs, preparation and selection remain internal. This is a proc-macro ABI, not a second consumer-facing expression API.

The full executor library now passes 120 tests and the aggregate library passes 40. Focused selection, projection, error, strict-depth and production compilation gates are also GREEN. The original RED receipt remains retained. [Receipts](../logs/m6-macro-abi-summary.txt).
