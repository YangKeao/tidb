# Expression unification experiment

Current paired checkpoint **m6-gates-186**. Functional **240/245 (97.96%)**, strict **0**; five explicit exceptions remain.

M6 gate status: TiDB `make lint` passed. Full Rust libraries retained the known four `tidb-expr` and one Unistore failures. TiKV `make clippy` reached cargo-deny but failed because experimental `tidb_query_crypto` directly depends on RustCrypto AES/cipher/SHA1 crates banned by the existing FIPS policy. No deny bypass was added. [Exact receipts](logs/m6-gates-summary.txt).

Ledger, exception set, relative links and diff checks pass. PR readiness, FIPS-compatible crypto ownership, broad dev/release/performance and lifecycle/type final acceptance remain active.
