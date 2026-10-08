# Expression unification experiment

Current paired checkpoint **m6-crypto-fips-187**. Functional **240/245 (97.96%)**, strict **0**; five explicit exceptions remain.

The experiment's AES and SHA1 primitives now use the workspace-approved OpenSSL dependency, resolving their cargo-deny violations. Vitess keeps one named Rust DES wrapper solely for its fixed-key non-security shard hash because OpenSSL 3's default provider rejects DES-ECB. Crypto vectors and focused clippy pass. Full `make clippy` passes cargo-deny but later fails compiling old grpcio/Abseil with the current C++ toolchain. [Receipts](logs/m6-crypto-fips-summary.txt).

TiDB `make lint` passed in R192. Historical full-Rust failures, broad dev/release gates and final lifecycle/type acceptance remain active.
