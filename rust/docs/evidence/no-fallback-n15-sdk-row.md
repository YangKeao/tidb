# N15 SDK scoped row demand

Local SDK DCO `cdd5c30` adds a scoped Fresh argument request to the existing
window HostFrame and an appended variadic Bytes window catalog operation.
Original SQL operations and metadata remain caller-owned. No evaluator callback,
second driver, invented row or replacement of a whole-vector cache is involved.

Three protocol tests pass, covering singleton/duplicate/2050 selections, original
row identities, repeated Fresh reads, intact complete-vector Reuse, later whole
cache misses, skipped batch hooks for scoped rows, bad requests/carriers, primary
error identity, panic/step-limit cleanup, reuse and row-role rejection.

Validation used `../tools/cargo-tikv test --release --locked --offline -j8 -p
tidb_query_expr --lib` with RUSTFLAGS empty and CARGO_BUILD_JOBS8:1144 pass,
exact inherited str_to_date failure,1 ignored. Repository make-run clippy over
all targets retains exact N9 warning-title Counter171 overall /89 expression
test warnings. Lib-only/test-only exploratory runs are not baseline substitutes.
Both pinned formatters pass the three changed SDK Rust files. The maintenance
guide records the protocol and ownership contract.

The compatible frontend's typed runtime4 tests include73872 comparisons and
cache/authentication/forwarding; its cached361 batch checks pass with the same
10 then-pending source guards. TiDB's remaining Source migrations are separate
work. This protocol checkpoint is local; overall task #4 continues.
