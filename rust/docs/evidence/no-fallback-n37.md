# WEIGHT_STRING retains original padding and value demands

Positive-arity ordinary WEIGHT_STRING now uses a small consuming state inside
EagerValuesNative, with no SDK operation, expression callback, replacement tree,
Ready expression worker or second driver. Its original padding conversion,
absent-source signature default and string/collation value boundaries are shared
with ScalarFunction. The existing packet/key kernels and ReadyValueCache remain
unchanged. Source fields stay genuinely optional in the compiled descriptor;
only the original value owner retains its intrinsic VarString default.

For logical arity3, the frame requests kind1 then length2, converts padding only
after both reads, and requests value0 only when the original signature requires
it. A genuine numeric source returns through the existing numeric NULL helper
unless AS BINARY overrides it. Other positive arities consume only value0, or no
values for a numeric source. Logical arity and data arity0/1/3 remain distinct;
unused subtrees retain their real bounded shape but need no value binding.
Original constant-first name probes, source explicit collation, connection
charset, packet callbacks, warnings and result-family conversion retain their
original boundaries. Present source fields are borrowed rather than copied per
row. No declaration is inferred from a Datum or caller output column.

Independent original capture/rerun and source Admission failures preceded the
production change. Both fixed witnesses now pass strictly without fallback or
resource exemptions:

| Witness | Observations | Original digest |
| --- | ---: | --- |
| Arity1/2/3/4/6, five source shapes, optional fields and callback phases | 144000 | A5235AE1014C7EBB |
| Eight declared domains, all149 actual values in each of three positions | 214560 | AA5EFD1A5E872C25 |

The observations include exact outcomes/panics, raw output bytes and published
prefixes, parameter reads, deferred variable writes, connection/packet/cache
getter order, warnings, prepared worker counts, both flags, ordinary/duplicate/
empty selections, failures and same-owner/context reuse. Original snapshots,
pinned tests, records, all unsuccessful intermediate checks and the final source
ownership checker live in the owner's evidence/no-fallback/n37-* artifacts.

The first implementation exposed the existing host's fixed first request0 and
the distinction between the original suite's once-only cache binding and the
batch route's lazy fallback binding. The new phase now supplies first request1.
A boolean derived once from actual compiled WEIGHT_STRING metadata selects the
original suite cache binding before flags/children, including its unwind guard;
the original suite scheduling/publication body is unchanged. Other owners keep
their existing binding policy. A subsequent arity check was corrected to compare
actual data arity rather than logical argument count. No witness or digest was
changed to accept these failures.

Direct protocol checks reject stale, wrong-role/node/row and wire-NULL replies at
all three phases, plus cancelled/completed replies. A16KiB kind is retained only
as a capability until length is demanded, then rejected at an8KiB host limit;
the same host recovers at the same limit. Connection and packet callbacks acquire
the arena, invalidate its epoch or panic; no Ref survives them, replies are
reauthenticated, and work scope is released. Three same-owner/context checks
reject32KiB at16KiB and recover with NULL at the unchanged cap. Another180 cases
span0/3/2051 selected rows, duplicates, late parameter failure and four alternating
flag rounds. Actual numeric zero-data calls skip invalid unbound columns.

Real Projection adds genuine prepared parameters and deferred variable writes,
six source/arity profiles, five actual value triples, present/absent declarations,
query/strict contexts, both flags,0/3/2051 rows and serial/parallel workers. Caller
columns and complete raw output, warnings, errors and effects match the original.

Final gates: frontend2093 passed, exactly the inherited3 failures,95 ignored;
all552 batch checks passed and aggregate warnings remained346. Projection40,
Expand6 with original64/B42FB8BE316E6C79, make lint, both supported rustfmt versions
and ownership reconstruction passed. Original common classification, argument
policies, observers, packet/cache/kernel implementations and SDKad73b11 remain
unchanged, so the unchanged SDK's N16 full/clippy baseline is reused. No push or
release. The bounded83088-case inventory now refuses128 cases across7 names,
comprising108 regexp/regexp_like cases and20 declared-string whole-parameter
cases. Public scalar/AST/filter/unistore/PB entry closure, generic fallback removal,
full service correctness and final performance validation remain unfinished.

Reproduce from the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib weight_source_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The full frontend intentionally exits101 for the same inherited three failures:
ifnull_string_column_literal_uses_go_signature_and_column_collation,
builtin_info_json_math_source::exp and builtin_math_misc_op_source::vectorized_builtin_op_func.
