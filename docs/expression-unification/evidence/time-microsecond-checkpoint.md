# TIME and MICROSECOND — time-microsecond-70

Round71 follows intdiv-decimal-69. Functional closure adds TIME and MICROSECOND:211/245, strict0;34 eligible remain and10 more are needed for221. Eight exclusive owners changed21 Rust files, including two new parser modules, and added13 tests. The209 prior family objects remain unchanged.

## Shared foundation, not a wire-parser substitution

The actual source parser is the private expression `time_fn/duration_parse.rs`, not the similarly named public datatype byte/nanosecond parser. Its original signed-i64 grammar, Unicode outer trim versus ASCII internal whitespace, day/colon/compact order, fractional rounding, maximum838:59:59.000000, fallback classification and FSP handling move into TiKV. The raw input's first-dot suffix byte count determines FSP before trimming. A successful grammar match retains the original supplied FSP; local fraction/rounding helpers retain their own clamping behavior. Ordinary arithmetic/panics are not silently replaced by SQL overflow or NULL.

The datetime fallback retains the original wide delimited domain and compact UTC guard. Compact parsing shares TiKV's existing width/year/fraction primitives plus the original native compact-clock absorption rules. The native public parser delegates this branch and deletes its three duplicate compact helpers; generic timezone carry, suffix, float and unrelated branches remain in place. The native non-Timestamp datetime validator also delegates a shared actual-core-field validator, preserving error kind/order, zero-date clock checks and the final9999 microsecond boundary; Timestamp policy remains untouched. The exact native byte-fraction parser, normalization and error type are shared too; native FspError is a public alias with identical variants/data/Display but shared nominal ownership. Empty input still precedes invalid FSP, signed byte prefixes and error payloads remain exact. Source review found that TiKV's existing wire rounder differs for leading-zero fractions and carry; it remains untouched, rather than becoming a silently incompatible replacement. Native grammar and public SDK now use one native-policy fraction core. This is not a claim that the full temporal type or every parser policy has migrated.

Two pure TiKV expression modules own duration parsing and its datetime fallback. Native value structs bridge actual fields; no callback into native parsing, host-preparsed answer, hidden origin or native algorithm fallback is introduced. Other datetime arithmetic/predicate/formatting consumers are not credited merely because a shared leaf changed.

## Evaluator boundaries

Three fixed unary profiles reuse Bytes/Int carriers and existing result kinds. Native MICROSECOND consumes actual coerced UTF8 text, parses with the shared source policy and returns an integer or actual NULL on parse failure. Native TIME returns a small strict computed report containing actual formatted text and parse-failure status. Native code only assembles the original diagnostic from its original source text after a reported parse failure, handles that warning, then returns the report's actual value. It neither parses nor supplies a fallback zero answer. True input NULL also invokes a worker.

TIME retains its string leaf; the existing typed result conversion to Duration stays separate. MICROSECOND forwards actual context through dispatch and PB. Its PB observed-NULL shim preserves NULL-before-arity, uncoerced prior values and unevaluated suffix children. TIME has no added ordinary PB admission.

Legacy MICROSECOND retains the original first-child typed duration reader, SQL-error folding versus infrastructure propagation and ignored extra children. Actual nullable raw i64 nanoseconds enter a worker using the already shared full-domain microsecond projection, including i64::MIN. Native only widens the result. CastTimeAsDuration is a different predicate, not SQL TIME, and is untouched.

No new carrier, role, driver, binding, result kind or cause type is planned. Unary argument plus call fits the existing factory allowance.

## Validation

Sixteen serialized launches are recorded in `../logs/time-microsecond-summary.txt`:13 nonzero green,2 unchanged old full-suite failures and1 retained zero-match wrong session target. The lifecycle source belongs to `--lib`, not `--test all`; only the command target was corrected, with no source/fixture/oracle changes. No new test failure or compile failure occurred. All13 added tests pass on their first matching gate; filters overlap.

CPP datatype1/parser3/wrappers2/local322+1ignored pass. Native time SDK90/FSP14, expression7/parser1/calendar22/warning1/values1, legacy1 and SQL2 pass. SQL fixtures distinguish TIME(6) typed inputs from VARCHAR inputs whose existing result FSP is0:8 inputs x2 functions x2 modes=32 values, metadata/warnings plus8 direct zero-slot probes. The modes do not imply a new vector backend. No output recording or provider-derived expectations.

Full expression1565/4old/94ignored and unistore211/1old/13ignored retain entire failure sections/list byte-identically versus intdiv-decimal-69 after only numeric panic-thread IDs become THREAD. Normalized SHA256s are411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95 and2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1.

CPP228/native512 original test bodies and native Timestamp validation branch are byte-identical. An initial source-audit script timed out after20s due quadratic substring scanning; a linear token-mask audit completed. This was not a Cargo/test attempt. All21 Rust sources pass pinned rustfmt checks and both repository diff checks; dependencies/locks/generated/Go/Bazel and original fixtures are unchanged. Architecture and coprocessor guides describe the new ownership without introducing policy. Explicit non-verification below remains in force.

## Deferred scope

Strict audit stays0. Full temporal parser/type migration, arbitrary invalid-raw parity, complete allocator/fault/peak accounting, performance neutrality, whole workspace/lint/dev/bazel_prepare/release/exhaustive differential/TiFlash/FIPS and M6/default-NoColumns remain unclaimed. Prior INTDIV raw-empty/nonzero arithmetic exception, JSON_KEYS, AST arity, SQL metadata, parser/GB/vector and other recorded gaps remain explicit. No complete upstream-package transcreation or PR readiness is claimed.

Eight agents hold exclusive file leases; the parent owns validation, formatting, guides, manifests, Plan and publication. TiKV publishes first, then paired TiDB with identical Plan snapshots; no force push or PR, and the old untracked client-differential BUILD.bazel remains excluded.
