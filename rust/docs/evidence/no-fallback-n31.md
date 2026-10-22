# CONCAT and CONCAT_WS retain actual values and packet demand

A native value source now represents genuine optional declarations for valid
CONCAT/CONCAT_WS calls. It retains actual argument ordinals, kind, result
FieldType option and output role. No default SQL type or replacement expression
is introduced. Existing typed CONCAT sources keep their original wire fields.

The continuation consumes its pending state before conversion or callbacks,
uses the original coerce_str_bytes primitive under a finite work budget, owns
and charges only the demanded byte prefix, and follows the existing SDK
ConcatDemand. Packet limit and overflow callbacks run at the original points,
outside the value-store borrow. Inputs are authenticated again after either
callback, so a callback that resets the store cannot continue with stale
authority. Completion uses the existing pure SDK prepare_concat_args/execute
codec and kernel, without a nested Ready frame. Actual optional return coercion
follows that computation once.

The original operation scope can remain poisoned after a caught callback panic.
An existing preparation unwind guard preserves that state across suspended
arguments; a new read-only status query preserves the original error at the
original completion point. It executes no Ready worker. Normal SQL errors and
cancellation leave the scope reusable. Original scalar computation, string
helpers, argument policy, other host bodies and SDK code are unchanged.

Independent capture and rerun preceded production changes:

| Scope | Observations | Original digest |
| --- | ---: | --- |
| Optional metadata, packet limits and demand | 221184 | EE3A7F821B85B619 |
| All 149 values in all three operand positions | 128736 | D556346B4B92D109 |
| Valid arities 1/2/3/5/6/9, respecting CONCAT_WS minimum | 4752 | 344B99C07DBAD21D |

The first matrix reproduced admission refusal before migration. It covers all
16 declaration masks, string/integer/JSON output fields, real parameters,
deferred writes, NULL and late errors, packet limits 0/1/4/U64_MAX, ordinary
overflow, callback errors and panic, both flags, duplicate/empty selections,
output prefixes and repeated calls. Its historical digest remains unchanged.
220032 observations now use the native source without fallback. The remaining
1152 observations retain the existing guard for declared whole-operand parameter
profiles; their values and effects are still compared. That gap is explicit and
is not claimed complete.

Direct protocol checks cover Some/None results and both output roles, invalid
and stale tokens, wrong role/node/row/occurrence, 128 repeated cancellations at
each prefix, replay refusal, callback borrow release and generation reset,
resource refusal followed by NULL recovery on the same provider, and original
scope poisoning on unwind. Actual Projection covers packet limits, strict and
query modes, all declaration masks, real prepared parameters and user variables,
NULL skips, warnings, exact errors and serial output prefixes.

Two existing tests exposed integration boundaries. A small Int literal declared
as string is now representable without the former typed temporary; its obsolete
admission assertion now verifies the unchanged original result and no fallback.
Its preceding large-literal resource gate remains unchanged. Nested typed CONCAT
inside REGEXP_LIKE retains its original complete wire declaration; native token
wire fields are selected only for native value views.

A third apparent regression was a historical stateful witness crossing UTC
midnight. The unchanged d36af6ec control and this implementation independently
produced the same October 22 digest, with all 1639 field/value cohorts identical.
Duration-to-calendar return conversion reads the current date. The witness now
uses the existing development-only conversion-clock scope to replay its original
October 21 input, reproducing the exact original 472032/E1A862511376506D digest.
Values, raw comparisons, expected digest and production default clock behavior
are unchanged. No temporal output was normalized or rebaselined.

Final validation: frontend 2039 passed, exactly the inherited 3 failures,
95 ignored; all 498 batch tests passed and warnings remained 346 (131 frontend).
Targeted CONCAT checks: 28 passed. Projection: 34 passed. Expand: 6 passed with
64/B42FB8BE316E6C79 unchanged. Root lint, both supported rustfmt versions,
diff checks and the ownership checker passed. SDK ad73b11 is clean and unchanged,
so its N16 baseline remains valid.

The fixed inventory reports 1374 refusals across 25 names in 83088 scenarios.
This is a local inventory, not a global completion percentage. Evidence,
snapshots, original/current cohort controls and the checker are in the owner's
evidence/no-fallback/n31-* files. Whole-parameter profiles, other public entries,
remaining metadata/PB gaps, generic fallback removal, server correctness and final
performance validation remain unfinished. No push or release.
