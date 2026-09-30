# Four packet-aware string families — packet-string-four-17

Functional delegation/native-deletion progress: 51/245; final acceptance: 0/245. SPACE, REPEAT, TO_BASE64 and FROM_BASE64 are four complete functional families on their existing admitted surfaces. The source cut covers seven TiDB and eight TiKV files, not completion of the overall unification target.

## Changes and domain coverage

Private packet recipes carry a typed allow/suppress disposition, represented by real flags0/1. Original warning/error1301 policy stays in the frontend, not a fabricated None or replacement Resource error; NULL, empty and packet-suppressed results still require real dispatch. REPEAT's explicitly Undemanded count is accepted only for that operation with NULL bytes and Allow, then becomes an irrelevant physical Some(0), never a claim that the count evaluated to SQL NULL. One shared core retains the original clamp/loop, with an empty-input exit that avoids 2^31-1 no-op iterations.

TO_BASE64 has one encode/wrap core: native inputs above16MiB remain supported while the wire path retains its empty-result cutoff. FROM_BASE64 has one decoder with distinct policies: native strips four whitespace characters and returns NULL for invalid cleaned length modulo4; wire strips six and retains its empty-result length-rejection branch. A fifth value-only FROM_BASE64 recipe uses Bytes without packet policy while retaining the execution context. The signed raw-length guard applies only to packet-native decoding. Silent length overflow does not read the frontend packet limit: original arguments and Allow reach the kernel, which decides NULL.

SQL coverage uses seven small rows, original Text versus Bytes results and 76-column wrapping; four packet1024 cases retain warning1301 plus NULL, including FROM_BASE64's raw-size estimate before whitespace cleaning. Nine direct zero-slot refusals include a retained warning1301 with a separately returned typed1105. Dispatch tests additionally verify actual output above16MiB, policy order, argument demand and value-only context.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/packet-string-four-summary.txt) retain all six receipts, including the initial SQL failure.

- TiKV `local::`: 207 passed/1 ignored/469 filtered.
- TiKV official string tests: 63 passed/614 filtered.
- TiDB `packet_string_dispatch_`: 3 passed/1495 filtered.
- Initial session lifecycle/SQL filter: 36 passed/1 failed/2078 filtered, exit101.
- Session lifecycle/SQL retry: 37 passed/2078 filtered, exit0.
- Full expression suite, run independently after the initial SQL failure: 1400 passed/4 existing failures/94 ignored; 1498 discovered, exit101. Parent compared all four complete failure blocks against pi-ip-five-16: identical after only thread-ID normalization. The full suite remains non-green.

## New SQL assertion correction

The initial serial chain stopped at SQL and did not run full expression. The new zero-slot test incorrectly expected the returned evaluation-origin1105 to also appear as an Error row in the warning list. E traced existing session handling through `lib.rs:2056` to2087 and2282–2287: that error is returned, not appended. Only the new assertion/comment changed to check the warning1301 list and returned typed1105 separately; this correction changed neither runtime behavior nor pre-existing expected values. The independent full-expression run and subsequent SQL retry are separate receipts.

## Review and not verified

Parent reviewed the native algorithm deletions. No PB/unistore admission was added, unistore was not rerun in17, and driver/pool/limits were not changed. Complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, release performance, full-workspace validation, make lint and final acceptance remain open. These receipts do not establish PR readiness.
