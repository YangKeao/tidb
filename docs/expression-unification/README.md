# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **intdiv-sdk-67**, following **tso-timediff-66**.

## Progress

Functional migration remains **208/245**; strict final-audited count stays **0**. Target221 still needs13 more families;37 eligible families remain. Latest functional migration: tso-timediff-66.

This step shares **six type/SDK implementations**, not the INTDIV evaluator: three original raw Decimal projections/conversions move to TiKV, and three native signed/mixed division helpers delegate to existing TiKV codec implementations. Native methods retain representation/error adaptation only. No family credit is added; no evaluator, profile, carrier, binding or admission changes.

## Evidence and limits

[Evidence](evidence/intdiv-sdk-checkpoint.md), [exact commands and hashes](logs/intdiv-sdk-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight agents mapped the closure; two exclusive writers changed4 Rust files. All5 new SDK tests passed on their first gates. Eight Cargo launches: six green gates and two unchanged old full-suite REDs; no new RED, compile failure, retry or zero-match run. Original test bodies/oracles remain byte-exact.

CPP raw2/local317+1ignored; native Decimal97/overflow18/expression2/SQL1 pass (filters overlap). The existing SQL test checks four original queries across both vector modes, not new worker takeover. Full expression **1554/4old/94ignored**, unistore **208/1old/13ignored** match prior complete failure sections after only thread-ID normalization.

Raw SDK behavior is not narrowed through the normalized math bridge: signed-text i128 projection, visible-scale independence, unsigned-negative early return, UTF8/slicing/scale panics and native diagnostic wording stay. Error-string allocation equivalence and performance are unverified. A new helper name collision was corrected before compilation without changing the old private method.

INTDIV still needs exact division SDK closure, native/legacy evaluator policies, original precision-getter demand, warning-before-integer-overflow ordering and scalar/vector NULL roots. Its reachable legacy i128 MIN/-1 panic must not be hidden. A future two-stage design must explicitly share the existing scope; no ABI is approved by this checkpoint.

Prior JSON_KEYS and AST/SQL compatibility gaps remain. M6/default-NoColumns whole roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and previous parser/GB/vector/Decimal exceptions are deferred. No dependency/generated/Go/Bazel changes or package-transcreation/PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. The old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
