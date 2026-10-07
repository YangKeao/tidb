# Expression unification experiment

Current paired checkpoint **cast-admission-178**. Functional **239/245 (97.55%)**, strict **0**, remaining **6**.

TiKV owns AST CAST range-sentinel and Vector target admission. TiDB retains Datum/CastType projection and established errors. [Evidence](evidence/cast-admission-checkpoint.md) · [receipts](logs/cast-admission-summary.txt) · [manifest](checkpoint.json).

Two targeted gates GREEN. Whole CAST remains partial; R100 historical failures and full workspace/lint/dev/bazel/release/performance/final-readiness gates remain unverified. Goal active.
