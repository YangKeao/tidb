## Decision: retain only the source-matched unary kernels

NOT measures8.085ns/row versus true native47.038 (5.82x). ASIN10.686 versus
112.748 (10.55x) and ACOS10.756 versus112.619 (10.47x), on the declared mixed
normal/out-of-domain dataset. Current fallback ablations826–1069ns are explicitly
separate; their roughly98–102x ratios are not historical-native improvements.

Controls retain tradeoffs: Decimal LT72.924→73.976ns (~2% slower), NULL-safe
comparison73.243→74.085 (~1% slower) and PLUS128.795→129.236 (near flat/slower).
Other controls are near flat or improve1–3%; these are observed build results,
not a new arithmetic algorithm. Independent M15 Int12.860→12.859 and LOWER
32.973→32.913 are flat, Datetime10.182→10.252 slightly slower and Duration
14.449→13.520 improves. All shapes and process ranges remain visible. No claim
that every old regression is permanently fixed, every function optimized or
application QPS improved.

Retained TiKV2b289f9eaa2f4c2620b4e128ec3f5a2b2e08be02 / this TiDB report commit.
Only existing matching kernels are admitted; no new driver, mathematical
algorithm or migration route. Native Go/libm trig and nonidentity ROUND policy
boundaries remain. Dataset selection is confined to test probes, with default
old controls unchanged. Signed/unsigned NOT preserves NULL and all input bits.
