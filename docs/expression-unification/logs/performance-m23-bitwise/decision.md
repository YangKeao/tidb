## Decision: retain the family and its explicit result contract

The five operations measure11.936–12.027ns/selected-row against true native
79.651–80.606ns, about6.6–6.7x faster. The same-current fallback ablation is
1078.955–1083.818ns and is labeled separately; its roughly90x ratio is not a
historical-native gain. Retain this complete family on the existing driver.

Old controls have tradeoffs: Int13.200→13.558ns (~3% slower), Real14.005→14.135
and DecimalPLUS129.951→130.790 (~1% slower); Decimal comparisons/JSON are near
flat, time/Duration/LOWER improve. All per-shape results and process dispersion
remain visible. The cause of small unrelated changes is not established. M15
independent dense Int12.842→13.584 (~6% slower), Datetime10.142→10.539 (~4%
slower), LOWER33.043→33.702 (~2% slower) remain, while Duration14.424→13.881
improves. Decimal PLUS remains slower than original native; this is not a claim
of universal improvement, all-function optimization or application QPS gain.

Retained TiKV57d3d13d48b425007ed26e0c995b85d5e50101cd / this TiDB report commit.
No temporary driver, alternate bit algorithm or migration execution gate is
introduced. Existing native fallback for functions outside the shared closure
is unchanged; no family-specific fallback remains alongside admitted bitwise
execution. The single registry/catalog and generated kernel selector own the
five signatures. Older row profiles remain unchanged.
