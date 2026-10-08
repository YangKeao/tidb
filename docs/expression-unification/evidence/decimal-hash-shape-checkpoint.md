# Decimal natural and hash shape

`decimal-hash-shape-203` makes TiKV `NativeDecimalParseRef` the single owner of natural `PrecisionAndFrac` and hash-key significant-fraction shape. Leading integer zeros, trailing fractional zeros, and minimum precision are normalized there.

TiDB deletes the repeated shape bodies from `precision_and_frac`, `to_hash_key`, and `hash_key_size`; it only feeds the shared shape to the binary codec and projects warnings. Hash vectors and executor grouping remain GREEN. A zero-match filter is explicitly excluded. [Receipts](../logs/decimal-hash-shape-summary.txt).

Natural codec shape is therefore no longer an M2 blocker. Assignment fit/clamp and chunk fixed-cell fallback remain active.
