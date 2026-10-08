# Decimal raw-word projection

`decimal-word-parts-196` moves the remaining duplicated raw-word-to-coefficient algorithm to TiKV `native_decimal_codec`. It owns leading-zero scanning, reverse integer digit emission, left-aligned fractional emission, inline `SmallVec<[u8;24]>` storage, empty-zero representation, sign and scale.

TiDB `MyDecimalWords::to_decimal` is now a thin concrete-value adapter. Binary decoding and JSON restoration both use that adapter; fractional negative zero remains preserved while scale-zero zero normalization is unchanged.

The MyDecimal JSON object schema and serde validation are TiDB-only persistence policy, not duplicated SDK logic, and remain native. Five focused filters are GREEN. [Receipts](../logs/decimal-word-parts-summary.txt).
