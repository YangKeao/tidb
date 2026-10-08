# Decimal fixed-word chunk projection

`decimal-chunk-projection-205` moves the final known Decimal M2 blocker to TiKV. `NativeMyDecimal::from_decimal_parts_lossy` projects coefficient bytes directly into the nine-word cell: integer overflow keeps the low 81 digits, fraction overflow keeps the prefix fitting after integer words, visible fraction is clamped to retained storage, and all-zero words clear the sign.

TiDB deletes its SQL-text bridge (`String`, decimal-point/sign insertion, `MyDecimal::from_string`, and result-fraction restamping). Its facade now only maps shared raw storage.

SDK/parser raw-part parity, three overflow/truncation vectors, and the chunk consumer are GREEN. [Receipts](../logs/decimal-chunk-projection-summary.txt).

Together with the preceding Decimal, temporal, duration, charset, JSON and vector checkpoints, this closes broader M2 for the planned Demo scope. FieldType aggregate remains an explicit no-lossless-SDK-value PASS; host JSON schemas and concrete diagnostics are metadata boundaries, not duplicate value algorithms. Strict family audit and M3/M6 are still open.
