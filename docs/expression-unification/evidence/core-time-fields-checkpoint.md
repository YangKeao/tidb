# CoreTime field projection

`core-time-fields-200` completes raw CoreTime field ownership in TiKV. Alongside year/month/day, SDK `Time` now supplies const hour/minute/second/microsecond accessors; its seven-field view calls those same accessors.

TiDB deletes four offset/mask algorithms and keeps `CoreTime` only as its public raw-bit facade. Three focused filters are GREEN. [Receipts](../logs/core-time-fields-summary.txt).

A FieldType aggregate cannot losslessly use an existing SDK value because TiDB-only exact names, element markers, ARRAY and wide flags would be lost. Decimal's exact five-field match to `NativeDecimalParseValue` is retained as the next larger M2 candidate.
