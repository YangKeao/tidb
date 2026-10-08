# Decimal fixed binary decode

`decimal-binary-decode-195` moves the complete fixed binary reader to TiKV `tidb_query_datatype::codec::mysql::native_decimal_codec`, beside the existing size and writer algorithms. The SDK owns sign restoration, shape and 40-byte bounds, nine-word clamping, partial-word decoding, corrupt-word checks, cursor disposition, soft warnings, and noncanonical negative zero.

TiDB `Decimal::from_bin_with_failure` now only projects decoded words into its private `Decimal` and concrete failure type. Its local `read_word`, `fix_word_cnt_error`, and decoder body were deleted. Four focused filters are GREEN. [Receipts](../logs/decimal-binary-decode-summary.txt).

This is an M2 type/value step, not a new function-family credit or a claim that broader M2 is complete.
