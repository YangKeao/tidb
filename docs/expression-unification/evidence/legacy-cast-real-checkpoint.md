# Legacy REAL CAST migration

**legacy-cast-real-175 / R181**, after [legacy JSON CAST](legacy-cast-json-checkpoint.md). Functional239/245, strict0, remaining6 unchanged; partial CAST deletion.

A new direct regression first proves the existing `SimpleSig::CastRealAsReal` path incorrectly returns NULL. `tidb_query_datatype::native_scalar_convert` will own legacy i128/real/decimal/lossy-string conversion and folding; Unistore will retain child/NULL/result projection.

## Validation

Pre-fix regression RED reproduced actual `None` versus expected `Some(2.5)`. The identical test and four other final gates are GREEN after implementation. [Exact commands/counts/hashes](../logs/legacy-cast-real-summary.txt). Three new tests;2 TiKV/118 TiDB old touched-file tests unchanged. Whole CAST remains uncredited; other legacy targets remain.
