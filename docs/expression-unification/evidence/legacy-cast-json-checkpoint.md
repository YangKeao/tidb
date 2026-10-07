# Legacy JSON CAST migration

**legacy-cast-json-174 / R180**, after [legacy DECIMAL CAST](legacy-cast-decimal-checkpoint.md). Functional239/245, strict0, remaining6 unchanged; this is a partial CAST deletion step.

`tidb_query_datatype::native_mysql_json` owns legacy JSON encoding/parsing and DateTime/Duration FSP6 restamping while folding legacy errors. A narrow TiDB bridge projects the shared encoded result into `BinaryJSON`. Unistore keeps child evaluation, NULL folding and concrete Datum projection; seven local `SimpleSig::*AsJson` conversion bodies are deleted.

## Validation

Four final Cargo gates GREEN: SDK1, native bridge1, all-seven direct Unistore1 and existing JSON composition1. [Exact commands/counts/hashes and retained new-test oracle RED](../logs/legacy-cast-json-summary.txt). Three new tests;2 TiKV/116 TiDB old touched-file test bodies unchanged. One clear new bridge Rust file; no SQL fixture/probe credit. Whole CAST remains uncredited because other direct legacy targets remain native. No unsupported signature or admission changes. R100 historical failures remain explicit.
