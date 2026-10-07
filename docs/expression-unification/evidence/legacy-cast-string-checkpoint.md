# Legacy STRING CAST migration

**legacy-cast-string-176 / R182**, after [legacy REAL CAST](legacy-cast-real-checkpoint.md). Functional239/245, strict0, remaining6 unchanged; partial CAST deletion.

TiKV owns legacy i128 sign selection, numeric/temporal rendering, raw string bytes passthrough and error folding. A narrow TiDB bridge projects Datum; Unistore keeps child evaluation and NULL/Datum projection. Six local `SimpleSig::*AsString` bodies are deleted.

Four final Cargo gates GREEN: SDK, bridge, decimal/raw source and existing five-source rendering composition. [Exact receipts](../logs/legacy-cast-string-summary.txt). Three new tests; old touched tests unchanged. Whole CAST remains uncredited because other legacy targets remain.
