# N41: preserve PB integer carriers and integer batch presentation

This checkpoint passed final frontend validation and ownership review. It fixes an admitted source correctness gap discovered after N40; no legacy-bulk alias guard was relaxed. No push, PR, release or cgo work is included.

The original PB branch interprets an integer carrier using the actual result declaration's unsigned flag before converting to the result type. N40 preserved unsigned integer results, but its generic regexp host omitted this preceding PB interpretation for other result types. An unsigned JSON result therefore used signed binary-JSON kind 0x09 instead of the original unsigned kind 0x0a, even for 0/1. The regression reproduced identical SQL outcomes and callback records with different binary JSON data.

The unchanged original c69e7b83 implementation was independently captured and rerun before production edits. All 1,798,960 observations were pinned before the fix:

| Corpus | Records | Original digest |
| --- | ---: | --- |
| Ten result declarations, flag presence and phase/effect matrix | 1,474,560 | 36AB4F4C21EA7B05 |
| Full actual values in every real argument position | 321,840 | C125E5697C81E883 |
| Long selections, cache/context and alternating modes | 2,560 | 4CF8E7A778781BF1 |

All actual observations pass strictly without fallback or resource exemptions. Tests retain the real decoded declarations and PbOrigin, including the fact that a wire NULL atom has an effective SomeNull SQL argument type regardless of its raw wire field override. Literal non-NULL shapes retain their real field types; additional controls use decoded byte atoms for effective parameter-type variation. Root result declarations and unsigned flags are real wire inputs in every case. No original record, digest, count, fixture or resource limit was changed.

The exact original integer-carrier match is now shared from pb_builtin.rs. The original ScalarFunction facade still invokes it before the unchanged result conversion. NativeRegexpValues records the actual PB origin as protobuf_result, and the existing host applies that same pure boundary after its closed value kernel and before result conversion. Ordinary operator/named regexp does not execute the PB boundary. The helper performs no expression evaluation, callback, allocation or metadata synthesis.

The wider original corpus also exposed a second existing boundary: a generic integer batch evaluates the scalar PB expression, applies Expression's enum/set view, normalizes each row as EvalInt, obtains integer bits and then presents them through the real result signedness. With a BIT result, this includes the original time-zone/flags callbacks and can publish integer data into a variable-width BIT destination, producing the original sink panic only after the complete batch has been collected. Returning the scalar BIT value instead changed both events and behavior.

The source now uses its genuine original static integer-batch candidate to select BulkRoot(Int), paired with the existing RootValue row profile. Arguments remain raw Fresh demands per row; the flag changes only the original result-consumer policy. The host calls the existing enum/set view, normalize_numeric_row_value and native_batch_integer_bits/value functions after result conversion in the bulk profile. The scalar profile retains its original value. The old sink produces the actual original panic where applicable; no panic or error was fabricated, and the real BIT declaration remains unchanged. Source metadata rejects impossible PB/operator/presentation combinations before the cache getter. Callback results are reauthenticated before publication.

Eight source checks pass, including 4320 ordinary non-PB controls, actual paired BIT metadata, five malformed-metadata refusals, time-zone/type-flag callback arena epoch changes and panic/cancellation/work-scope cleanup, and four fixed 16KiB resource-failure-to-NULL recoveries. Three JSON/YEAR cases additionally recover to small non-NULL results at the same cap. The existing BIT return formatter reserves padding work exceeding 16KiB even for a small value, so the BIT small-value check explicitly retains Resource refusal and unchanged output/cap; it is not counted as native equivalence. The new callback fixture uses 64KiB to reach BIT presentation; production and historical budgets remain unchanged.

Ownership reconstruction verifies the complete old PB scalar facade around the pure extraction, unchanged selected PB algorithms, original normalization helpers, all old regexp phases/cache algorithms, decoder/provenance rules, native eligibility, legacy-bulk guard, SDK and prior semantic observers. Both rustfmt toolchains cover eight changed Rust files.

Final caller results: all 44 Projection checks pass, including actual JSON/YEAR unsigned result declarations with real decoded parameter producers, three display names, both flags, 0/3/2051 rows and serial/parallel execution. The reference and actual collectors use the same range API. Lint exits zero. The complete frontend has 2125 passes, exactly the three inherited failures and 95 ignored tests; all 584 batch checks pass with the unchanged 346 aggregate warnings. Current Expand passes all six checks and preserves 64/B42FB8BE316E6C79. The fixed optional inventory retains 83088/0/0. Final ownership, both format checks and diff review pass. These inventories do not establish whole-project completion.

Commands from the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_result_source_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Workspace evidence is under evidence/no-fallback/n41-*, including before snapshots, pinned observers, original capture/rerun, both actual failure logs and the ownership checker. SDK ad73b11 remains clean and unchanged; its N16 baseline remains applicable. The old N40 and N39 corpora remain fixed and execute in the complete frontend.

Legacy-specialized bulk aliases, other PB signatures, public scalar/AST routing, filter/join, Unistore, generic Admission removal and full service/final performance work remain open. The overall task continues.
