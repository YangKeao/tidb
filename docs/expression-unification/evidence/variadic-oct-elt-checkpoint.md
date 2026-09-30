# Variadic strings, OCT and ELT — variadic-oct-elt-four-23

**Final functional checkpoint: five actual Rust receipts; the full expression suite remains non-green.**

After the core/native/SQL gates, functional delegation/native-deletion advances **70→74/245**; final acceptance remains **0/245**. The four added frozen IDs are `oct`, `concat`, `concat_ws`, `elt` ([coverage baseline](coverage-baseline.json), lines259/117/118/145), covering their existing complete arity/type domains rather than a small-arity subset. There is no alias double count, denominator change or completed Go-package claim.

All16 source files are frozen and parent-formatted with the pinned toolchains: B3 (`string_fn.rs`, `func.rs`, `lib.rs`), D4, E1 and C8. [Exact parent commands/results](../logs/variadic-oct-elt-summary.txt) distinguish all five receipts, including the non-green full expression run.

## Entry coverage and full domains

- Both native CONCAT/CONCAT_WS surfaces—the value helpers in `string_fn.rs` and typed child loops in `scalar_function.rs`—route through `ConcatPreparation`. Actual byte joining is the shared backend `concat_join`, also used by the wire functions; neither native surface retains its own join.
- The public production `lib.rs::concat_values` is retained as a compatibility wrapper through `concat_values_in(values, &NoColumns)`. New `concat_values_in(values, ctx)` carries the caller's real context to `concat_with_context`; it is not a cfg(test)-only escape or a new projection activation.
- AST evaluation (`lib.rs` → `func::eval_func`) eagerly evaluates all children before the value helper. Typed CONCAT/CONCAT_WS evaluate and prepare children left-to-right, stopping at their original NULL/packet point. Moving both through one preparation type must not erase this difference.
- OCT has exactly one SQL operand; CONCAT accepts one or more; CONCAT_WS and ELT accept two or more with no fixed maximum. ELT's cast-mask bit31 repeats for all higher positions: no3/4/32-operand truncation is permitted.
- The inspected vector output route calls scalar row evaluation. No independent four-family implementation was found in `builtin_ext`, the native PB catalog or unistore SimpleSig/eval paths. Existing TiKV wire signatures are not authorization for new native PB/unistore admission.

## Final CONCAT transport and terminal protocol

`ConcatPreparation` retains the demanded `Vec<Option<Vec<u8>>>` prefix, its original complete SQL arity and its CONCAT/CONCAT_WS kind. `prepare_concat_args` builds an opaque input carrier, **not a SQL result**. The packed carrier uses **one physical Bytes column**, preserving actual NULL and empty operands, the actual consumed prefix, and the original arity. It is neither a prejoined answer nor a padded sequence of fake SQL NULLs for undemanded children.

The final terminal is exactly `ConcatTerminal::{Complete, InputNull, PacketExceeded { limit: u64 }}`. `InputNull` carries a real observed NULL (for WS, its separator), not a generic suppression instruction. The packet terminal captures the **real limit returned by the comparison that stopped that prefix**: the first getter of that overflow event, not the later warning-message getter or an invented fixed limit.

The agreed C-side contract revalidates the prefix/terminal shape and performs the single relevant prefix-threshold check against that supplied limit. It does not call a context getter, replay the frontend's getter history or perform a second frontend policy decision. Legal NULL, empty and warning-suppressed outcomes still require the real C4 invocation; an original strict packet error remains a frontend error, not a fabricated worker failure.

Native preparation retains coercion/demand/packet bookkeeping, not output joining:

- CONCAT coerces each demanded operand, stops at NULL without that operand's packet getter, and otherwise adds its length before the getter/comparison.
- WS does not read the packet getter for the separator or NULL data operands. Its separator budget uses the **original argument index >1**, not the number of surviving parts; a preceding NULL can therefore leave a budget larger than the final joined output.
- The original overflow handler can read `max_allowed_packet` again while formatting the diagnostic and then warn1301 or return the strict error. **There is no claim that an overflow has only one getter read.** A warning is not permission to bypass the backend with a native NULL answer.

The pure input-builder wrapper retains LocalError as opaque ExpressionRuntimeFailure with phase None; it does not invent a SQL site or call encoding a worker invocation. Encoding alone remains distinct from backend execution.

## ELT selection versus expression demand

`elt_selected_arg(index, total_sql_arity)` is the shared selector used by preparation and the backend contract. **SQL arity includes the index operand**, and its result is the SQL operand offset (first candidate =1), not a zero-based candidate index. It does not evaluate a child.

AST and generic typed evaluation still eagerly evaluate every child, then run the existing int/string wrappers before ELT's body. Only the selected value is subsequently read with `eval_string`; a NULL/out-of-range selector cannot newly suppress earlier all-child/wrap errors. UInt selectors retain the existing `as i64` wrap, and selected uncast types remain errors rather than silent NULL.

`EltReady` keeps the real index, total SQL arity and selected `ReadyBytesArg`: `Value(None)` means a selected SQL NULL; `Undemanded` means there was no selected operand to convert. The declared recipe is three physical columns, not a new generic variadic/four-column driver. Arbitrary selected bytes remain byte-preserving, and **any candidate's binary type**, including an unselected one, still controls the result's binary/string classification and static charset.

## OCT policies and shared scanner

The frozen native adapter keeps Int/UInt as all64 two's-complement bits and Bit/BinaryLiteral via `to_int().value()`, then dispatches OctInt. Every other value, including Enum/Set names and NULL, goes through byte coercion to OctStringNative. OCT does not join the generic ETInt cast layer or acquire float/Decimal rounding semantics.

The shared OCT scanner/formatter (`oct_decimal_prefix`, `oct_bits`) retains both old policies: native valid UTF8 uses Unicode trimming, malformed UTF8 uses only ASCII trimming; wire OctString uses ASCII trimming. Original empty bytes mean NULL, nonempty whitespace/invalid-prefix bytes mean zero. The signed decimal prefix, in-range unsigned wrapping negation, and saturation at u64::MAX on overflow without negating an overflowing negative magnitude remain intact. A Unicode-space prefix does not acquire wire behavior. The former native OCT scanner/formatting body is removed; native and wire adapters use the shared helpers.

## Validated coverage and five receipts

The three dispatcher tests passed, covering getter/diagnostic/stop ordering, original-index WS budgets and >4-operand/root-terminal cases. Both new session tests passed within the49-test gate: **five rows by nine output columns** (eight family results plus an unsigned control), six-operand CONCAT/WS, and six ELT candidates/seven total SQL operands with selectors reaching5/6. String result widths remain [64,64,20,20,21,21,8,8], with the original decimal/charset/binary metadata.

The second session test passed **16 direct zero-slot probes**, including NULL, empty, invalid-selector and selected-NULL answers: PoolResource/Pool, evaluation-origin1105/HY000, no warning row. These are real column-driven calls, not a constant-folded small-arity subset. Original expected values and fixtures are not changed.

- TiKV `local::`:233 discovered,232 passed,1 ignored,469 filtered,0.19s, exit0.
- Official string:63 passed,639 filtered,0.02s, exit0.
- Native concat_stream_dispatch_:3 passed,1516 filtered,0.00s, exit0.
- SQL evaluated_ascii_:49 passed,2078 filtered,0.69s, exit0.
- Full expression:1519 discovered,1421 passed,4 failed,94 ignored,0 filtered,10.38s, **exit101**.

Parent compared the entire current failures section with collated-five-expr-full.log: byte-identical after **only thread-ID normalization**; reported normalized-section SHA256 `fb665bb70597cfff204649928faef5a1284edcae0a2f1ecd275295b3b36e7cb3` (not a whole-log hash). All four failures remain; this is not a green full suite. The first January compilation completed in9.29s, without an actual compilation failure or new runtime failure. Parent reports both git diff --check and both lockfile git diff --exit-code checks exited0; these are not extra Rust receipts.

## Scope, resources and unverified gates

There is no new four-column allowance, generic driver/graph broadening, default projection activation or native PB/unistore admission, and no native replay/fallback after a backend error.

Packing/decoding, input ownership and retained/copy storage can consume resources beyond the logical SQL output. Neither one physical input column nor these functional gates prove zero-copy operation, allocator behavior, physical peak/OOM safety or performance. These remain unmeasured.

Standalone foundation datatype/collation, unistore, parser and generator checks were **not rerun this round**. The older unistore one-failure and parser7-versus5/poison observations remain historical non-green evidence, not current passes. The full-expression comparison above does use the fresh current run. This writer ran no build/test/fmt and changed only these two documents, not earlier evidence, Plan or index.

Whole-workspace validation, make lint, complete/deeper scope and guard validation, allocator/performance measurement, physical resource guarantees and final acceptance remain unverified. Functional credit is **74/245**, final acceptance **0/245**; this is not PR-ready. Sources and these two documents are frozen.
