# Five retained families: narrow host adapters

Checkpoint `five-host-adapters-216` does not migrate or credit another family. It narrows the already-approved 240/245 Demo exception boundary so the five retained algorithms do not require a second generic native expression evaluator.

## Boundary

- `rust/crates/tidb-expr/src/host_compat.rs` is the only value-level name/arity entry for JSON_SCHEMA_VALID, TIDB_DECODE_PLAN, TIDB_DECODE_BINARY_PLAN, TIDB_ENCODE_SQL_DIGEST and VALIDATE_PASSWORD_STRENGTH.
- Unknown names return `None`; there is no fallback evaluator.
- The production `builtin_ext::{json,info,crypto}` family matches no longer contain these names.
- JSON_SCHEMA_VALID alone has a dedicated expression-level adapter because the existing per-node schema cache and no-I/O-on-NULL behavior require lazy child demand.
- The retained parser normalizer, plan codecs/renderers, JSON resource resolver and password/global-variable policy remain TiDB-owned. This simplification does not claim their migration or alter the frozen count.

## Validation

- Generic-family refusal and closed ownership: 1 passed, static ownership audit 2 PASS.
- Focused retained semantics: 6 passed across decode/password/schema/cache/lazy tests.
- SQL entry tests: password 1 passed; plan decode 1 passed; digest session 1 passed.
- Required TiDB `make lint`: exit 0.
- `docs/agents/agents-review-guide.md` scoped review: no normative-policy addition and all new index paths exist.
- Distilled receipts and SHA-256 values: `../logs/five-host-adapters-summary.txt`.

Command-construction and filter mistakes are disclosed in the receipt and Plan. No zero-match run or failed command is counted as a gate.
