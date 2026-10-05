# Shared YEAR expression controller

**year-control-121 / R126**, after [signed-datum foundations](signed-datum-checkpoint.md). Functional238/245, strict0 and remaining7 are unchanged; no whole CAST/M2 or Go-package credit.

SDK `native_cast_year.rs` owns the complete existing YEAR value controller. Duration first obtains and validates the statement timestamp, then reads the actual session zone, then concat mode and invokes shared calendar/year conversion. Missing/invalid clocks fail before later getters. Even concat=true retains the controller's original clock/zone demand; the lower datatype operation's lazy calendar behavior is a separate contract.

Other inputs use shared expression string coercion and date parsing first, then the original value-only signed CAST policy in UTC. This is not bare Datum.to_i64: UInt still wraps, and strings keep the signed CAST prefix policy. `native_cast_integer_signed_numeric` composes existing signed CAST and shared datatype conversion entirely inside SDK. Its input projection is shared with ordinary integer CAST; no second selector or host conversion callback is added.

Native `tikv/cast_year.rs` supplies actual string/numeric views and lazy clock/zone/concat data getters, then maps the integer or original Unsupported message. Outer NULL handling remains unchanged. Ordinary integer-controller callback interfaces, DATE/DATETIME, implicit argument and other typed/write controllers remain separate work.

## Validation

Eight matched gates passed without failure or retry: SDK YEAR1/integer2, four native expression gates and two SQL gates. Three new tests;2 SDK and221 native old test bodies are unchanged. [Exact commands and receipts](../logs/year-control-summary.txt).

New SQL executes four SELECTs/20 cells across two vector modes and two actual session zones with fixed SET timestamp. Stored Duration observes the date boundary (2021/2020); date text produces2024, prefix text42, unsigned MAX wraps to-1, and NULL remains NULL. YEAR4/0 signed metadata and no warnings are asserted. Existing temporal-calendar SQL and integer/context-demand consumers also pass. New literal expectations come from source; original tests remain unchanged. No full suites/lint/performance/physical-memory claim. Historical R100 expression4/unistore1 failures remain unrepaired.
