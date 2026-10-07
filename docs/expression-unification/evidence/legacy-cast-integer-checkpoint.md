# Legacy integer CAST migration

**legacy-cast-integer-177 / R183**. Functional239/245, strict0, remaining6 unchanged; partial CAST deletion.

TiKV owns REAL half-away rounding/range subject, Decimal truncation/overflow subject and lossy string prefix saturation. TiDB projects closed Value/Overflow; Unistore retains child/NULL and concrete legacy error construction. Three local bodies are deleted.

Four final targeted Cargo gates GREEN. One new bridge test initially failed compilation because the new projection enum lacked test assertion derives; adding `Debug, PartialEq` changed no algorithm and rerun passed. See [receipts](../logs/legacy-cast-integer-summary.txt). Whole CAST remains uncredited.
