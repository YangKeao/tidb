# Legacy numeric prefix migration

**legacy-numeric-prefix-179 / R185**. TiKV owns the optional-sign/integer/fraction/exponent scanner shared by JSON→REAL, JSON→INT and string-condition truth. Unistore's duplicate function is deleted. Four final gates GREEN. Initial native compile RED used a transitive crate directly; fixed with a narrow tidb-expr bridge, production scanner unchanged. [Receipts](../logs/legacy-numeric-prefix-summary.txt). Functional239/245, strict0, remaining6; partial CAST only.
