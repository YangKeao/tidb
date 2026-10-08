# Strict metadata timing

`strict-metadata-timing-207` fixes two M3 timing gaps.

Constant REGEXP calls are now excluded from bottom-up folding. A malformed constant in an unselected IF/IFNULL/CASE/COALESCE branch is neither compiled nor cached; a demanded constant still compiles and memoizes in the TiKV kernel. Previously folding executed compilation and swallowed the error, making visible SQL results appear lazy while violating demand timing.

String-IN cache construction now has an explicit prepare-once marker. Recursive ancestor preparation no longer rebuilds the same node; argument invalidation clears the marker and prepared-statement rebinding rebuilds from current values. Runtime parameters remain outside the strict-literal hash.

Nine focused gates are GREEN. Strict-local IN itself remains rejected because the legacy wire mapper reorders dynamic arguments; nested wire AND/OR beyond 32 remains the other M3 deferral. [Receipts](../logs/strict-metadata-timing-summary.txt).
