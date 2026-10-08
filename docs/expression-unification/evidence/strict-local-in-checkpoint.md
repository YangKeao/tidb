# Strict local signed IN

`strict-local-in-208` admits ordinary signed-LongLong SQL IN through a private TiKV local identity, `InIntSourceOrder`.

The local registry retains every argument in source order and selects the ready-value comparer with unit metadata. It never calls the PB/wire `init_compare_in_data`, so runtime bindings are not extracted, reordered, or inserted into a persistent hash. The ready comparer now delegates its three-valued reduction to `native_in_ready_values` instead of retaining a second reducer. Reusing one compiled program with changed input values proves rebinding changes true/NULL outcomes.

The original PB `InInt` local-rejection oracle remains unchanged and GREEN; the wire mapper's existing `[0,4,2]` retained-order oracle is also GREEN. Thus this local route does not alter wire behavior. Eight focused gates are GREEN. [Receipts](../logs/strict-local-in-summary.txt).

This closes the strict-local signed-IN gap. The official wire builder's eager fallback for nested AND/OR deeper than 32 remains M3's explicit open item.
