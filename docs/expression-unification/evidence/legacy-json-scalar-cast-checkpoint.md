# Legacy JSON scalar CAST

**legacy-json-scalar-cast-180 / R186**. TiKV owns JSON tag/payload decoding, lossy prefix and zero folding for `CastJsonAsReal/Int`; Unistore keeps child/NULL/result projection. Two local bodies deleted. SDK1, bridge1 and existing composition1 GREEN. Receipts: `../logs/legacy-json-scalar-cast-summary.txt`. Functional239/245, strict0, remaining6; whole CAST still partial.
