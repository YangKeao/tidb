# Expression unification experiment

Current paired checkpoint **legacy-json-scalar-cast-180**. Functional **239/245 (97.55%)**, strict **0**, remaining **6**.

TiKV owns legacy JSON tag/payload decoding, lossy prefix and zero folding for REAL/INT casts; two Unistore bodies are deleted. [Evidence](evidence/legacy-json-scalar-cast-checkpoint.md) · [receipts](logs/legacy-json-scalar-cast-summary.txt) · [manifest](checkpoint.json).

Three targeted gates GREEN. Whole CAST remains partial; R100 and broad final gates remain. Goal active.
