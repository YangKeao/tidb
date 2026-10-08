# Expression unification experiment

Current paired checkpoint **legacy-numeric-prefix-179**. Functional **239/245 (97.55%)**, strict **0**, remaining **6**.

TiKV owns the legacy numeric-prefix scanner shared by JSON→REAL, JSON→INT and string-condition truth; the Unistore duplicate is deleted. [Evidence](evidence/legacy-numeric-prefix-checkpoint.md) · [receipts](logs/legacy-numeric-prefix-summary.txt) · [manifest](checkpoint.json).

Four final gates GREEN; one dependency-boundary compile RED retained and corrected with a narrow bridge. Whole CAST remains partial; R100 and broad final gates remain. Goal active.
