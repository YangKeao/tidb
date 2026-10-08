# CAST functional completion

**cast-complete-184 / R190** earns frozen family `cast` functional credit. TiKV owns AST admission, every implemented target conversion, legacy source conversion and Time→Duration. AST JSON uses the existing thin `builtin_ext` projection over `native_cast_as_json`; Unistore implemented value casts use `eval_legacy_*` bridges. Native retains child/NULL/Datum/error/context effects and full-width i128 identity projection. Condition arms only project truth/presence.

Explicit exceptions are not hidden fallback: now-anchored Duration→Time remains refused, and baseline-unimplemented signatures remain unsupported. SDK and Unistore focused gates GREEN; [receipts](../logs/cast-complete-summary.txt).

Functional **240/245 (97.96%)**, strict0, remaining5. Broader M2/root/live-DAG/final gates remain active.
