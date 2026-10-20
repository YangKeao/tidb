// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Immutable demand selection. No value conversion or expression execution occurs here.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArgumentPolicy {
    BeforeNameProbe,
    AfterNameProbe,
    CommonEager,
}
pub(crate) fn classify(function: &ScalarFunction) -> ArgumentPolicy {
    use ArgumentPolicy::*;
    let name = function.func_name.lowercase();
    let n = function.args.len();
    if function.pb_builtin.is_some()
        || matches!(
            name,
            "values"
                | "json_schema_valid"
                | "grouping"
                | "benchmark"
                | "sleep"
                | "get_lock"
                | "release_lock"
                | "is_free_lock"
                | "is_used_lock"
                | "release_all_locks"
                | "case"
                | "coalesce"
                | "istrue"
                | "istrue_with_null"
                | "isnottrue"
                | "isfalse"
                | "isnotfalse"
                | "isunknown"
                | "isnotunknown"
        )
        || (n == 2 && binary_op_for_name(name).is_some())
        || (n == 1 && unary_op_for_name(name).is_some())
        || (n == 1 && matches!(name, "charset" | "collation" | "coercibility"))
        || (name == "if" && n == 3)
        || (name == "ifnull" && n == 2)
        || (name == "interval" && n >= 2)
        || (name == "concat" && n > 0)
        || (name == "concat_ws" && n >= 2)
        || (matches!(name, "like" | "ilike") && n == 3)
    {
        return BeforeNameProbe;
    }
    if (name.starts_with("getvar_") && n == 1)
        || (name == "setvar" && n == 2)
        || (name == "regexp" && n == 2)
        || (name == "regexp_like" && matches!(n, 2 | 3))
        || (matches!(name, "to_binary" | "from_binary") && n == 1)
        || (name == "convert_using" && n == 2)
        || (name.ends_with("_in_union") && n == 1)
        || (name.starts_with("cast_") && n == 1)
        || ((name.starts_with("date_add_") || name.starts_with("date_sub_")) && n == 2)
        || (n == 0
            && matches!(
                name,
                "database"
                    | "schema"
                    | "version"
                    | "tidb_version"
                    | "current_user"
                    | "current_role"
                    | "current_resource_group"
                    | "user"
                    | "session_user"
                    | "system_user"
                    | "connection_id"
                    | "found_rows"
                    | "row_count"
                    | "last_insert_id"
            ))
        || (name == "last_insert_id" && n == 1)
        || matches!(
            name,
            "ltrim_with"
                | "rtrim_with"
                | "rand"
                | "tidb_is_ddl_owner"
                | "aes_encrypt"
                | "aes_decrypt"
        )
        || (name == "trim" && n == 2)
        || (n == 1
            && matches!(
                name,
                "length" | "octet_length" | "char_length" | "character_length"
            ))
        || (name == "in" && n >= 2)
        || (name == "locate" && matches!(n, 2 | 3))
        || (matches!(name, "instr" | "strcmp" | "find_in_set") && n == 2)
        || (matches!(name, "greatest" | "least" | "weight_string") && n > 0)
        || (name == "field" && n >= 2)
    {
        AfterNameProbe
    } else {
        CommonEager
    }
}
