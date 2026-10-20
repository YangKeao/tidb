// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Session-value policy without expression arguments or stored session values.
use super::*;
#[derive(Clone, Copy, Debug)]
pub(crate) enum SessionRead {
    Database,
    Version,
    ProcessInfo,
    GrantUser,
    ActiveRoles,
    ResourceGroup,
    LoginUser,
    ConnectionId,
    FoundRows,
    RowCount,
    LastInsertId,
}
impl SessionRead {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "database" | "schema" => Self::Database,
            "version" => Self::Version,
            "tidb_version" => Self::ProcessInfo,
            "current_user" => Self::GrantUser,
            "current_role" => Self::ActiveRoles,
            "current_resource_group" => Self::ResourceGroup,
            "user" | "session_user" | "system_user" => Self::LoginUser,
            "connection_id" => Self::ConnectionId,
            "found_rows" => Self::FoundRows,
            "row_count" => Self::RowCount,
            "last_insert_id" => Self::LastInsertId,
            _ => return None,
        })
    }
    pub(crate) fn for_function(function: &ScalarFunction) -> Option<Self> {
        if function.args.is_empty() && function.pb_builtin.is_none() {
            Self::from_name(function.func_name.lowercase())
        } else {
            None
        }
    }
    pub(crate) fn eval(self, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        Ok(match self {
            Self::Database => match ctx.current_database() {
                Some(name) => Datum::new_string(name.into_bytes()),
                None => Datum::Null,
            },
            Self::Version => ctx.sysvar(None, "version").unwrap_or(Datum::Null),
            Self::ProcessInfo => Datum::new_string(ctx.tidb_info().into_bytes()),
            Self::GrantUser => match ctx.current_user() {
                Some(user) => Datum::new_string(user.into_bytes()),
                None => Datum::Null,
            },
            Self::ActiveRoles => match ctx.current_role() {
                Some(roles) => Datum::new_string(roles.into_bytes()),
                None => Datum::Null,
            },
            Self::ResourceGroup => match ctx.current_resource_group() {
                Some(group) => Datum::new_string(group.into_bytes()),
                None => Datum::Null,
            },
            Self::LoginUser => match ctx.login_user() {
                Some(user) => Datum::new_string(user.into_bytes()),
                None => Datum::Null,
            },
            Self::ConnectionId => match ctx.connection_id() {
                Some(id) => Datum::UInt(id),
                None => Datum::Null,
            },
            Self::FoundRows => match ctx.found_rows() {
                Some(rows) => Datum::UInt(rows),
                None => Datum::Null,
            },
            Self::RowCount => match ctx.row_count() {
                Some(rows) => Datum::Int(rows),
                None => Datum::Null,
            },
            Self::LastInsertId => match ctx.last_insert_id() {
                Some(id) => Datum::UInt(id),
                None => Datum::Null,
            },
        })
    }
}
