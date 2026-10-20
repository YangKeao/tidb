// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Metadata selectors and pure variable/session value effects, no expressions.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UserVariableKind {
    Int,
    UInt,
    Real,
    Decimal,
    String,
    Time,
    Unsupported,
}
impl UserVariableKind {
    fn from_name(name: &str) -> Self {
        match name {
            "int" => Self::Int,
            "uint" => Self::UInt,
            "real" => Self::Real,
            "decimal" => Self::Decimal,
            "string" => Self::String,
            "time" => Self::Time,
            _ => Self::Unsupported,
        }
    }
    fn as_name(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::UInt => "uint",
            Self::Real => "real",
            Self::Decimal => "decimal",
            Self::String => "string",
            Self::Time => "time",
            Self::Unsupported => "unknown",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UserVariableAction {
    Get(UserVariableKind),
    Set,
}
impl UserVariableAction {
    pub(crate) fn for_function(function: &ScalarFunction) -> Option<Self> {
        if function.pb_builtin.is_some() {
            return None;
        }
        let name = function.func_name.lowercase();
        if function.args.len() == 1 {
            if let Some(kind) = name.strip_prefix("getvar_") {
                return Some(Self::Get(UserVariableKind::from_name(kind)));
            }
        }
        (name == "setvar" && function.args.len() == 2).then_some(Self::Set)
    }
    pub(crate) fn read(self, ctx: &dyn Columns, name: &str) -> Result<Datum, EvalError> {
        let Self::Get(kind) = self else {
            unreachable!("variable read selector")
        };
        let value = ctx.get_uservar(name).unwrap_or(Datum::Null);
        super::uservar_as_kind(kind.as_name(), value, ctx)
    }
    pub(crate) fn write(
        self,
        ctx: &dyn Columns,
        name: &str,
        value: Datum,
    ) -> Result<Datum, EvalError> {
        assert_eq!(self, Self::Set, "variable write selector");
        if !value.is_null() {
            ctx.set_uservar(name, value.clone());
        }
        Ok(value)
    }
}
pub(crate) fn record_last_insert_id(
    value: Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let cast = crate::cast::cast_arg_as_int(&value, source, ctx)?;
    let recorded = match cast {
        Datum::Int(i) => i as u64,
        Datum::UInt(u) => u,
        _ => return Err(EvalError::Unsupported("LAST_INSERT_ID argument")),
    };
    ctx.set_last_insert_id(recorded);
    Ok(Datum::Int(recorded as i64))
}
