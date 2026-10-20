// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original early string policy over actual values and immutable declarations.
use super::*;
#[derive(Debug)]
enum Operation {
    Length(crate::BuiltStringLength),
    Trim(tidb_ast::TrimDirection),
    Locate {
        position: Option<FieldType>,
        three: bool,
    },
    Instr,
    Strcmp,
    Field,
}
#[derive(Debug)]
pub(crate) struct EarlyString {
    operation: Operation,
    collation: tidb_datatype::Collation,
}
impl EarlyString {
    /// Allocation-free eligibility, used before the source metadata byte gate.
    pub(crate) fn supports(function: &ScalarFunction) -> bool {
        if function.pb_builtin.is_some() {
            return false;
        }
        let n = function.args.len();
        match function.func_name.lowercase() {
            "ltrim_with" | "rtrim_with" => true,
            "length" | "octet_length" | "char_length" | "character_length" => n == 1,
            "trim" | "instr" | "strcmp" => n == 2,
            "locate" => matches!(n, 2 | 3),
            "field" => n >= 2,
            _ => false,
        }
    }
    pub(crate) fn for_function(function: &ScalarFunction) -> Option<Self> {
        if function.pb_builtin.is_some() {
            return None;
        }
        let name = function.func_name.lowercase();
        let n = function.args.len();
        let operation = match name {
            "length" | "octet_length" | "char_length" | "character_length" if n == 1 => {
                let selected = if matches!(name, "length" | "octet_length") {
                    crate::StringLengthFunction::Length
                } else {
                    crate::StringLengthFunction::CharLength
                };
                let argument_type = function.args[0]
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null));
                Operation::Length(
                    crate::BuildContext::default().build_string_length(selected, argument_type),
                )
            }
            "trim" if n == 2 => Operation::Trim(tidb_ast::TrimDirection::Both),
            "ltrim_with" => Operation::Trim(tidb_ast::TrimDirection::Leading),
            "rtrim_with" => Operation::Trim(tidb_ast::TrimDirection::Trailing),
            "locate" if matches!(n, 2 | 3) => Operation::Locate {
                position: function
                    .args
                    .get(2)
                    .and_then(|arg| arg.static_type())
                    .cloned(),
                three: n == 3,
            },
            "instr" if n == 2 => Operation::Instr,
            "strcmp" if n == 2 => Operation::Strcmp,
            "field" if n >= 2 => Operation::Field,
            _ => return None,
        };
        Some(Self {
            operation,
            collation: function.derived_collation(),
        })
    }
    /// Trim's original early branch demands at most the first two children.
    /// Missing children remain genuine bounds panics in the value body below.
    pub(crate) fn demanded_arguments(&self, arity: usize) -> usize {
        if matches!(self.operation, Operation::Trim(_)) {
            arity.min(2)
        } else {
            arity
        }
    }
    pub(crate) fn eval(&self, ctx: &dyn Columns, values: Vec<Datum>) -> Result<Datum, EvalError> {
        match &self.operation {
            Operation::Length(built) => built.eval_in(&values[0], ctx),
            Operation::Trim(direction) => {
                let value = &values[0];
                let remstr = &values[1];
                let binary = matches!(value, Datum::Bytes(_));
                let text = crate::coerce::coerce_str_bytes(value)?;
                let remove = crate::coerce::coerce_str_bytes(remstr)?;
                crate::string_fn::trim_value_in(text, remove, *direction, binary, ctx)
            }
            Operation::Locate { position, three } => {
                if *three {
                    let mut values = values.into_iter();
                    let substr = values.next().expect("locate substring");
                    let text = values.next().expect("locate string");
                    let position = crate::cast::cast_arg_as_int(
                        &values.next().expect("locate position"),
                        position.as_ref(),
                        ctx,
                    )?;
                    crate::string_fn::locate_with_position_in(
                        &[substr, text, position],
                        self.collation,
                        ctx,
                    )
                } else {
                    crate::string_fn::locate_in(&values[0], &values[1], self.collation, ctx)
                }
            }
            Operation::Instr => {
                crate::string_fn::locate_in(&values[1], &values[0], self.collation, ctx)
            }
            Operation::Strcmp => {
                crate::string_fn::strcmp_with_collation_in(&values, self.collation, ctx)
            }
            Operation::Field => {
                crate::string_fn::field_with_collation(&values, self.collation, ctx)
            }
        }
    }
}
