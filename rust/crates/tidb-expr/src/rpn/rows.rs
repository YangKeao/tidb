// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    cell::{OnceCell, RefCell},
    collections::HashMap,
};

use super::engine::StatementRand;
use super::{HostCall, HostOperation, InputBinding};
use tipb::FieldType as QueryFieldType;

use tidb_chunk::chunk::Chunk;

use crate::{context::EvalError, expression::Expression};
use tidb_datatype::{
    BinaryJSON, BinaryLiteral, BinaryLiteralWidth, Datum, FieldType, FieldTypeCode, MySqlDuration,
    MysqlEnum, StringDatum, Time, VectorFloat32,
};
use tidb_query_common::{Result, error::EvaluateError};
use tidb_query_datatype::{
    EvalType, FieldTypeAccessor,
    codec::{
        data_type::{ChunkedVec, ChunkedVecSet, Duration, Enum, Real, ScalarValueRef, VectorValue},
        mysql::{
            Decimal as QueryDecimal, DecimalDecoder, DecimalEncoder, Time as QueryTime,
            VectorFloat32 as QueryVector,
            decimal::DECIMAL_STRUCT_SIZE,
            json::{Json, JsonType},
        },
    },
    expr::EvalContext,
};
use tidb_query_expr::{RpnStackNode, RpnStackNodeVectorValue};

#[derive(Clone, Copy)]
pub(super) enum ReadMode {
    LogicalBatch,
    QuietIntBatch,
    PhysicalBatch,
    Scalar,
}

pub(super) type StatementSeedSlot = OnceCell<Option<super::StatementSeededRandom>>;

/// Exposes native physical values to the same RPN loop as decoded storage data.
pub(super) struct NativeRows<'a> {
    pub(super) chunk: &'a Chunk,
    pub(super) fields: &'a [FieldType],
    pub(super) column_paths: &'a [Vec<String>],
    pub(super) host_calls: &'a [HostCall],
    pub(super) seed_keys: &'a [usize],
    pub(super) statement_seeds: Option<&'a [StatementSeedSlot]>,
    pub(super) execution_seeds: &'a [super::StatementSeededRandom],
    pub(super) diagnostics: &'a [crate::scalar_function::ScalarFunction],
    pub(super) bindings: &'a [InputBinding],
    pub(super) binding_error: RefCell<Option<EvalError>>,
    pub(super) decimals: RefCell<HashMap<(usize, usize), Option<QueryDecimal>>>,
    pub(super) context: &'a dyn crate::Columns,
    pub(super) mode: ReadMode,
    pub(super) block_encryption_mode: crate::context::BlockEncryptionMode,
    pub(super) scalar_operands: bool,
}

impl NativeRows<'_> {
    pub(super) fn kernel_error(
        &self,
        slot: usize,
        arguments: &[RpnStackNode<'_>],
        error: tidb_query_common::Error,
    ) -> tidb_query_common::Error {
        use tidb_query_common::error::ErrorInner;
        if let ErrorInner::Evaluate(EvaluateError::Custom { code: 1690, msg }) = &*error.0 {
            if !self.scalar_operand_demand()
                && self.diagnostics[slot].func_name.lowercase() == "intdiv"
                && msg.contains(" / ")
            {
                return EvaluateError::Custom {
                    code: 1690,
                    msg: msg.replace(" / ", " DIV "),
                }
                .into();
            }
            let value = match msg.split(" value is out of range").next() {
                Some("BIGINT UNSIGNED") => "BIGINT UNSIGNED",
                Some("BIGINT") => "BIGINT",
                Some("DOUBLE" | "REAL") => "DOUBLE",
                Some("DECIMAL" | "Decimal") => "DECIMAL",
                _ if self.diagnostics[slot].numeric_operand_domain()
                    == Some(tidb_datatype::EvalType::Decimal) =>
                {
                    "DECIMAL"
                }
                _ => "BIGINT",
            };
            let scalar = self.scalar_operand_demand();
            let decimals = match arguments {
                [left, right] if scalar => {
                    let decimal = |value| match value {
                        ScalarValueRef::Decimal(Some(value)) => {
                            Some(tidb_datatype::Decimal::parse_mysql(&value.to_string()).0)
                        }
                        _ => None,
                    };
                    [
                        decimal(left.get_logical_scalar_ref(0)),
                        decimal(right.get_logical_scalar_ref(0)),
                    ]
                }
                _ => [None, None],
            };
            if let Some(diagnostic) = self.diagnostics[slot].shared_arithmetic_error(
                value,
                self.context,
                scalar,
                decimals.each_ref().map(Option::as_ref),
            ) {
                self.binding_error.replace(Some(diagnostic));
            }
        }
        error
    }

    pub(super) fn scalar_operand_demand(&self) -> bool {
        self.scalar_operands
    }

    fn block_encryption_mode(&self) -> crate::context::BlockEncryptionMode {
        self.block_encryption_mode
    }

    pub(super) fn next_random(&self) -> Option<Result<f64>> {
        Some(
            self.context
                .rand_next()
                .ok_or_else(|| error("RAND requires a session")),
        )
    }

    pub(super) fn seed_slot(&self, slot: usize) -> usize {
        self.seed_keys.get(slot).copied().unwrap_or(slot)
    }

    pub(super) fn statement_seeded_random(&self, slot: usize) -> Option<StatementRand<'_>> {
        self.statement_seeds
            .and_then(|slots| slots.get(slot))
            .and_then(|seed| {
                seed.get_or_init(|| self.context.statement_seeded_random(self.seed_slot(slot)))
                    .as_ref()
            })
            .or_else(|| self.execution_seeds.get(slot))
            .map(super::StatementSeededRandom::demand)
    }

    pub(super) fn evaluate_host_call(
        &self,
        slot: usize,
        field: &QueryFieldType,
        args: &[RpnStackNode<'_>],
        rows: usize,
    ) -> Result<VectorValue> {
        let call = self
            .host_calls
            .get(slot)
            .ok_or_else(|| error("unknown native call slot"))?;
        let mut output = output_vector(field, rows)?;
        for row in 0..rows {
            let value = match (&call.operation, args, call.arguments.as_slice()) {
                (HostOperation::BenchmarkCount { body_error }, [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    crate::func::benchmark_loop_count(value, Some(field), self.context).and_then(
                        |count| {
                            if let Some(error) = body_error
                                .as_ref()
                                .filter(|_| count.is_some_and(|count| count >= 0))
                            {
                                return Err(error.clone());
                            }
                            Ok(count.map_or(Datum::Null, Datum::Int))
                        },
                    )
                }
                (HostOperation::PrepareJsonSchema(validation), [schema], [field]) => {
                    let schema = from_query_value(schema.get_logical_scalar_ref(row), field)?;
                    validation.prepare_operand(&schema, self.context)
                }
                (HostOperation::ValidateJsonSchema(validation), [_, document], [_, field]) => {
                    let document = from_query_value(document.get_logical_scalar_ref(row), field)?;
                    validation.validate_operand(&document)
                }
                (HostOperation::RejectVectorJsonCast, [], []) => {
                    Err(crate::scalar_function::ScalarFunction::vector_json_cast_error())
                }
                (HostOperation::TemporalFsp(fsp), [value], [field]) => {
                    match from_query_value(value.get_logical_scalar_ref(row), field)? {
                        Datum::Time(mut value) => {
                            let _ = value.set_fsp(*fsp);
                            Ok(Datum::Time(value))
                        }
                        Datum::Duration(value) => Ok(Datum::Duration(
                            MySqlDuration::from_raw_parts(value.nanoseconds(), *fsp),
                        )),
                        value => Ok(value),
                    }
                }
                (HostOperation::JsonCast { parse }, [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    let value = match value {
                        Datum::Int(value) if field.code() == FieldTypeCode::Year => {
                            Datum::UInt(value as u64)
                        }
                        value @ (Datum::Enum(..) | Datum::Set(..)) => {
                            Datum::new_bytes_datum(value.sql_bytes().map_err(error)?)
                        }
                        value => value,
                    };
                    if *parse {
                        crate::builtin_ext::cast_as_json_typed(&value, Some(field))
                    } else {
                        crate::builtin_ext::cast_as_json_value_typed(&value, Some(field))
                    }
                }
                (HostOperation::NumericCast(target), [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    crate::scalar_function::cast_numeric_value(
                        field,
                        value,
                        *target,
                        self.context,
                        !self.scalar_operands,
                        || None,
                    )
                }
                (HostOperation::SqlCast(target), [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    if value.is_null() {
                        Ok(value)
                    } else {
                        crate::cast::eval_cast(target, value, Some(field), self.context)
                    }
                }
                (HostOperation::DeferredValue(target), [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    crate::constant::Constant::convert_deferred_value(
                        value,
                        Some(target),
                        self.context,
                    )
                    .map_err(|(_, error)| error)
                }
                (HostOperation::InvalidArity(name), [], []) => {
                    Err(EvalError::WrongParameterCount(name))
                }
                (HostOperation::AdvisoryLockName, [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    crate::scalar_function::advisory_lock_name_value(
                        &value,
                        Some(field),
                        self.context,
                    )
                    .map(Datum::new_string_datum)
                }
                (HostOperation::LoadFile, [_], [_]) => Ok(Datum::Null),
                (HostOperation::RejectVectorCast, [value], [field]) => {
                    let value = from_query_value(value.get_logical_scalar_ref(row), field)?;
                    if value.is_null() {
                        Ok(value)
                    } else {
                        let source = tidb_datatype::type_str(field.code());
                        Err(EvalError::Vector(format!(
                            "cannot cast from {source} to vector"
                        )))
                    }
                }
                (HostOperation::LocalBuiltin(function), _, fields)
                    if args.len() == fields.len() =>
                {
                    let values = args
                        .iter()
                        .zip(fields)
                        .map(|(argument, field)| {
                            from_query_value(argument.get_logical_scalar_ref(row), field)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    function.evaluate(values, self.context, !self.scalar_operand_demand())
                }
                _ => return Err(error("invalid native call operands")),
            }
            .map_err(|native| self.native_error(native))?;
            append(&mut output, value)?;
        }
        Ok(output)
    }

    pub(super) fn read_column<'a>(
        &'a self,
        _ctx: &mut EvalContext,
        offset: usize,
        result_type: &'a QueryFieldType,
        rows: &'a [usize],
    ) -> Result<RpnStackNode<'a>> {
        let mut vector = output_vector(result_type, rows.len())?;
        if let Some(path) = self.column_paths.get(offset) {
            let value = self
                .context
                .get(path)
                .ok_or_else(|| self.native_error(EvalError::Unsupported("unknown column")))?;
            for _ in rows {
                append(&mut vector, value.clone())?;
            }
        } else if let Some(field) = self.fields.get(offset) {
            match &mut vector {
                VectorValue::Int(values) if field.code().is_type_integer() => {
                    let column = self.chunk.column(offset);
                    for &row in rows {
                        let row = self.row(row).idx();
                        values.push((!column.is_null(row)).then(|| column.get_int64(row)));
                    }
                }
                VectorValue::Decimal(values) if field.code() == FieldTypeCode::NewDecimal => {
                    use std::collections::hash_map::Entry;
                    let mut cache = self.decimals.borrow_mut();
                    let column = self.chunk.column(offset);
                    for &row in rows {
                        let row = self.row(row).idx();
                        let datum = match cache.entry((offset, row)) {
                            Entry::Occupied(entry) => entry.get().clone(),
                            Entry::Vacant(entry) => {
                                let datum = if column.is_null(row) {
                                    None
                                } else {
                                    let native = tidb_datatype::Decimal::from_my_decimal(
                                        &column.get_my_decimal(row),
                                    );
                                    Some(to_query_decimal(&native)?)
                                };
                                entry.insert(datum).clone()
                            }
                        };
                        values.push(datum);
                    }
                }
                _ => {
                    for &row in rows {
                        append(&mut vector, self.row(row).get_datum(offset, field))?;
                    }
                }
            }
        } else {
            let binding = &self.bindings[offset - self.fields.len()];
            let binding_error = |native| self.native_error(native);
            if let super::InputSource::HybridInteger(expression) = &binding.source {
                for &row in rows {
                    let value = crate::scalar_function::eval_numeric_row(
                        expression,
                        self.context,
                        self.row(row),
                        tidb_datatype::EvalType::Int,
                    )
                    .map_err(binding_error)?;
                    append(&mut vector, value)?;
                }
            } else if let super::InputSource::Context(source) = &binding.source {
                for _ in rows {
                    append(
                        &mut vector,
                        source.read(self.context).map_err(binding_error)?,
                    )?;
                }
            } else {
                let value = match &binding.source {
                    super::InputSource::Expression(Expression::Constant(constant)) => {
                        constant.eval_in(self.context)
                    }
                    super::InputSource::Expression(Expression::CorrelatedColumn(column)) => {
                        Ok(column.eval())
                    }
                    _ => unreachable!(),
                }
                .map_err(binding_error)?;
                for _ in rows {
                    append(&mut vector, value.clone())?;
                }
            }
        }
        Ok(RpnStackNode::Vector {
            value: RpnStackNodeVectorValue::Generated {
                physical_value: vector,
            },
            field_type: result_type,
        })
    }
}

impl NativeRows<'_> {
    fn row(&self, index: usize) -> tidb_chunk::row::Row<'_> {
        match self.mode {
            ReadMode::LogicalBatch | ReadMode::QuietIntBatch => self.chunk.get_row(index),
            ReadMode::PhysicalBatch | ReadMode::Scalar => self.chunk.physical_row(index),
        }
    }

    pub(super) fn native_error(&self, native: EvalError) -> tidb_query_common::Error {
        let _ = self.binding_error.replace(Some(native));
        error("native expression binding failed")
    }
}

fn output_vector(field: &QueryFieldType, rows: usize) -> Result<VectorValue> {
    let kind = EvalType::try_from(field.as_accessor().tp()).map_err(error)?;
    Ok(VectorValue::with_capacity(rows, kind))
}

fn append(vector: &mut VectorValue, value: Datum) -> Result<()> {
    match (vector, value) {
        (VectorValue::Int(values), Datum::Int(value)) => values.push_data(value),
        (VectorValue::Int(values), Datum::UInt(value)) => values.push_data(value as i64),
        (VectorValue::Real(values), Datum::Real(value) | Datum::Float32(value)) => {
            values.push_data(Real::new(value).map_err(error)?);
        }
        (VectorValue::Duration(values), Datum::Duration(value)) => {
            let fsp = i8::try_from(value.fsp()).map_err(error)?;
            values.push_data(Duration::from_nanos(value.nanoseconds(), fsp)?);
        }
        (VectorValue::Bytes(values), Datum::Bytes(value)) => values.push_data(value),
        (VectorValue::Bytes(values), Datum::String(value)) => values.push_data(value.into_bytes()),
        (VectorValue::Bytes(values), Datum::BinaryLiteral(value)) => {
            values.push_data(value.as_bytes().to_vec());
        }
        (VectorValue::Int(values), Datum::Bit(value)) => {
            let integer = value.to_int();
            if integer.is_truncated() {
                return Err(error("BIT column value exceeds 64 bits"));
            }
            values.push_data(integer.value() as i64);
        }
        (VectorValue::Int(values), Datum::Enum(value, _)) => values.push_data(value.value() as i64),
        (VectorValue::Int(values), Datum::Set(value, _)) => values.push_data(value.value() as i64),
        (VectorValue::Decimal(values), Datum::Decimal(value)) => {
            values.push_data(to_query_decimal(&value)?);
        }
        (VectorValue::DateTime(values), Datum::Time(value)) => {
            values.push_data(to_query_time(value)?);
        }
        (VectorValue::Enum(values), Datum::Enum(value, _)) => {
            values.push_data(Enum::new(value.name_bytes().to_vec(), value.value()));
        }
        (VectorValue::Enum(values), Datum::Set(value, _)) => {
            values.push_data(Enum::new(value.name_bytes().to_vec(), value.value()));
        }
        (VectorValue::Set(values), Datum::Set(value, _)) => {
            let mut names = tikv_util::buffer_vec::BufferVec::new();
            for name in value.name_bytes().split(|&byte| byte == b',') {
                names.push(name);
            }
            values.push_data(tidb_query_datatype::codec::data_type::Set::new(
                std::sync::Arc::new(names),
                value.value(),
            ));
        }
        (VectorValue::Json(values), Datum::Json(value)) => {
            values.push_data(Json::new(
                JsonType::try_from(value.type_code())?,
                value.value().to_vec(),
            ));
        }
        (VectorValue::VectorFloat32(values), Datum::VectorFloat32(value)) => {
            values.push_data(QueryVector::from_f32(value.elements().to_vec())?);
        }
        (vector, Datum::Null) => super::engine::append_null(vector),
        (vector, actual) => {
            return Err(error(format_args!(
                "cannot read native datum {:?} as {}",
                actual.kind(),
                vector.eval_type()
            )));
        }
    }
    Ok(())
}

/// Read an RPN value according to the native caller's result declaration.
pub(super) fn from_query_value(value: ScalarValueRef<'_>, field: &FieldType) -> Result<Datum> {
    Ok(match value {
        ScalarValueRef::Int(Some(value)) if field.code() == FieldTypeCode::Bit => {
            let width = u8::try_from((field.flen() + 7) / 8)
                .ok()
                .and_then(|width| BinaryLiteralWidth::try_from(width).ok());
            Datum::Bit(BinaryLiteral::from_uint(*value as u64, width))
        }
        ScalarValueRef::Int(Some(value)) if field.is_unsigned() => Datum::UInt(*value as u64),
        ScalarValueRef::Int(Some(value)) => Datum::Int(*value),
        ScalarValueRef::Real(Some(value)) if field.code() == FieldTypeCode::Float => {
            Datum::Float32(value.into_inner())
        }
        ScalarValueRef::Real(Some(value)) => Datum::Real(value.into_inner()),
        ScalarValueRef::Bytes(Some(value)) if field.is_binary_string() => {
            Datum::Bytes(value.to_vec())
        }
        ScalarValueRef::Bytes(Some(value)) => {
            Datum::String(StringDatum::new(value, field.collation()))
        }
        ScalarValueRef::Duration(Some(value)) => Datum::Duration(MySqlDuration::from_raw_parts(
            value.to_nanos(),
            i64::from(value.fsp()),
        )),
        ScalarValueRef::DateTime(Some(value)) => {
            use tidb_query_datatype::codec::mysql::TimeEncoder;
            let mut raw = [0; 8];
            raw.as_mut_slice().write_time(*value)?;
            Datum::Time(Time::from_go_raw(u64::from_le_bytes(raw)).map_err(error)?)
        }
        ScalarValueRef::Decimal(Some(value)) => {
            let mut encoded = Vec::new();
            let (precision, scale) = value.prec_and_frac();
            encoded.write_decimal(value, precision.max(1), scale)?;
            let (_, decoded, _, _) = tidb_codec::decode_decimal(&encoded).map_err(error)?;
            Datum::Decimal(decoded.with_result_scale(u32::from(decimal_result_scale(value)?)))
        }
        ScalarValueRef::Enum(Some(value)) if field.code() == FieldTypeCode::Set => Datum::Set(
            tidb_datatype::MysqlSet::new(value.name(), value.value()),
            field.collation(),
        ),
        ScalarValueRef::Enum(Some(value)) => Datum::Enum(
            MysqlEnum::new(value.name(), value.value()),
            field.collation(),
        ),
        ScalarValueRef::Set(Some(value)) => Datum::Set(
            tidb_datatype::parse_set_value(&field.elems_snapshot(), value.value())
                .map_err(error)?,
            field.collation(),
        ),
        ScalarValueRef::Json(Some(value)) => Datum::Json(BinaryJSON::from_encoded_parts(
            value.get_type() as u8,
            value.value(),
        )),
        ScalarValueRef::VectorFloat32(Some(value)) => {
            use tidb_query_datatype::codec::mysql::VectorFloat32Encoder;
            let mut encoded = Vec::with_capacity(value.encoded_len());
            encoded.write_vector_float32(value)?;
            let (decoded, _) =
                tidb_datatype::deserialize_vector_float32(&encoded).map_err(error)?;
            Datum::VectorFloat32(decoded)
        }
        _ => Datum::Null,
    })
}

pub(super) fn query_field_type(field: &FieldType) -> Option<QueryFieldType> {
    Some(query_field_from_wire(super::native_field_declaration(
        field,
    )?))
}

pub(super) fn query_field_from_wire(wire: tidb_proto::tipb::FieldType) -> QueryFieldType {
    let mut declaration = QueryFieldType::new();
    declaration.set_tp(wire.tp());
    declaration.set_flag(wire.flag());
    declaration.set_flen(wire.flen());
    declaration.set_decimal(wire.decimal());
    declaration.set_collate(wire.collate());
    declaration.set_array(wire.array());
    declaration.set_charset(wire.charset.unwrap_or_default());
    declaration.set_elems(wire.elems.into());
    declaration
}

fn error(error: impl std::fmt::Display) -> tidb_query_common::Error {
    EvaluateError::Other(error.to_string()).into()
}

fn to_query_decimal(value: &tidb_datatype::Decimal) -> Result<QueryDecimal> {
    let mut encoded = Vec::new();
    let decimal = match tidb_codec::encode_decimal_fixed(&mut encoded, value, 0, 0) {
        Ok(()) => encoded.as_slice().read_decimal()?,
        Err(codec_error) => {
            // Native values can hold arithmetic precision above the
            // wire encoder's scale limit. Parse the exact coefficient.
            let storage_scale = value.storage_scale();
            let full = value.clone().with_result_scale(storage_scale).to_string();
            let outcome = QueryDecimal::from_bytes(full.as_bytes())?;
            if !outcome.is_ok() {
                return Err(error(codec_error));
            }
            outcome.unwrap()
        }
    };
    let scale = u8::try_from(value.scale()).map_err(error)?;
    if decimal.frac_cnt() == scale {
        return Ok(decimal);
    }
    let mut raw = decimal_chunk_bytes(&decimal)?;
    raw[DECIMAL_RESULT_SCALE] = scale;
    Ok(raw.as_slice().read_decimal_from_chunk()?)
}

// At the pinned TiKV revision its public chunk codec writes Decimal's repr(C)
// layout. Its first four bytes are integer digits, stored fractional digits,
// result scale and sign. Only the result byte can change here.
const DECIMAL_RESULT_SCALE: usize = 2;

fn decimal_chunk_bytes(value: &QueryDecimal) -> Result<[u8; DECIMAL_STRUCT_SIZE]> {
    let mut raw = [0; DECIMAL_STRUCT_SIZE];
    raw.as_mut_slice().write_decimal_to_chunk(value)?;
    Ok(raw)
}

pub(super) fn decimal_result_scale(value: &QueryDecimal) -> Result<u8> {
    Ok(decimal_chunk_bytes(value)?[DECIMAL_RESULT_SCALE])
}

fn to_query_time(value: Time) -> Result<QueryTime> {
    use tidb_query_datatype::codec::mysql::TimeDecoder;
    if value.fsp() > 6 {
        return Err(error("invalid native time fractional precision"));
    }
    value
        .go_raw()
        .to_le_bytes()
        .as_slice()
        .read_time_from_chunk()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{CoreTime, TimeType};

    #[test]
    fn decimal_chunk_bridge_keeps_coefficient_and_result_scale_independent() {
        let native =
            tidb_datatype::Decimal::from_literal("0.333333333333333333").with_result_scale(3);
        let query = to_query_decimal(&native).unwrap();
        assert_eq!(query.to_string(), "0.333");
        assert_eq!(query.frac_cnt(), 18);
        let Datum::Decimal(restored) = from_query_value(
            ScalarValueRef::Decimal(Some(&query)),
            &FieldType::new(FieldTypeCode::NewDecimal),
        )
        .unwrap() else {
            panic!("expected a decimal");
        };
        assert_eq!(restored.scale(), 3);
        assert_eq!(
            restored.with_result_scale(18).to_string(),
            "0.333333333333333333"
        );
    }

    #[test]
    fn time_bridge_retains_raw_zero_date_and_hidden_date_fields() {
        let core = CoreTime::from_date(2024, 3, 10, 1, 30, 15, 123_456);
        for time in [
            Time::from_go_raw(0xf).unwrap(),
            Time::new(core, TimeType::Date, 0).unwrap(),
            Time::new(core, TimeType::DateTime, 6).unwrap(),
            Time::new(core, TimeType::Timestamp, 6).unwrap(),
        ] {
            let field = FieldType::new(FieldTypeCode::Datetime);
            let adapted = to_query_time(time).unwrap();
            let Datum::Time(restored) =
                from_query_value(ScalarValueRef::DateTime(Some(&adapted)), &field).unwrap()
            else {
                panic!("expected a time value")
            };
            assert_eq!(restored.go_raw(), time.go_raw());
        }
    }
}
