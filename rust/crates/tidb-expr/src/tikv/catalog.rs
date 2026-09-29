// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Signature facts and closed seed admission, never a second kernel selector.

use protobuf::ProtobufEnum;
use tidb_proto::tipb::ScalarFuncSig;
use tidb_query_expr::local::FunctionRef;

use crate::pushdown_catalog::{conditional_signature, CATALOG};
use crate::scalar_function::ScalarFunction;

use super::{SeedError, SeedResult};

pub(super) fn int_control(function: &ScalarFunction) -> SeedResult<FunctionRef> {
    if function.has_values_offset() || function.has_grouping_metadata() {
        return Err(SeedError::Admission(
            "opaque native call metadata is outside IntControlSeed",
        ));
    }
    let signature = if let Some(signature) = function.pb_signature() {
        // A display-name mutation must not change PB-selected execution. Nor
        // may an internal from_pb construction stand in for real ingestion.
        let origin = function.pb_origin().ok_or(SeedError::Admission(
            "PB-selected function lacks ingestion provenance",
        ))?;
        if origin.signature != Some(signature as i32) {
            return Err(SeedError::Admission(
                "stale or missing PB signature provenance",
            ));
        }
        signature
    } else {
        if function.pb_origin().is_some() {
            return Err(SeedError::Admission("PB origin lost its selected builtin"));
        }
        let name = function.func_name.lowercase();
        match name {
            "case" | "if" | "ifnull" | "coalesce" => conditional_signature(
                name,
                function.args.len(),
                function
                    .ret_type
                    .as_ref()
                    .ok_or(SeedError::Admission("missing result type"))?
                    .eval_type(),
            ),
            // Use the existing SQL signature row, not another name-to-ID map.
            "and" | "or" if function.args.len() == 2 => CATALOG
                .iter()
                .find(|row| row.name == name)
                .map(|row| row.sig),
            _ => None,
        }
        .ok_or(SeedError::Admission("function is outside IntControlSeed"))?
    };
    // This is admission/arity checking, not an ID-to-kernel dispatch. TiKV's
    // official selector remains the only implementation mapping.
    let admitted = match signature {
        ScalarFuncSig::LogicalAnd | ScalarFuncSig::LogicalOr | ScalarFuncSig::IfNullInt => {
            function.args.len() == 2
        }
        ScalarFuncSig::IfInt => function.args.len() == 3,
        ScalarFuncSig::CaseWhenInt => function.args.len() >= 2,
        ScalarFuncSig::CoalesceInt => !function.args.is_empty(),
        _ => false,
    };
    if !admitted {
        return Err(SeedError::Admission(
            "signature/arity is outside IntControlSeed",
        ));
    }
    // Numeric identity passes through the generated enum conversion. In
    // particular PlusInt=203 is never substituted with a signed-specific ID.
    let signature = tipb::ScalarFuncSig::from_i32(signature as i32).ok_or(SeedError::Admission(
        "official TiKV signature enum has no matching ID",
    ))?;
    Ok(FunctionRef::TiPb(signature))
}

/// Pure SQL Datum-control signature facts. Native provenance, actual SQL types
/// and child consumer roles are proved by the separately bounded caller. This
/// does not change the older IntControlSeed/PB/PLUS entrypoints.
pub(super) fn datum_control(function: &ScalarFunction) -> SeedResult<FunctionRef> {
    if function.pb_signature().is_some()
        || function.pb_origin().is_some()
        || function.has_values_offset()
        || function.has_grouping_metadata()
    {
        return Err(SeedError::Admission("nonordinary Datum control metadata"));
    }
    let result = function
        .ret_type
        .as_ref()
        .ok_or(SeedError::Admission("missing Datum control result type"))?
        .eval_type();
    if !matches!(
        result,
        tidb_datatype::EvalType::Int | tidb_datatype::EvalType::String
    ) {
        return Err(SeedError::Admission("Datum control result family"));
    }
    let name = function.func_name.lowercase();
    let signature = match name {
        "case" | "if" | "ifnull" | "coalesce" => {
            conditional_signature(name, function.args.len(), result)
        }
        "and" | "or" if function.args.len() == 2 && result == tidb_datatype::EvalType::Int => {
            CATALOG
                .iter()
                .find(|row| row.name == name)
                .map(|row| row.sig)
        }
        _ => None,
    }
    .ok_or(SeedError::Admission("outside Datum control grammar/arity"))?;
    let signature = tipb::ScalarFuncSig::from_i32(signature as i32)
        .ok_or(SeedError::Admission("missing official control signature"))?;
    Ok(FunctionRef::TiPb(signature))
}

/// Closed PLUS-row admission, not a kernel selector or a remote serializer.
/// The caller additionally proves homogeneous ingestion and full node types.
pub(super) fn int_plus_row(function: &ScalarFunction) -> SeedResult<FunctionRef> {
    if function.args.len() != 2 || function.has_values_offset() || function.has_grouping_metadata()
    {
        return Err(SeedError::Admission("PLUS row arity/opaque metadata"));
    }
    super::lower::signed_longlong(
        function
            .ret_type
            .as_ref()
            .ok_or(SeedError::Admission("missing PLUS result type"))?,
    )?;
    for argument in &function.args {
        super::lower::signed_longlong(
            argument
                .static_type()
                .ok_or(SeedError::Admission("missing PLUS argument type"))?,
        )?;
    }
    let signature = if let Some(signature) = function.pb_signature() {
        let origin = function
            .pb_origin()
            .ok_or(SeedError::Admission("PLUS lacks PB ingestion"))?;
        if origin.signature != Some(signature as i32) {
            return Err(SeedError::Admission("stale PLUS PB signature"));
        }
        signature
    } else {
        if function.pb_origin().is_some() || function.func_name.lowercase() != "plus" {
            return Err(SeedError::Admission("not an ordinary typed PLUS"));
        }
        // Read the actual signed/signed SQL signature row. No fabricated
        // PbScalar arguments, return-type formula, or ID-to-kernel mapping.
        CATALOG
            .iter()
            .find(|row| {
                row.name == "plus"
                    && row.selector.len() == 2
                    && row.selector.iter().all(|arg| {
                        arg.eval == Some(tidb_datatype::EvalType::Int)
                            && arg.unsigned == Some(false)
                            && arg.binary_string.is_none()
                    })
                    && row.arg_types.len() == 2
                    && row
                        .arg_types
                        .iter()
                        .all(|ty| *ty == tidb_datatype::EvalType::Int)
                    && row.ret == tidb_datatype::EvalType::Int
            })
            .map(|row| row.sig)
            .ok_or(SeedError::Admission("missing signed PLUS catalog fact"))?
    };
    if signature != ScalarFuncSig::PlusInt {
        return Err(SeedError::Admission(
            "PLUS row requires exact signature 203",
        ));
    }
    let signature = tipb::ScalarFuncSig::from_i32(signature as i32)
        .ok_or(SeedError::Admission("missing official PLUS signature"))?;
    Ok(FunctionRef::TiPb(signature))
}
