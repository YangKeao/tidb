// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual selected JSON value implementation, original argument declarations,
//! and the original owner's live path cache. No expression evaluation is stored.
use super::ScalarFunction;
use crate::expression::Expression;
use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;
use tidb_proto::tipb::ScalarFuncSig;

#[derive(Debug)]
pub(crate) struct PbJsonValues {
    signature: ScalarFuncSig,
    argument_types: Box<[Option<FieldType>]>,
    paths: Option<crate::builtin_ext::BuiltinFuncCache<Option<Vec<crate::builtin_ext::JsonPath>>>>,
}
impl PbJsonValues {
    pub(crate) fn capture(function: &ScalarFunction) -> Option<Self> {
        let signature = function.pb_json_signature()?;
        let cache_paths = function.args.get(1..).is_some_and(|arguments| {
            !arguments.is_empty()
                && arguments
                    .iter()
                    .step_by(2)
                    .all(|arg| arg.const_level() >= crate::expression::ConstLevel::ONLY_IN_CONTEXT)
        });
        Some(Self {
            signature,
            argument_types: function
                .args
                .iter()
                .map(Expression::static_type)
                .map(|field| field.cloned())
                .collect(),
            paths: cache_paths.then(|| function.json_modify_path_cache.share_for_invocation()),
        })
    }
    pub(crate) fn signature(&self) -> ScalarFuncSig {
        self.signature
    }
    pub(crate) fn eval_values(
        &self,
        values: &[Datum],
        context: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        crate::builtin_ext::json::eval_pb(
            self.signature,
            values,
            &self.argument_types,
            context,
            self.paths.as_ref(),
        )
    }
}
