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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tidb_query_expr::decode_native_cast_real_unsigned_result;

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{evaluate_args_in, identity_value, EvaluatedArgs, EvaluatedBytesOp};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid real-to-unsigned CAST result report",
    ))
}

fn project_report(report: Option<Vec<u8>>, ctx: &dyn Columns) -> Result<u64, EvalError> {
    let report = report.ok_or_else(invalid_report)?;
    let report = decode_native_cast_real_unsigned_result(&report).ok_or_else(invalid_report)?;
    if let Some(bits) = report.overflow_bits {
        ctx.append_warning(
            1690,
            &format!(
                "constant {} overflows bigint",
                tidb_datatype::format_float_g_shortest(f64::from_bits(bits)),
            ),
        );
    }
    Ok(report.value)
}

pub(crate) fn eval_cast_real_unsigned_in(
    ctx: &dyn Columns,
    actual: &Datum,
) -> Result<u64, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::CastRealUnsignedNative,
        ctx,
        || Ok(EvaluatedArgs::Bytes(identity_value::encode(actual)?)),
        |computed| project_report(computed.into_bytes()?, ctx),
    )
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ErrorLevel, ExpressionAdapterFailureClass};

    #[test]
    fn real_unsigned_cast_keeps_sdk_rounding_warning_projection_and_admission() {
        #[derive(Default)]
        struct Warnings {
            values: RefCell<Vec<(u16, String)>>,
            level_reads: Cell<usize>,
            panic_on_warning: Cell<bool>,
        }
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> ErrorLevel {
                self.level_reads.set(self.level_reads.get() + 1);
                ErrorLevel::Error
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.values.borrow_mut().push((code, message.to_owned()));
                assert!(!self.panic_on_warning.get(), "CAST warning callback panic");
            }
        }
        let below_upper = f64::from_bits((u64::MAX as f64).to_bits() - 1);
        let cases = [
            (Datum::Real(2.5), 2, None),
            (Datum::Real(-0.4), 0, None),
            (Datum::Real(-0.0), 0, None),
            (Datum::Real(-1.5), u64::MAX - 1, Some(-2.0)),
            (Datum::Real(-1e300), 1u64 << 63, Some(-1e300)),
            (
                Datum::Real(u64::MAX as f64),
                u64::MAX,
                Some(u64::MAX as f64),
            ),
            (Datum::Real(f64::INFINITY), u64::MAX, Some(f64::INFINITY)),
            (
                Datum::Real(f64::NEG_INFINITY),
                1u64 << 63,
                Some(f64::NEG_INFINITY),
            ),
            (
                Datum::Real(f64::from_bits(0x7ff8_0000_0000_1234)),
                u64::MAX,
                Some(f64::NAN),
            ),
            (Datum::Float32(16_777_217.0), 16_777_217, None),
            (Datum::Float32(below_upper), u64::MAX - 2047, None),
        ];
        for slots in [0, 1] {
            let native = Warnings::default();
            let policy =
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                    .unwrap();
            let owner = AsciiPoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&native, |bound| {
                for (actual, expected, overflow) in &cases {
                    let result = eval_cast_real_unsigned_in(bound, actual);
                    if slots == 0 {
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                        assert!(
                            native.values.borrow().is_empty(),
                            "no SDK report means no projected warning"
                        );
                    } else {
                        assert_eq!(result, Ok(*expected));
                        let expected = overflow
                            .map(|rounded| {
                                (
                                    1690,
                                    format!(
                                        "constant {} overflows bigint",
                                        tidb_datatype::format_float_g_shortest(rounded)
                                    ),
                                )
                            })
                            .into_iter()
                            .collect::<Vec<_>>();
                        assert_eq!(native.values.replace(Vec::new()), expected);
                    }
                    assert_eq!(
                        native.level_reads.get(),
                        0,
                        "append_warning must not become a strict-policy error"
                    );
                }
                // Report validation is structural; invalid replies cannot be
                // treated as NULL, a numeric fallback, or a business warning.
                for report in [
                    None,
                    Some(vec![]),
                    Some(vec![0; 8]),
                    Some(vec![2; 9]),
                    Some(vec![1; 16]),
                ] {
                    assert!(matches!(project_report(report, bound),
                        Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::ScopeContract));
                    assert!(native.values.borrow().is_empty());
                }
                if slots == 1 {
                    native.panic_on_warning.set(true);
                    let panic = catch_unwind(AssertUnwindSafe(|| {
                        eval_cast_real_unsigned_in(bound, &Datum::Real(-1.5))
                    }));
                    assert!(panic.is_err());
                    native.panic_on_warning.set(false);
                    assert!(
                        matches!(eval_cast_real_unsigned_in(bound, &Datum::Real(1.0)),
                        Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned)
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        assert_eq!(
            eval_cast_real_unsigned_in(&crate::NoColumns, &Datum::Float32(16_777_217.0)),
            Ok(16_777_217)
        );
        let native = Warnings::default();
        assert_eq!(
            eval_cast_real_unsigned_in(&native, &Datum::Real(-1.5)),
            Ok(u64::MAX - 1)
        );
        assert_eq!(
            native.values.into_inner(),
            [(1690, "constant -2 overflows bigint".to_owned())]
        );
        assert_eq!(native.level_reads.get(), 0);
    }
}
