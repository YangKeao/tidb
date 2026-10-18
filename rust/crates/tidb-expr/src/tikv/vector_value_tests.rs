// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Frozen original vector SQL observations and actual-value regression checks.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::VectorFloat32;
pub(crate) const NAMES: [&str; 8] = [
    "VEC_DIMS",
    "VEC_L1_DISTANCE",
    "VEC_L2_DISTANCE",
    "VEC_NEGATIVE_INNER_PRODUCT",
    "VEC_COSINE_DISTANCE",
    "VEC_L2_NORM",
    "VEC_FROM_TEXT",
    "VEC_AS_TEXT",
];
pub(crate) fn arity(name: &str) -> usize {
    if name.ends_with("DISTANCE") || name == "VEC_NEGATIVE_INNER_PRODUCT" {
        2
    } else {
        1
    }
}
pub(crate) fn original(
    name: &str,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::builtin_ext::vec::dispatch_in(name, values, ctx).unwrap()
}
pub(crate) fn values() -> Vec<Datum> {
    let mut out = super::duration_construction_value_tests::values();
    out.extend(
        [
            "[]",
            "[1]",
            "[1,2]",
            "[-0,0]",
            "[3,4]",
            "[1,2,3]",
            "[1e38,-1e38]",
            "[1e999]",
            "[NaN]",
            "[Infinity]",
            "[1,]",
            "[\"1\"]",
            "null",
            "[true]",
            " [ 1 , 2 ] ",
            "[1e-45]",
        ]
        .into_iter()
        .map(Datum::new_string),
    );
    for bits in [
        vec![],
        vec![0],
        vec![0x80000000],
        vec![0x3f800000, 0x40000000],
        vec![0x7f7fffff, 0xff7fffff],
        vec![0x7fc01234, 0x3f800000],
        vec![0x7f800000],
        vec![0xff800000],
        vec![1, 0x80000001],
    ] {
        let mut v = VectorFloat32::init(bits.len());
        for (a, b) in v.elements_mut().iter_mut().zip(bits) {
            *a = f32::from_bits(b);
        }
        out.push(Datum::VectorFloat32(v));
    }
    out
}
pub(crate) fn inputs(name: &str) -> Vec<Vec<Datum>> {
    let v = values();
    if arity(name) == 1 {
        v.into_iter().map(|v| vec![v]).collect()
    } else {
        let mut out = vec![];
        for a in &v {
            for b in &v {
                out.push(vec![a.clone(), b.clone()]);
            }
        }
        out
    }
}
#[test]
fn vector_original_observations() {
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for name in NAMES {
            for values in inputs(name) {
                let ctx = super::calendar_value_tests::context(profile, 0, 0);
                let result = super::duration_construction_value_tests::observation(
                    || original(name, &values, &ctx),
                    &ctx,
                );
                let raw = if let Ok(Datum::Real(v)) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        original(name, &values, &ctx)
                    }))
                    .unwrap_or(Err(EvalError::Unsupported("panic")))
                {
                    format!("/{:016X}", v.to_bits())
                } else {
                    String::new()
                };
                for b in result.bytes().chain(raw.bytes()) {
                    hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                }
                count += 1;
            }
        }
    }
    eprintln!("VECTOR ORIGINAL {count} {hash:016X}");
    assert_eq!((count, hash), (207504, 0xD0CDB9F6BB8FB5EB));
}
#[test]
fn vector_never_requests_ready() {
    let mut failures = vec![];
    for name in NAMES {
        let mut ctx = super::calendar_value_tests::context(1, 0, 0);
        ctx.reject = true;
        let values = if arity(name) == 1 {
            vec![Datum::new_string("[1,2]")]
        } else {
            vec![Datum::new_string("[1,2]"), Datum::new_string("[3,4]")]
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original(name, &values, &ctx)
        }))
        .is_err()
        {
            failures.push(name);
        }
    }
    assert!(failures.is_empty(), "vector Ready {failures:?}");
}
pub(crate) fn op(name: &str) -> tidb_query_expr::NativeVectorOperation {
    use tidb_query_expr::NativeVectorOperation as O;
    match name {
        "VEC_DIMS" => O::Dims,
        "VEC_L1_DISTANCE" => O::L1Distance,
        "VEC_L2_DISTANCE" => O::L2Distance,
        "VEC_NEGATIVE_INNER_PRODUCT" => O::NegativeInnerProduct,
        "VEC_COSINE_DISTANCE" => O::CosineDistance,
        "VEC_L2_NORM" => O::L2Norm,
        "VEC_FROM_TEXT" => O::FromText,
        "VEC_AS_TEXT" => O::AsText,
        _ => panic!("unknown vector"),
    }
}

#[test]
fn vector_values_bound_demanded_work_without_copying_ignored_storage() {
    use super::vector_value::{eval_refs, eval_source};
    use tidb_query_expr::NativeVectorOperation as O;
    let huge = Datum::VectorFloat32(VectorFloat32::init(1 << 18));
    assert_eq!(eval_refs(O::Dims, &[&huge], 0), Ok(Datum::Int(262144)));
    for op in [
        O::L1Distance,
        O::L2Distance,
        O::NegativeInnerProduct,
        O::CosineDistance,
    ] {
        let ignored = Datum::Bytes(vec![255; 1 << 20]);
        assert_eq!(eval_refs(op, &[&Datum::Null, &ignored], 0), Ok(Datum::Null));
        assert_eq!(eval_refs(op, &[&huge, &Datum::Null], 0), Ok(Datum::Null));
        let error = original(
            op.name().to_ascii_uppercase().as_str(),
            &[Datum::new_string("bad"), Datum::Null],
            &crate::NoColumns,
        )
        .unwrap_err();
        assert_eq!(
            eval_refs(op, &[&Datum::new_string("bad"), &Datum::Null], 8192),
            Err(error)
        );
        let small = Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0]));
        let err = original(
            op.name().to_ascii_uppercase().as_str(),
            &[huge.clone(), small.clone()],
            &crate::NoColumns,
        )
        .unwrap_err();
        assert_eq!(eval_refs(op, &[&huge, &small], 0), Err(err));
    }
    for op in [O::L2Norm, O::AsText] {
        assert!(matches!(
            eval_refs(op, &[&huge], 8192),
            Err(EvalError::ExpressionRuntimeFailure(_))
        ));
    }
    let text = Datum::new_string(vec![b'x'; 1 << 20]);
    assert!(matches!(
        eval_refs(O::FromText, &[&text], 8192),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    let decimal = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
        false,
        vec![b'3'],
        1000000,
        0,
    ));
    let original_error = original("VEC_DIMS", &[decimal.clone()], &crate::NoColumns).unwrap_err();
    assert_eq!(eval_refs(O::Dims, &[&decimal], 0), Err(original_error));
    assert!(matches!(
        eval_source(O::FromText, &[&decimal], 8192),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    for op in NAMES.into_iter().map(op) {
        let values = vec![&Datum::Null; op.arity()];
        assert!(matches!(
            eval_refs(op, &values, usize::MAX),
            Err(EvalError::ExpressionRuntimeFailure(_))
        ));
    }
}
