//! Compile-only experiment probe; no product traits or unsafe implementations.
//! This checks only the exact linked artifact snapshot, not a future C4 runtime.
#![allow(dead_code)]

use std::sync::Mutex;
use tidb_query_datatype::expr::EvalContext;
use tidb_query_expr::local::{LocalEvalState, LocalProgram};

struct CandidateParts {
    program: LocalProgram,
    state: LocalEvalState,
    context: EvalContext,
}

struct IdleOwner(Mutex<Vec<CandidateParts>>);

fn require_send<T: Send>() {}
fn require_send_sync<T: Send + Sync>() {}

fn check_artifact_traits() {
    require_send::<LocalProgram>();
    require_send::<LocalEvalState>();
    require_send::<EvalContext>();
    require_send::<CandidateParts>();
    require_send_sync::<IdleOwner>();
}
