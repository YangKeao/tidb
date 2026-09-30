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

//! Closed ready-Bytes value dispatch plus private signed-LongLong control/PLUS slices.
//! This alone is not complete migrated-family evidence. No rejection/error can replay native
//! evaluation. Only immutable lowering results may be shared; a worker compiles
//! and owns its own non-Sync official TiKV RPN program.

// Control/PLUS prototypes remain private; only the closed ready-Bytes hooks are active.
#![allow(dead_code)]

mod adapter_failure;
mod batch;
mod catalog;
mod context;
// The closed ready-Bytes families share this value boundary and one pool.
mod evaluated_ascii;
pub use adapter_failure::{
    ExpressionAdapterFailure, ExpressionAdapterFailureClass, ExpressionAdapterFailureOrigin,
};
pub(crate) use evaluated_ascii::{evaluate_ascii_in, evaluate_bytes_in, EvaluatedBytesResult};
pub use evaluated_ascii::{
    AsciiExecution, AsciiOwnerError, AsciiPoolOwner, AsciiPoolPolicy, AsciiScope,
    ScopedAsciiColumns,
};
pub(crate) use tidb_query_expr::local::EvaluatedBytesOp;
mod lineage;
mod lower;
mod ordinary;
// Only native opaque types cross the crate boundary, never LocalError itself.
mod runtime_failure;
pub use runtime_failure::{
    ExpressionRuntimeFailure, ExpressionRuntimeFailureClass, ExpressionRuntimeFailurePhase,
};
#[cfg(test)]
mod tests;

// These other crate-private entrypoints remain outside general evaluation.
#[allow(unused_imports)]
pub(crate) use batch::PreparedIntControlSeed;
#[allow(unused_imports)]
pub(crate) use lineage::{
    lower_typed_control_lineage, ControlSourceLimits, LoweredControlLineage, NativeControlBatch,
    PreparedControlLineage,
};
#[allow(unused_imports)]
pub(crate) use lower::{lower_int_control_seed, LoweredSpec};
#[allow(unused_imports)]
pub(crate) use ordinary::{
    lower_pb_int_plus_row, lower_typed_int_plus_row, LoweredIntPlusRow, PreparedIntPlusRow,
};

use tidb_datatype::tikv_compat::value::BridgeError;
use tidb_query_expr::local::LocalError;

/// Do not erase TiKV's typed SQL/contract/resource errors or pretend that an
/// admission failure is a SQL NULL. General native diagnostic plumbing is not
/// part of this effect-free seed.
#[derive(Debug)]
pub(super) enum SeedError {
    Admission(&'static str),
    Bridge(BridgeError),
    Local(LocalError),
}

impl From<BridgeError> for SeedError {
    fn from(error: BridgeError) -> Self {
        Self::Bridge(error)
    }
}

impl From<LocalError> for SeedError {
    fn from(error: LocalError) -> Self {
        Self::Local(error)
    }
}

type SeedResult<T> = Result<T, SeedError>;
