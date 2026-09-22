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

//! Engine-only local execution. New sessions use the copying TiKV adapter.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub use tidb_expr::tikv::Backend;

use crate::Session;

pub(crate) struct State {
    backend: Option<Backend>,
    rows: Arc<AtomicU64>,
    borrowed_rows: Arc<AtomicU64>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            backend: Some(Backend::Copying),
            rows: Arc::new(AtomicU64::new(0)),
            borrowed_rows: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Session {
    /// Selects a local TiKV expression adapter for this engine-only session.
    /// Unsupported expressions return a structured error and are never replayed.
    #[must_use]
    pub fn with_tikv_expression_backend(mut self, backend: Backend) -> Self {
        self.set_tikv_expression_backend(Some(backend));
        self
    }

    /// Selects an adapter. `None` deliberately removes the engine context so
    /// evaluation proves that no native fallback remains. Existing counters are retained.
    pub fn set_tikv_expression_backend(&mut self, backend: Option<Backend>) {
        self.tikv_expression.backend = backend;
    }

    /// The selected adapter; a newly created session returns `Some(Copying)`.
    #[must_use]
    pub fn tikv_expression_backend(&self) -> Option<Backend> {
        self.tikv_expression.backend
    }

    /// Cumulative successful local-engine expression-row evaluations.
    /// This is not a count of SQL rows: several expressions may visit each row.
    #[must_use]
    pub fn tikv_expression_rows(&self) -> u64 {
        self.tikv_expression.rows.load(Ordering::Relaxed)
    }

    /// The subset evaluated through borrowed kernels, excluding copying fallback.
    #[must_use]
    pub fn tikv_borrowed_expression_rows(&self) -> u64 {
        self.tikv_expression.borrowed_rows.load(Ordering::Relaxed)
    }

    pub(crate) fn configure_tikv_expression(
        &self,
        context: tidb_executor::StmtContext,
    ) -> tidb_executor::StmtContext {
        match self.tikv_expression.backend {
            Some(backend) => context
                .with_tikv_expression_backend(backend)
                .with_tikv_expression_counters(
                    Arc::clone(&self.tikv_expression.rows),
                    Arc::clone(&self.tikv_expression.borrowed_rows),
                ),
            None => context,
        }
    }
}
