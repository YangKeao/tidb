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

//! Dormant session ownership, distinct from native statement reset/finish.
//!
//! The marker describes a lexical call, not an outstanding detached result.
//! Only its outer entry owns the marker reset and the captured execution closer;
//! transferring the latter to a result must never transfer the former.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tidb_executor::{
    ReadyValueExecution, ReadyValueOwnerError, ReadyValuePoolOwner, ReadyValuePoolPolicy,
};

#[derive(Default)]
pub(super) struct SessionReadyValueRuntime {
    pool: Option<ReadyValuePoolOwner>,
    lexical_active: Arc<AtomicBool>,
    /// Most recently admitted execution, retained for lexical context propagation
    /// and diagnostics. Ordinary close owners use only their captured execution.
    latest_execution: Option<ReadyValueExecution>,
    /// Every independently live execution admitted by this session. Closed
    /// entries are pruned on the next outer admission; shutdown closes all that
    /// remain, including executions owned by detached record sets.
    executions: Vec<ReadyValueExecution>,
}

impl SessionReadyValueRuntime {
    pub(super) fn try_install(
        &mut self,
        policy: ReadyValuePoolPolicy,
    ) -> Result<bool, ReadyValueOwnerError> {
        if self.pool.is_some() || self.lexical_active.load(Ordering::Acquire) {
            return Ok(false);
        }
        // Construct here rather than accepting an owner that another Session
        // could share. Never replace this root to erase outstanding worker debt.
        self.pool = Some(ReadyValuePoolOwner::new(policy)?);
        Ok(true)
    }

    pub(super) fn enter(&mut self) -> Result<ReadyValueStatementEntry, ReadyValueOwnerError> {
        if self.lexical_active.swap(true, Ordering::AcqRel) {
            return Ok(ReadyValueStatementEntry {
                closer: None,
                _lexical: None,
            });
        }
        // Armed before begin_execution and therefore before error conversion.
        // Even an unconfigured session is busy while its outer call runs.
        let lexical = LexicalReset(Arc::clone(&self.lexical_active));
        self.executions.retain(|execution| !execution.is_closed());
        let closer = match &self.pool {
            Some(pool) => {
                let execution = pool.begin_execution()?;
                self.executions.push(execution.clone());
                self.latest_execution = Some(execution.clone());
                Some(ReadyValueStatementCloser { execution })
            }
            None => None,
        };
        Ok(ReadyValueStatementEntry {
            closer,
            _lexical: Some(lexical),
        })
    }

    pub(super) fn execution(&self) -> Option<&ReadyValueExecution> {
        if self.lexical_active.load(Ordering::Acquire) {
            self.latest_execution.as_ref()
        } else {
            None
        }
    }

    /// Session teardown cancels every execution still owned by a detached result.
    /// Closing is idempotent and never forgives live worker debt.
    pub(super) fn shutdown(&self) {
        for execution in &self.executions {
            execution.close();
        }
    }

    #[cfg(test)]
    pub(super) fn latest_execution_for_test(&self) -> Option<&ReadyValueExecution> {
        self.latest_execution.as_ref()
    }
}

struct LexicalReset(Arc<AtomicBool>);

impl Drop for LexicalReset {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) struct ReadyValueStatementEntry {
    // Field drop order closes the captured execution before resetting the marker.
    closer: Option<ReadyValueStatementCloser>,
    _lexical: Option<LexicalReset>,
}

impl ReadyValueStatementEntry {
    pub(super) fn take_closer(&mut self) -> Option<ReadyValueStatementCloser> {
        self.closer.take()
    }
}

/// Non-Clone outer close authority. Contexts receive only execution clones.
pub(super) struct ReadyValueStatementCloser {
    execution: ReadyValueExecution,
}

impl ReadyValueStatementCloser {
    pub(super) fn unwind_guard(&self) -> ReadyValueNextUnwindGuard {
        ReadyValueNextUnwindGuard(self.execution.clone())
    }
}

impl Drop for ReadyValueStatementCloser {
    fn drop(&mut self) {
        self.execution.close();
    }
}

/// Derived only from this record set's owning closer, never from a borrowed
/// context or Session.latest. Ordinary Next success, EOF and errors keep it live.
pub(super) struct ReadyValueNextUnwindGuard(ReadyValueExecution);

impl Drop for ReadyValueNextUnwindGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.close();
        }
    }
}
