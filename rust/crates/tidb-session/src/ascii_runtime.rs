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

use tidb_executor::{AsciiExecution, AsciiOwnerError, AsciiPoolOwner, AsciiPoolPolicy};

#[derive(Default)]
pub(super) struct SessionAsciiRuntime {
    pool: Option<AsciiPoolOwner>,
    lexical_active: Arc<AtomicBool>,
    /// Retained past the lexical call for session teardown. Ordinary close
    /// owners must never look up this slot instead of their captured execution.
    latest_execution: Option<AsciiExecution>,
}

impl SessionAsciiRuntime {
    pub(super) fn try_install(&mut self, policy: AsciiPoolPolicy) -> Result<bool, AsciiOwnerError> {
        if self.pool.is_some() || self.lexical_active.load(Ordering::Acquire) {
            return Ok(false);
        }
        // Construct here rather than accepting an owner that another Session
        // could share. Never replace this root to erase outstanding worker debt.
        self.pool = Some(AsciiPoolOwner::new(policy)?);
        Ok(true)
    }

    pub(super) fn enter(&mut self) -> Result<AsciiStatementEntry, AsciiOwnerError> {
        if self.lexical_active.swap(true, Ordering::AcqRel) {
            return Ok(AsciiStatementEntry {
                closer: None,
                _lexical: None,
            });
        }
        // Armed before begin_execution and therefore before error conversion.
        // Even an unconfigured session is busy while its outer call runs.
        let lexical = LexicalReset(Arc::clone(&self.lexical_active));
        let closer = match &self.pool {
            Some(pool) => {
                let execution = pool.begin_execution()?;
                self.latest_execution = Some(execution.clone());
                Some(AsciiStatementCloser { execution })
            }
            None => None,
        };
        Ok(AsciiStatementEntry {
            closer,
            _lexical: Some(lexical),
        })
    }

    pub(super) fn execution(&self) -> Option<&AsciiExecution> {
        if self.lexical_active.load(Ordering::Acquire) {
            self.latest_execution.as_ref()
        } else {
            None
        }
    }

    /// Session teardown may cancel its latest captured execution. Earlier ones
    /// are already invalidated by begin_execution; their worker debt stays live.
    pub(super) fn shutdown(&self) {
        if let Some(execution) = &self.latest_execution {
            execution.close();
        }
    }

    #[cfg(test)]
    pub(super) fn latest_execution_for_test(&self) -> Option<&AsciiExecution> {
        self.latest_execution.as_ref()
    }
}

struct LexicalReset(Arc<AtomicBool>);

impl Drop for LexicalReset {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) struct AsciiStatementEntry {
    // Field drop order closes the captured execution before resetting the marker.
    closer: Option<AsciiStatementCloser>,
    _lexical: Option<LexicalReset>,
}

impl AsciiStatementEntry {
    pub(super) fn take_closer(&mut self) -> Option<AsciiStatementCloser> {
        self.closer.take()
    }
}

/// Non-Clone outer close authority. Contexts receive only execution clones.
pub(super) struct AsciiStatementCloser {
    execution: AsciiExecution,
}

impl AsciiStatementCloser {
    pub(super) fn unwind_guard(&self) -> AsciiNextUnwindGuard {
        AsciiNextUnwindGuard(self.execution.clone())
    }
}

impl Drop for AsciiStatementCloser {
    fn drop(&mut self) {
        self.execution.close();
    }
}

/// Derived only from this record set's owning closer, never from a borrowed
/// context or Session.latest. Ordinary Next success, EOF and errors keep it live.
pub(super) struct AsciiNextUnwindGuard(AsciiExecution);

impl Drop for AsciiNextUnwindGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.close();
        }
    }
}
