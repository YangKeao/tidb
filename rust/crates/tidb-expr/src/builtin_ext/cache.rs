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

//! Go `pkg/expression/builtinFuncCache[T]` shared context-cache facade.
//!
//! The cache has one item for one statement context. A failed constructor is
//! deliberately not retained, and a new context replaces the old item. The
//! native owner serializes construction before child effects; an owned
//! reservation can span same-frame demand without a borrowed lock guard.
//! Failed construction preserves the previous item. Owner Clone starts empty;
//! explicit invocation handles alone share live state.

pub(crate) use tidb_query_expr::NativeContextCache as BuiltinFuncCache;
