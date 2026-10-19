// Copyright 2023 Greptime Team
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

pub mod handler;
pub mod mailbox;
pub mod utils;

/// Request/response extension with value `b"1"` negotiating per-stream caching
/// of the node version, git commit, and start time.
/// Once negotiated, all three fields at their protobuf defaults reuse the cache.
pub const COMPACT_NODE_INFO_KEY: &str = "__compact_node_info";
