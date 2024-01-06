// Copyright 2023 AntGroup CO., Ltd.
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

use num_enum::{FromPrimitive, IntoPrimitive};

pub mod edge;
pub mod graph_comparator;
pub mod graph_info_util;
pub mod graph_serde;
pub mod serialized_vertex_and_edge;
pub mod vertex;
pub mod vertex_and_edge;

pub const DEFAULT_LABEL: &str = "default";
pub const DEFAULT_TS: u64 = 0;

#[derive(
    Copy, Clone, PartialEq, Debug, PartialOrd, Eq, Ord, Hash, IntoPrimitive, FromPrimitive,
)]
#[repr(u8)]
pub enum EdgeDirection {
    In = 0,
    #[num_enum(default)]
    Out = 1,
}
