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

#![allow(unused_imports, dead_code, clippy::all)]
#[allow(non_snake_case)]
pub mod graph_data_generated;

#[allow(non_snake_case)]
pub mod meta {
    include!(concat!(env!("OUT_DIR"), "/meta.rs"));
}

#[allow(non_snake_case)]
pub mod pushdown {
    include!(concat!(env!("OUT_DIR"), "/pushdown.rs"));
}

#[allow(non_snake_case)]
pub mod manifest {
    include!(concat!(env!("OUT_DIR"), "/manifest.rs"));
}

#[allow(non_snake_case)]
pub mod memory_index_manifest {
    include!(concat!(env!("OUT_DIR"), "/memory_index_manifest.rs"));
}
