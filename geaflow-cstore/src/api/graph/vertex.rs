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

use crate::api::graph::{DEFAULT_LABEL, DEFAULT_TS};

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Vertex {
    src_id: Vec<u8>,
    ts: u64,
    label: Option<String>,
    property: Vec<u8>,
}

impl Vertex {
    /// Create vertex with id and property, other fields in vertex is set to
    /// default.
    pub fn create_id_vertex(src_id: Vec<u8>, property: Vec<u8>) -> Self {
        Vertex {
            src_id,
            ts: DEFAULT_TS,
            label: None,
            property,
        }
    }

    /// Create vertex with id, label and property, other fields in vertex is set
    /// to default.
    pub fn create_id_label_vertex(src_id: Vec<u8>, label: String, property: Vec<u8>) -> Self {
        Vertex {
            src_id,
            ts: DEFAULT_TS,
            label: Some(label),
            property,
        }
    }

    /// Create vertex with id, time and property, other fields in vertex is set
    /// to default.
    pub fn create_id_time_vertex(src_id: Vec<u8>, ts: u64, property: Vec<u8>) -> Self {
        Vertex {
            src_id,
            ts,
            label: None,
            property,
        }
    }

    /// Create vertex with id, time and label.
    pub fn create_id_time_label_vertex(
        src_id: Vec<u8>,
        ts: u64,
        label: String,
        property: Vec<u8>,
    ) -> Self {
        Vertex {
            src_id,
            ts,
            label: Some(label),
            property,
        }
    }

    pub fn src_id(&self) -> &[u8] {
        self.src_id.as_slice()
    }

    pub fn ts(&self) -> u64 {
        self.ts
    }

    pub fn label(&self) -> &str {
        match self.label.as_ref() {
            Some(str) => str.as_str(),
            None => DEFAULT_LABEL,
        }
    }

    pub fn property(&self) -> &[u8] {
        self.property.as_slice()
    }
}
