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

use crate::api::graph::{EdgeDirection, DEFAULT_LABEL, DEFAULT_TS};

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Edge {
    src_id: Vec<u8>,
    target_id: Vec<u8>,
    ts: u64,
    label: Option<String>,
    direction: EdgeDirection,
    property: Vec<u8>,
}

impl Edge {
    /// Create edge with src_id, target_id, direction and property,
    /// other fields in vertex is set to default.
    pub fn create_id_edge(
        src_id: Vec<u8>,
        target_id: Vec<u8>,
        direction: EdgeDirection,
        property: Vec<u8>,
    ) -> Self {
        Edge {
            src_id,
            target_id,
            ts: DEFAULT_TS,
            label: None,
            direction,
            property,
        }
    }

    /// Create edge with src_id, target_id, label, direction and property,
    /// ts was set to default.
    pub fn create_id_label_edge(
        src_id: Vec<u8>,
        target_id: Vec<u8>,
        label: String,
        direction: EdgeDirection,
        property: Vec<u8>,
    ) -> Self {
        Edge {
            src_id,
            target_id,
            ts: DEFAULT_TS,
            label: Some(label),
            direction,
            property,
        }
    }

    /// Create edge with src_id, target_id, ts, direction and property,
    /// label was set to default.
    pub fn create_id_time_edge(
        src_id: Vec<u8>,
        target_id: Vec<u8>,
        ts: u64,
        direction: EdgeDirection,
        property: Vec<u8>,
    ) -> Self {
        Edge {
            src_id,
            target_id,
            ts,
            label: None,
            direction,
            property,
        }
    }

    /// Create edge with src_id, target_id, ts, label, direction and property.
    pub fn create_id_time_label_edge(
        src_id: Vec<u8>,
        target_id: Vec<u8>,
        ts: u64,
        label: String,
        direction: EdgeDirection,
        property: Vec<u8>,
    ) -> Self {
        Edge {
            src_id,
            target_id,
            ts,
            label: Some(label),
            direction,
            property,
        }
    }

    pub fn src_id(&self) -> &[u8] {
        self.src_id.as_slice()
    }

    pub fn target_id(&self) -> &[u8] {
        self.target_id.as_slice()
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

    pub fn direction(&self) -> &EdgeDirection {
        &self.direction
    }

    pub fn property(&self) -> &[u8] {
        self.property.as_slice()
    }
}
