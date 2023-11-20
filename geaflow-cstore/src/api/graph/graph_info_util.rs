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

use crate::{get_u16_from_bytes, get_u64_from_bytes};

// low 42 bit
pub const TIME_MASK: u64 = (1 << 42) - 1;
pub const VERTEX_MASK: u64 = 1 << 63;
pub const IN_EDGE_MASK: u64 = 1 << 62;
pub const HAS_VALUE_MASK: u64 = 1 << 61;
pub const LABEL_MASK: u64 = (1 << 10) - 1;
pub const RIGHT_LABEL_POS: u32 = 48;
pub const LABEL_TIME_MASK: u64 = (1 << 58) - 1;

#[inline]
pub fn is_vertex_graph_meta(graph_meta: &[u8]) -> bool {
    (graph_meta[0] & 0x80) == 128
}

#[inline]
pub fn is_in_edge_graph_meta(graph_meta: &[u8]) -> bool {
    (graph_meta[0] & 0x40) == 64
}

#[inline]
pub fn get_event_time_from_graph_meta(graph_meta: &[u8]) -> u64 {
    get_u64_from_bytes!(graph_meta, 0) & TIME_MASK
}

#[inline]
pub fn get_label_from_graph_meta(graph_meta: &[u8]) -> u16 {
    // label should be less than u16, it has been checked when written to engine.
    get_u16_from_bytes!(graph_meta, 0) & (LABEL_MASK as u16)
}

#[inline]
pub fn is_vertex(graph_info: u64) -> bool {
    (graph_info >> 63) == 1
}

#[inline]
pub fn is_in_edge(graph_info: u64) -> bool {
    (graph_info >> 62) == 1
}

#[inline]
pub fn get_edge_direction(graph_info: u64) -> u64 {
    graph_info & IN_EDGE_MASK
}

#[inline]
pub fn get_event_time(graph_info: u64) -> u64 {
    graph_info & TIME_MASK
}

#[inline]
pub fn get_label(graph_info: u64) -> u16 {
    ((graph_info >> RIGHT_LABEL_POS) & LABEL_MASK) as u16
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}
