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

use bytes::BufMut;

use crate::{
    api::graph::{edge::Edge, vertex::Vertex, EdgeDirection},
    get_u32_from_bytes, get_u64_from_bytes, get_u8_from_bytes, SIZE_OF_U32, SIZE_OF_U64,
    SIZE_OF_U8,
};

pub fn serialize_vertex(vertex: &Vertex) -> Vec<u8> {
    let mut byte_buf: Vec<u8> = vec![];

    // src_id,
    let src_id_ref = vertex.src_id();
    let src_id_len = src_id_ref.len() as u32;
    byte_buf.put_u32(src_id_len);
    byte_buf.put_slice(src_id_ref);

    // ts.
    byte_buf.put_u64(vertex.ts());

    // label.
    let label_ref = vertex.label().as_bytes();
    let label_len = label_ref.len() as u32;
    byte_buf.put_u32(label_len);
    byte_buf.put_slice(label_ref);

    // property.
    let property_ref = vertex.property();
    let property_len = property_ref.len() as u32;
    byte_buf.put_u32(property_len);
    byte_buf.put_slice(property_ref);

    byte_buf
}

pub fn deserialize_vertex(vertex_bytes: Vec<u8>) -> Vertex {
    let mut cursor = 0;
    let src_id_len = get_u32_from_bytes!(vertex_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let src_id = vertex_bytes[cursor..cursor + src_id_len].to_vec();
    cursor += src_id_len;

    let ts = get_u64_from_bytes!(vertex_bytes, cursor);
    cursor += SIZE_OF_U64;

    let label_len = get_u32_from_bytes!(vertex_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let label = vertex_bytes[cursor..cursor + label_len].to_vec();
    cursor += label_len;

    let property_len = get_u32_from_bytes!(vertex_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let property = vertex_bytes[cursor..cursor + property_len].to_vec();

    Vertex::create_id_time_label_vertex(src_id, ts, String::from_utf8(label).unwrap(), property)
}

pub fn serialize_edge(edge: &Edge) -> Vec<u8> {
    let mut byte_buf: Vec<u8> = vec![];

    // src_id.
    let src_id_ref = edge.src_id();
    let src_id_len = src_id_ref.len() as u32;
    byte_buf.put_u32(src_id_len);
    byte_buf.put_slice(src_id_ref);

    // target_id.
    let target_id_ref = edge.target_id();
    let target_id_len = target_id_ref.len() as u32;
    byte_buf.put_u32(target_id_len);
    byte_buf.put_slice(target_id_ref);

    // ts.
    byte_buf.put_u64(edge.ts());

    // label.
    let label_ref = edge.label().as_bytes();
    let label_len = label_ref.len() as u32;
    byte_buf.put_u32(label_len);
    byte_buf.put_slice(label_ref);

    // direction.
    byte_buf.put_u8((*edge.direction()).into());

    // property.
    let property_ref = edge.property();
    let property_len = property_ref.len() as u32;
    byte_buf.put_u32(property_len);
    byte_buf.put_slice(property_ref);

    byte_buf
}

pub fn deserialize_edge(edge_bytes: Vec<u8>) -> Edge {
    let mut cursor = 0;
    let src_id_len = get_u32_from_bytes!(edge_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let src_id = edge_bytes[cursor..cursor + src_id_len].to_vec();
    cursor += src_id_len;

    let target_id_len = get_u32_from_bytes!(edge_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let target_id = edge_bytes[cursor..cursor + target_id_len].to_vec();
    cursor += target_id_len;

    let ts = get_u64_from_bytes!(edge_bytes, cursor);
    cursor += SIZE_OF_U64;

    let label_len = get_u32_from_bytes!(edge_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let label = edge_bytes[cursor..cursor + label_len].to_vec();
    cursor += label_len;

    let direction = EdgeDirection::from(get_u8_from_bytes!(edge_bytes, cursor));
    cursor += SIZE_OF_U8;

    let property_len = get_u32_from_bytes!(edge_bytes, cursor) as usize;
    cursor += SIZE_OF_U32;
    let property = edge_bytes[cursor..cursor + property_len].to_vec();

    Edge::create_id_time_label_edge(
        src_id,
        target_id,
        ts,
        String::from_utf8(label).unwrap(),
        direction,
        property,
    )
}

#[cfg(test)]
mod test {
    use bytes::BufMut;

    use crate::{
        api::{
            graph::{edge::Edge, vertex::Vertex, EdgeDirection},
            iterator::serializer::{
                deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex,
            },
        },
        test_util::{edge_is_equal, vertex_is_equal},
        SIZE_OF_U32,
    };

    #[test]
    pub fn test() {
        let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        src_id_bytes.put_u32(10);

        let mut property_bytes: Vec<u8> = vec![];
        property_bytes.put_u32(100);

        let vertex = Vertex::create_id_time_label_vertex(
            src_id_bytes,
            10,
            String::from("test"),
            property_bytes,
        );

        let serialized_vertex = serialize_vertex(&vertex);
        let deserialized_vertex = deserialize_vertex(serialized_vertex);

        vertex_is_equal(vertex, deserialized_vertex);

        let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        src_id_bytes.put_u32(10);

        let mut target_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        target_id_bytes.put_u32(11);

        let mut property_bytes: Vec<u8> = vec![];
        property_bytes.put_u32(1000);

        let edge = Edge::create_id_time_label_edge(
            src_id_bytes,
            target_id_bytes,
            10,
            String::from("test1"),
            EdgeDirection::In,
            property_bytes,
        );

        let serialized_edge = serialize_edge(&edge);
        let deserialized_edge = deserialize_edge(serialized_edge);
        edge_is_equal(&edge, &deserialized_edge);
    }
}
