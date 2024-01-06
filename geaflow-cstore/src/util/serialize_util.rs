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
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use strum_macros::{Display, EnumString};

use crate::{
    api::{
        filter,
        filter::{filter_graph_meta_by_ttl, FilterHandler},
    },
    common::{
        gen::graph_data_generated, GraphDataIndex, SecondKey, FID_SIZE, SECOND_FIXED_LENGTH,
        SECOND_KEY_GRAPH_INFO_SIZE, SECOND_KEY_SEQUENCE_SIZE, SECOND_KEY_TARGET_LEN_SIZE,
        SECOND_KEY_TS_SIZE, SECOND_KEY_VEC_LEN_SIZE, TABLE_OFFSET_SIZE,
    },
    convert_usize_to_u32,
    engine::dict::ttl_dict::TtlDict,
    get_u32_from_bytes, get_u64_from_bytes,
};

#[derive(Clone, Copy, EnumString, Display, PartialEq, Debug)]
#[repr(u32)]
#[strum(ascii_case_insensitive)]
pub enum SerializeType {
    ISerde = 1,

    Serde,

    BinCode,

    FlatBuffer,
}

pub fn serialize_index(
    serialize_type: SerializeType,
    graph_data_index: &GraphDataIndex,
) -> Vec<u8> {
    match serialize_type {
        SerializeType::ISerde => {
            let mut estimated_size: usize = 0;

            estimated_size += FID_SIZE + TABLE_OFFSET_SIZE;
            estimated_size += SECOND_KEY_VEC_LEN_SIZE;

            for second_key in &graph_data_index.second_keys {
                estimated_size +=
                    SECOND_FIXED_LENGTH + SECOND_KEY_TARGET_LEN_SIZE + second_key.target_id.len();
            }

            let mut bf = Vec::with_capacity(estimated_size);

            bf.put_u32(graph_data_index.graph_data_fid);
            bf.put_u64(graph_data_index.graph_data_offset);

            bf.put_u32(convert_usize_to_u32!(graph_data_index.second_keys.len()));

            for item in graph_data_index.second_keys.iter() {
                bf.put_u32(item.ts);

                bf.put_u32(convert_usize_to_u32!(item.target_id.len()));
                bf.put_u64(item.graph_info);
                bf.put_slice(item.target_id.as_slice());

                bf.put_u64(item.sequence_id);
            }

            bf
        }

        SerializeType::Serde => serde_json::to_vec(graph_data_index).unwrap(),

        SerializeType::BinCode => bincode::serialize(graph_data_index).unwrap(),

        SerializeType::FlatBuffer { .. } => unreachable!(),
    }
}

pub fn deserialize_index_with_filter_pushdown(
    serialize_type: SerializeType,
    data: &[u8],
    user_filter: &FilterHandler,
    ttl_dict: &TtlDict,
) -> GraphDataIndex {
    match serialize_type {
        SerializeType::ISerde => {
            let mut cursor = 0;

            let fid = get_u32_from_bytes!(data, cursor);
            cursor += FID_SIZE;

            let offset = get_u64_from_bytes!(data, cursor);
            cursor += TABLE_OFFSET_SIZE;

            let key_size = get_u32_from_bytes!(data, cursor);
            cursor += SECOND_KEY_VEC_LEN_SIZE;

            let mut vec_second_key: Vec<SecondKey> = Vec::with_capacity(key_size as usize);

            for _i in 0..key_size {
                let ts = get_u32_from_bytes!(data, cursor);
                cursor += SECOND_KEY_TS_SIZE;

                let target_id_len = get_u32_from_bytes!(data, cursor) as usize;
                cursor += SECOND_KEY_TARGET_LEN_SIZE;

                // filter index in deserializer to decrease filter and memory-copy overhead.
                let graph_meta =
                    data[cursor..cursor + SECOND_KEY_GRAPH_INFO_SIZE + target_id_len].as_ref();

                let second_key = if !filter::filter_graph_meta(user_filter, graph_meta)
                    || !filter_graph_meta_by_ttl(ttl_dict, graph_meta, ts)
                {
                    cursor += SECOND_KEY_GRAPH_INFO_SIZE;
                    cursor += target_id_len;
                    cursor += SECOND_KEY_SEQUENCE_SIZE;

                    SecondKey::build_empty_second_key()
                } else {
                    let graph_info = get_u64_from_bytes!(data, cursor);
                    cursor += SECOND_KEY_GRAPH_INFO_SIZE;

                    let target_id = data[cursor..cursor + target_id_len].to_vec();
                    cursor += target_id_len;

                    let sequence_id = get_u64_from_bytes!(data, cursor);
                    cursor += SECOND_KEY_SEQUENCE_SIZE;

                    SecondKey {
                        ts,
                        graph_info,
                        sequence_id,
                        target_id,
                    }
                };
                vec_second_key.push(second_key);
            }

            GraphDataIndex {
                graph_data_fid: fid,
                graph_data_offset: offset,
                all_filtered: false,
                second_keys: vec_second_key,
            }
        }

        SerializeType::Serde => serde_json::from_slice(data).unwrap(),

        SerializeType::BinCode => bincode::deserialize(data).unwrap(),

        SerializeType::FlatBuffer { .. } => unreachable!(),
    }
}

pub fn with_second_key_vec_capacity_for_flatbuffer<'a>(
    capacity: usize,
) -> Vec<WIPOffset<graph_data_generated::graph_data::SecondKey<'a>>> {
    Vec::with_capacity(capacity)
}

pub fn add_second_key_for_flatbuffer<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    second_key_vec: &mut Vec<WIPOffset<graph_data_generated::graph_data::SecondKey<'a>>>,
    ts: u32,
    graph_info: u64,
    sequence_id: u64,
    target_id: &[u8],
) {
    let target_id_buf = builder.create_vector(target_id);

    let second_key = graph_data_generated::graph_data::SecondKey::create(
        builder,
        &graph_data_generated::graph_data::SecondKeyArgs {
            ts,
            graph_info,
            sequence_id,
            target_id: Some(target_id_buf),
        },
    );

    second_key_vec.push(second_key);
}

pub fn serialize_index_for_flatbuffer<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    second_key_vec: &[WIPOffset<graph_data_generated::graph_data::SecondKey<'a>>],
    fid: u32,
    offset: u64,
) -> Vec<u8> {
    let second_key_vec_data = builder.create_vector(second_key_vec);

    let graph_data_index: WIPOffset<graph_data_generated::graph_data::GraphDataIndex<'_>> =
        graph_data_generated::graph_data::GraphDataIndex::create(
            builder,
            &graph_data_generated::graph_data::GraphDataIndexArgs {
                fid,
                offset,
                second_key_vec: Some(second_key_vec_data),
            },
        );
    builder.finish(graph_data_index, None);
    Vec::from(builder.finished_data())
}

pub fn deserialize_index_for_flatbuffer(
    data: &[u8],
) -> graph_data_generated::graph_data::GraphDataIndex {
    flatbuffers::root::<graph_data_generated::graph_data::GraphDataIndex>(data).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::filter::create_empty_filter;

    #[test]
    fn test_serialize() {
        let graph_data_index = GraphDataIndex {
            graph_data_fid: 1,
            graph_data_offset: 2,
            all_filtered: false,
            second_keys: vec![
                SecondKey {
                    ts: 0,
                    graph_info: 1,
                    sequence_id: 2,
                    target_id: [1u8, 2u8, 3u8].to_vec(),
                },
                SecondKey {
                    ts: 1,
                    graph_info: 2,
                    sequence_id: 3,
                    target_id: [2u8, 3u8, 4u8].to_vec(),
                },
                SecondKey {
                    ts: 2,
                    graph_info: 22,
                    sequence_id: 55,
                    target_id: [34u8, b'a', 124u8].to_vec(),
                },
                SecondKey {
                    ts: 0,
                    graph_info: 232,
                    sequence_id: 11123,
                    target_id: [33u8, 55u8, 77u8].to_vec(),
                },
            ],
        };

        // ISerde
        let val = serialize_index(SerializeType::ISerde, &graph_data_index);
        let gdi = deserialize_index_with_filter_pushdown(
            SerializeType::ISerde,
            &val,
            &create_empty_filter(),
            &TtlDict::default(),
        );
        assert_eq!(gdi, graph_data_index);

        // Serde
        let val = serialize_index(SerializeType::Serde, &graph_data_index);
        let gdi = deserialize_index_with_filter_pushdown(
            SerializeType::Serde,
            &val,
            &create_empty_filter(),
            &TtlDict::default(),
        );
        assert_eq!(gdi, graph_data_index);

        // Bincode
        let val = serialize_index(SerializeType::BinCode, &graph_data_index);
        let gdi = deserialize_index_with_filter_pushdown(
            SerializeType::BinCode,
            &val,
            &create_empty_filter(),
            &TtlDict::default(),
        );
        assert_eq!(gdi, graph_data_index);

        // Flatbuffer
        let mut builder = FlatBufferBuilder::new();
        let mut vec = with_second_key_vec_capacity_for_flatbuffer(4);

        add_second_key_for_flatbuffer(&mut builder, &mut vec, 0, 1, 2, [1u8, 2u8, 3u8].as_ref());
        add_second_key_for_flatbuffer(
            &mut builder,
            &mut vec,
            0,
            232,
            11123,
            [33u8, 55u8, 77u8].as_ref(),
        );

        let val = serialize_index_for_flatbuffer(&mut builder, &vec, 1, 2);

        let gdi = deserialize_index_for_flatbuffer(&val);
        let key_vec = gdi.second_key_vec().unwrap();

        assert_eq!(
            (
                gdi.fid(),
                gdi.offset(),
                key_vec.get(0).ts(),
                key_vec.get(0).graph_info(),
                key_vec.get(0).sequence_id(),
                key_vec.get(0).target_id().unwrap().iter().collect(),
            ),
            (1u32, 2u64, 0u32, 1u64, 2u64, [1u8, 2u8, 3u8].to_vec())
        );

        assert_eq!(
            (
                key_vec.get(1).ts(),
                key_vec.get(1).graph_info(),
                key_vec.get(1).sequence_id(),
                key_vec.get(1).target_id().unwrap().iter().collect(),
            ),
            (0u32, 232u64, 11123u64, [33u8, 55u8, 77u8].to_vec())
        );
    }
}
