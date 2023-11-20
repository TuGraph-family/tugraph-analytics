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

use std::cmp::Ordering;

use crate::{
    api::graph::{
        edge::Edge,
        graph_comparator::GraphSortField::{DescTime, Direction, DstId, Label},
        graph_info_util::{get_edge_direction, get_event_time, get_label, is_vertex},
    },
    SecondKey,
};

pub enum GraphSortField {
    Direction,
    DescTime,
    Label,
    DstId,
}

impl GraphSortField {
    pub fn from(input: &str) -> GraphSortField {
        match input {
            "direction" => Direction,
            "disc_time" => DescTime,
            "label" => Label,
            "dst_id" => DstId,
            _ => panic!("unknown sort field {}", input),
        }
    }
}

pub fn parse_graph_sort_field(sort_field: &str) -> Vec<GraphSortField> {
    let sort_field_str_split = sort_field.split(',');
    let mut sort_field = vec![];
    for sort_field_str in sort_field_str_split {
        sort_field.push(GraphSortField::from(sort_field_str));
    }

    sort_field
}

fn get_second_key_comparator(
    graph_data_sort_field: &GraphSortField,
) -> fn(&SecondKey, &SecondKey) -> Ordering {
    match graph_data_sort_field {
        Direction => |second_key_1: &SecondKey, second_key_2: &SecondKey| {
            get_edge_direction(second_key_1.graph_info)
                .cmp(&get_edge_direction(second_key_2.graph_info))
        },
        DescTime => |second_key_1: &SecondKey, second_key_2: &SecondKey| {
            get_event_time(second_key_2.graph_info).cmp(&get_event_time(second_key_1.graph_info))
        },
        Label => |second_key_1: &SecondKey, second_key_2: &SecondKey| {
            get_label(second_key_1.graph_info).cmp(&get_label(second_key_2.graph_info))
        },
        DstId => |second_key_1: &SecondKey, second_key_2: &SecondKey| {
            second_key_1.target_id.cmp(&second_key_2.target_id)
        },
    }
}

pub fn get_edge_comparator(graph_data_sort_field: &GraphSortField) -> fn(&Edge, &Edge) -> Ordering {
    match graph_data_sort_field {
        Direction => |edge_1: &Edge, edge_2: &Edge| edge_1.direction().cmp(edge_2.direction()),
        DescTime => |edge_1: &Edge, edge_2: &Edge| edge_2.ts().cmp(&edge_1.ts()),
        Label => |edge_1: &Edge, edge_2: &Edge| edge_1.label().cmp(edge_2.label()),
        DstId => |edge_1: &Edge, edge_2: &Edge| edge_1.target_id().cmp(edge_2.target_id()),
    }
}

fn cmp_by_sequence_id(second_key_1: &SecondKey, second_key_2: &SecondKey) -> Ordering {
    second_key_2.sequence_id.cmp(&second_key_1.sequence_id)
}

pub fn cmp_second_key(
    second_key_1: &SecondKey,
    second_key_2: &SecondKey,
    store_comparators: &[GraphSortField],
) -> Ordering {
    let is_vertex_1 = is_vertex(second_key_1.graph_info);
    let is_vertex_2 = is_vertex(second_key_2.graph_info);
    if is_vertex_1 && !is_vertex_2 {
        return Ordering::Less;
    } else if !is_vertex_1 && is_vertex_2 {
        return Ordering::Greater;
    }

    for field in store_comparators {
        let ordering = get_second_key_comparator(field)(second_key_1, second_key_2);
        match ordering {
            Ordering::Equal => continue,
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
        }
    }

    cmp_by_sequence_id(second_key_1, second_key_2)
}

pub fn cmp_edge(edge1: &Edge, edge2: &Edge, store_comparators: &[GraphSortField]) -> Ordering {
    for field in store_comparators {
        let ordering = get_edge_comparator(field)(edge1, edge2);
        match ordering {
            Ordering::Equal => continue,
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
        }
    }

    Ordering::Equal
}
