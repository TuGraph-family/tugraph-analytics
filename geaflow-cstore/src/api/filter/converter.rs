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

use std::default::Default;

use itertools::Itertools;
use prost::Message;
use rustc_hash::FxHashMap;

use crate::{
    api::filter::{
        empty_filter::StoreEmptyFilter, store_and_filter::StoreAndFilter,
        store_composed_edge_limit_filter::StoreComposedEdgeLimitFilter,
        store_edge_label_filter::StoreEdgeLabelFilter, store_edge_ts_filter::StoreEdgeTsFilter,
        store_in_edge_filter::StoreInEdgeFilter,
        store_multi_edge_ts_filter::StoreMultiEdgeTsFilter,
        store_only_vertex_filter::StoreOnlyVertexFilter, store_or_filter::StoreOrFilter,
        store_out_edge_filter::StoreOutEdgeFilter,
        store_single_edge_limit_filter::StoreSingleEdgeLimitFilter,
        store_ttl_filter::StoreTtlFilter, store_vertex_label_filter::StoreVertexLabelFilter,
        store_vertex_ts_filter::StoreVertexTsFilter, EdgeLimit, FilterContext, FilterHandler,
        StoreFilterPushdown,
    },
    engine::dict::label_dict::LabelDict,
    gen::pushdown::{filter_node::Content, push_down::Filter, FilterNode, FilterType, PushDown},
};

const EMPTY_PUSHDOWN: PushDown = PushDown {
    filter: None,
    edge_limit: None,
    sort_type: vec![],
};

pub fn encode_pushdown(pushdown: &PushDown) -> Vec<u8> {
    pushdown.encode_to_vec()
}

pub fn decode_pushdown(value: &[u8]) -> PushDown {
    if value.is_empty() {
        return EMPTY_PUSHDOWN;
    }
    PushDown::decode(value).unwrap()
}

pub fn convert_to_filter_pushdown(
    push_down: &PushDown,
    label_dict: &LabelDict,
) -> StoreFilterPushdown {
    let edge_limit = convert_edge_limit(push_down);
    if push_down.filter.is_none() {
        return wrap_composed_edge_limit_filter_if_need(
            FilterHandler::Empty(StoreEmptyFilter::default()),
            0,
            &edge_limit,
        );
    }

    let filter = push_down.filter.as_ref().unwrap();
    match filter {
        Filter::FilterNode(filter_node) => {
            convert_to_filter_pushdown_inner(filter_node, label_dict, &edge_limit, 0)
        }
        Filter::FilterNodes(_filter_nodes) => {
            panic!("should be filter_node, please check push down.")
        }
    }
}

pub fn convert_to_filter_pushdown_map(
    src_ids: &[Vec<u8>],
    push_down: &PushDown,
    label_dict: &LabelDict,
) -> FxHashMap<Vec<u8>, StoreFilterPushdown> {
    let edge_limit = convert_edge_limit(push_down);

    if push_down.filter.is_none() {
        return if push_down.edge_limit.is_none() {
            FxHashMap::default()
        } else {
            let mut filter_node_map = FxHashMap::default();
            for src_id in src_ids {
                let filter_pushdown = wrap_composed_edge_limit_filter_if_need(
                    FilterHandler::Empty(StoreEmptyFilter::default()),
                    0,
                    &edge_limit,
                );
                filter_node_map.insert(src_id.clone(), filter_pushdown);
            }
            filter_node_map
        };
    }

    let filter = push_down.filter.as_ref().unwrap();
    match filter {
        Filter::FilterNode(filter_node) => {
            let mut filter_node_map = FxHashMap::default();
            for src_id in src_ids {
                filter_node_map.insert(
                    src_id.clone(),
                    convert_to_filter_pushdown_inner(filter_node, label_dict, &edge_limit, 0),
                );
            }

            filter_node_map
        }
        Filter::FilterNodes(filter_nodes) => {
            let mut filter_node_map = FxHashMap::default();

            assert_eq!(
                filter_nodes.filter_nodes.len(),
                filter_nodes.keys.len(),
                "filter num is not equals to key num."
            );

            for filter_node_pair in filter_nodes.filter_nodes.iter().enumerate() {
                let key = filter_nodes.keys.get(filter_node_pair.0).unwrap().clone();
                filter_node_map.insert(
                    key,
                    convert_to_filter_pushdown_inner(
                        filter_node_pair.1,
                        label_dict,
                        &edge_limit,
                        0,
                    ),
                );
            }

            filter_node_map
        }
    }
}

fn convert_edge_limit(push_down: &PushDown) -> EdgeLimit {
    if push_down.edge_limit.is_none() {
        EdgeLimit::default()
    } else {
        let edge_limit = push_down.edge_limit.as_ref().unwrap();
        EdgeLimit::new(edge_limit.out, edge_limit.r#in, edge_limit.is_single)
    }
}

fn wrap_composed_edge_limit_filter_if_need(
    filter_handler: FilterHandler,
    level: usize,
    edge_limit: &EdgeLimit,
) -> StoreFilterPushdown {
    if level == 0 && (edge_limit.in_edge_limit != u64::MAX || edge_limit.out_edge_limit != u64::MAX)
    {
        let mut filter_context = FilterContext {
            drop_vertex_value_property: false,
            drop_edge_value_property: false,
            vertex_must_contains: false,
            has_edge_limit: true,
        };
        filter_context.has_edge_limit = true;
        StoreFilterPushdown::new(
            FilterHandler::ComposedEdgeLimit(StoreComposedEdgeLimitFilter::new(
                Box::new(filter_handler),
                *edge_limit,
            )),
            filter_context,
            *edge_limit,
        )
    } else {
        StoreFilterPushdown::new(filter_handler, FilterContext::default(), *edge_limit)
    }
}

pub fn convert_to_filter_pushdown_inner(
    filter_node: &FilterNode,
    label_dict: &LabelDict,
    edge_limit: &EdgeLimit,
    level: usize,
) -> StoreFilterPushdown {
    let filter_type_ordinal = FilterType::from_i32(filter_node.filter_type);

    match filter_type_ordinal {
        Some(FilterType::Empty) => wrap_composed_edge_limit_filter_if_need(
            FilterHandler::Empty(StoreEmptyFilter::default()),
            level,
            edge_limit,
        ),
        Some(FilterType::OnlyVertex) => wrap_composed_edge_limit_filter_if_need(
            FilterHandler::OnlyVertex(StoreOnlyVertexFilter::default()),
            level,
            edge_limit,
        ),
        Some(FilterType::InEdge) => wrap_composed_edge_limit_filter_if_need(
            FilterHandler::InEdge(StoreInEdgeFilter::default()),
            level,
            edge_limit,
        ),
        Some(FilterType::OutEdge) => wrap_composed_edge_limit_filter_if_need(
            FilterHandler::OutEdge(StoreOutEdgeFilter::default()),
            level,
            edge_limit,
        ),
        Some(FilterType::VertexTs) => {
            if let Content::LongContent(long_content) = filter_node.content.as_ref().unwrap() {
                assert_eq!(long_content.long.len(), 2);
                let filter_handler = FilterHandler::VertexTs(StoreVertexTsFilter::new((
                    *long_content.long.first().unwrap(),
                    *long_content.long.get(1).unwrap(),
                )));
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit)
            } else {
                panic!("VertexTsFilter with wrong content type.")
            }
        }
        Some(FilterType::EdgeTs) => {
            if let Content::LongContent(long_content) = filter_node.content.as_ref().unwrap() {
                assert_eq!(long_content.long.len(), 2);
                let filter_handler = FilterHandler::EdgeTs(StoreEdgeTsFilter::new((
                    *long_content.long.first().unwrap(),
                    *long_content.long.get(1).unwrap(),
                )));
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit)
            } else {
                panic!("EdgeTsFilter with wrong content type.")
            }
        }
        Some(FilterType::MultiEdgeTs) => {
            if let Content::LongContent(long_content) = filter_node.content.as_ref().unwrap() {
                assert_eq!(long_content.long.len() % 2, 0);

                let mut ts_vec = vec![];
                for (a, b) in long_content.long.iter().tuples() {
                    ts_vec.push((*a, *b))
                }
                let filter_handler =
                    FilterHandler::MultiEdgeTs(StoreMultiEdgeTsFilter::new(ts_vec));
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit)
            } else {
                panic!("MultiEdgeTsFilter with wrong content type.")
            }
        }
        Some(FilterType::EdgeLabel) => {
            if let Content::StrContent(string_content) = filter_node.content.as_ref().unwrap() {
                let label_type_vec = string_content
                    .str
                    .iter()
                    .filter_map(|label| label_dict.get_label_type(label))
                    .collect_vec();
                let filter_handler =
                    FilterHandler::EdgeLabel(StoreEdgeLabelFilter::new(label_type_vec));
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit)
            } else {
                panic!("EdgeLabelFilter with wrong content type.")
            }
        }
        Some(FilterType::VertexLabel) => {
            if let Content::StrContent(string_content) = filter_node.content.as_ref().unwrap() {
                let label_type_vec = string_content
                    .str
                    .iter()
                    .filter_map(|label| label_dict.get_label_type(label))
                    .collect_vec();
                let filter_handler =
                    FilterHandler::VertexLabel(StoreVertexLabelFilter::new(label_type_vec));
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit)
            } else {
                panic!("VertexLabelFilter with wrong content type.")
            }
        }
        Some(FilterType::VertexValueDrop) => {
            let filter_handler = FilterHandler::Empty(StoreEmptyFilter::default());
            let mut filter_pushdown =
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit);
            filter_pushdown.filter_context.drop_vertex_value_property = true;
            filter_pushdown
        }
        Some(FilterType::EdgeValueDrop) => {
            let filter_handler = FilterHandler::Empty(StoreEmptyFilter::default());
            let mut filter_pushdown =
                wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit);
            filter_pushdown.filter_context.drop_edge_value_property = true;
            filter_pushdown
        }
        Some(FilterType::Ttl) => wrap_composed_edge_limit_filter_if_need(
            FilterHandler::Ttl(StoreTtlFilter::default()),
            level,
            edge_limit,
        ),
        Some(FilterType::And) => {
            let mut filter_vec = vec![];
            let mut filter_context = FilterContext::default();
            for filter in filter_node.filters.iter() {
                let filter_push_down =
                    convert_to_filter_pushdown_inner(filter, label_dict, edge_limit, level + 1);
                filter_vec.push(filter_push_down.filter_handler);
                filter_context.and(&filter_push_down.filter_context);
            }

            let filter_handler = FilterHandler::And(StoreAndFilter::new(filter_vec));
            wrap_composed_edge_limit_filter_if_need(filter_handler, level, edge_limit)
        }
        Some(FilterType::Or) => {
            let mut filter_vec = vec![];
            let mut filter_context = FilterContext::default();
            for filter in filter_node.filters.iter() {
                let filter_push_down =
                    convert_to_filter_pushdown_inner(filter, label_dict, edge_limit, level + 1);
                filter_vec.push(filter_push_down.filter_handler);
                filter_context.or(&filter_push_down.filter_context);
            }

            let or_filter = StoreOrFilter::new(filter_vec);

            let filter_handler = if (edge_limit.in_edge_limit != u64::MAX
                || edge_limit.out_edge_limit != u64::MAX)
                && level == 0
            {
                filter_context.has_edge_limit = true;
                if edge_limit.is_single {
                    FilterHandler::SingleEdgeLimit(StoreSingleEdgeLimitFilter::new(
                        or_filter,
                        *edge_limit,
                    ))
                } else {
                    FilterHandler::ComposedEdgeLimit(StoreComposedEdgeLimitFilter::new(
                        Box::new(FilterHandler::Or(or_filter)),
                        *edge_limit,
                    ))
                }
            } else {
                FilterHandler::Or(or_filter)
            };
            StoreFilterPushdown::new(filter_handler, filter_context, *edge_limit)
        }
        Some(FilterType::VertexMustContain) => {
            let filter_handler = FilterHandler::Empty(StoreEmptyFilter::default());
            let filter_context = FilterContext {
                drop_vertex_value_property: false,
                drop_edge_value_property: false,
                vertex_must_contains: true,
                has_edge_limit: false,
            };
            StoreFilterPushdown::new(filter_handler, filter_context, *edge_limit)
        }
        _ => panic!("unknown filter type"),
    }
}
