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

use crate::{
    api::{
        filter::{
            empty_filter::StoreEmptyFilter, store_and_filter::StoreAndFilter,
            store_composed_edge_limit_filter::StoreComposedEdgeLimitFilter,
            store_edge_label_filter::StoreEdgeLabelFilter, store_edge_ts_filter::StoreEdgeTsFilter,
            store_edge_value_drop_filter::StoreEdgeValueDropFilter,
            store_in_edge_filter::StoreInEdgeFilter,
            store_multi_edge_ts_filter::StoreMultiEdgeTsFilter,
            store_only_edge_filter::StoreOnlyEdgeFilter,
            store_only_vertex_filter::StoreOnlyVertexFilter, store_or_filter::StoreOrFilter,
            store_out_edge_filter::StoreOutEdgeFilter,
            store_single_edge_limit_filter::StoreSingleEdgeLimitFilter,
            store_ttl_filter::StoreTtlFilter, store_vertex_label_filter::StoreVertexLabelFilter,
            store_vertex_ts_filter::StoreVertexTsFilter,
            store_vertex_value_drop_filter::StoreVertexValueDropFilter,
        },
        graph::graph_info_util::{get_label, get_label_from_graph_meta},
    },
    engine::dict::ttl_dict::TtlDict,
    log_util::debug,
    time_util::time_now_as_secs,
    GraphData,
};

pub mod converter;
pub mod empty_filter;
pub mod store_and_filter;
pub mod store_composed_edge_limit_filter;
pub mod store_edge_label_filter;
pub mod store_edge_ts_filter;
pub mod store_edge_value_drop_filter;
pub mod store_in_edge_filter;
pub mod store_multi_edge_ts_filter;
pub mod store_only_edge_filter;
pub mod store_only_vertex_filter;
pub mod store_or_filter;
pub mod store_other_filter;
pub mod store_out_edge_filter;
pub mod store_single_edge_limit_filter;
pub mod store_ttl_filter;
pub mod store_vertex_label_filter;
pub mod store_vertex_ts_filter;
pub mod store_vertex_value_drop_filter;

pub struct StoreFilterPushdown {
    pub filter_handler: FilterHandler,
    pub filter_context: FilterContext,
    pub edge_limit: EdgeLimit,
}

impl StoreFilterPushdown {
    pub fn get_edge_limit(&self) -> &EdgeLimit {
        &self.edge_limit
    }

    pub fn get_order_field(&self) {
        todo!()
    }

    pub fn new(
        filter_handler: FilterHandler,
        filter_context: FilterContext,
        edge_limit: EdgeLimit,
    ) -> Self {
        Self {
            filter_handler,
            filter_context,
            edge_limit,
        }
    }
}

pub fn create_empty_filter_pushdown() -> StoreFilterPushdown {
    StoreFilterPushdown::new(
        create_empty_filter(),
        FilterContext::default(),
        EdgeLimit::default(),
    )
}

#[derive(Default)]
pub struct FilterContext {
    pub drop_vertex_value_property: bool,
    pub drop_edge_value_property: bool,
    pub vertex_must_contains: bool,
    pub has_edge_limit: bool,
}

impl FilterContext {
    pub fn and(&mut self, filter_context: &FilterContext) {
        self.drop_vertex_value_property =
            self.drop_edge_value_property || filter_context.drop_vertex_value_property;
        self.drop_edge_value_property =
            self.drop_edge_value_property || filter_context.drop_edge_value_property;
        self.vertex_must_contains =
            self.vertex_must_contains || filter_context.vertex_must_contains;
    }

    pub fn or(&mut self, filter_context: &FilterContext) {
        self.drop_vertex_value_property =
            self.drop_edge_value_property && filter_context.drop_vertex_value_property;
        self.drop_edge_value_property =
            self.drop_edge_value_property && filter_context.drop_edge_value_property;
        self.vertex_must_contains =
            self.vertex_must_contains && filter_context.vertex_must_contains;
    }
}

pub trait IFilter {
    // return true means keep the graph_info.
    fn filter_graph_info(&self, _graph_info: u64) -> bool {
        true
    }

    // return true means keep the graph_meta.
    fn filter_graph_meta(&self, _graph_meta: &[u8]) -> bool {
        true
    }

    fn and(&self, filter: FilterHandler) -> FilterHandler {
        FilterHandler::And(StoreAndFilter::new(vec![filter]))
    }

    fn or(&self, filter: FilterHandler) -> FilterHandler {
        FilterHandler::Or(StoreOrFilter::new(vec![filter]))
    }
}

pub trait IEdgeLimit {
    fn select_graph_info(&self, graph_info: u64) -> bool;

    fn select_graph_meta(&self, graph_meta: &[u8]) -> bool;
    // return true means drop all the remaining data.
    fn drop_all_remaining(&self) -> bool;

    fn reset(&self);
}

pub enum FilterHandler {
    Empty(StoreEmptyFilter),
    OnlyVertex(StoreOnlyVertexFilter),
    InEdge(StoreInEdgeFilter),
    OutEdge(StoreOutEdgeFilter),
    VertexTs(StoreVertexTsFilter),
    EdgeTs(StoreEdgeTsFilter),
    MultiEdgeTs(StoreMultiEdgeTsFilter),
    VertexLabel(StoreVertexLabelFilter),
    EdgeLabel(StoreEdgeLabelFilter),
    VertexValueDrop(StoreVertexValueDropFilter),
    EdgeValueDrop(StoreEdgeValueDropFilter),
    Ttl(StoreTtlFilter),
    And(StoreAndFilter),
    Or(StoreOrFilter),
    VertexMustContains,
    OnlyEdge(StoreOnlyEdgeFilter),
    SingleEdgeLimit(StoreSingleEdgeLimitFilter),
    ComposedEdgeLimit(StoreComposedEdgeLimitFilter),
    Generated,
    Other,
}

pub fn create_empty_filter() -> FilterHandler {
    FilterHandler::Empty(StoreEmptyFilter::default())
}

pub fn print_filter(filter: &FilterHandler) {
    match filter {
        FilterHandler::Empty(_) => {
            debug!("empty.")
        }
        FilterHandler::OnlyVertex(_) => {
            debug!("only vertex.")
        }
        FilterHandler::InEdge(_) => {
            debug!("in edge.")
        }
        FilterHandler::OutEdge(_) => {
            debug!("out edge.")
        }
        FilterHandler::VertexTs(_) => {
            debug!("vertex ts.")
        }
        FilterHandler::EdgeTs(_) => {
            debug!("edge ts.")
        }
        FilterHandler::MultiEdgeTs(_) => {
            debug!("multi edge ts.")
        }
        FilterHandler::VertexLabel(_) => {
            debug!("vertex label.")
        }
        FilterHandler::EdgeLabel(_) => {
            debug!("edge label.")
        }
        FilterHandler::VertexValueDrop(_) => {
            debug!("vertex value drop.")
        }
        FilterHandler::EdgeValueDrop(_) => {
            debug!("edge value drop.")
        }
        FilterHandler::Ttl(_) => {
            debug!("ttl.")
        }
        FilterHandler::And(_) => {
            debug!("and.")
        }
        FilterHandler::Or(_) => {
            debug!("or.")
        }
        FilterHandler::VertexMustContains => {
            debug!("must contains vertex.")
        }
        FilterHandler::OnlyEdge(_) => {
            debug!("only edge.")
        }
        FilterHandler::Generated => {
            debug!("generated.")
        }
        FilterHandler::Other => {
            debug!("other.")
        }
        _ => {}
    }
}

pub fn reset_filter(filter: &FilterHandler) {
    match filter {
        FilterHandler::SingleEdgeLimit(filter) => filter.reset(),
        FilterHandler::ComposedEdgeLimit(filter) => filter.reset(),
        _ => {}
    }
}

pub fn select_graph_info(filter: &FilterHandler, graph_info: u64) -> bool {
    match filter {
        FilterHandler::SingleEdgeLimit(filter) => filter.select_graph_info(graph_info),
        FilterHandler::ComposedEdgeLimit(filter) => filter.select_graph_info(graph_info),
        _ => true,
    }
}

pub fn select_graph_meta(filter: &FilterHandler, graph_meta: &[u8]) -> bool {
    match filter {
        FilterHandler::SingleEdgeLimit(filter) => filter.select_graph_meta(graph_meta),
        FilterHandler::ComposedEdgeLimit(filter) => filter.select_graph_meta(graph_meta),
        _ => true,
    }
}

pub fn filter_graph_info(filter: &FilterHandler, graph_info: u64) -> bool {
    match filter {
        FilterHandler::Empty(empty_filter) => empty_filter.filter_graph_info(graph_info),
        FilterHandler::OnlyVertex(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::InEdge(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::OutEdge(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::VertexTs(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::EdgeTs(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::MultiEdgeTs(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::VertexLabel(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::EdgeLabel(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::OnlyEdge(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::And(filter) => filter.filter_graph_info(graph_info),
        FilterHandler::Or(filter) => filter.filter_graph_info(graph_info),
        _ => true,
    }
}

pub fn filter_graph_meta(filter: &FilterHandler, graph_meta: &[u8]) -> bool {
    match filter {
        FilterHandler::Empty(empty_filter) => empty_filter.filter_graph_meta(graph_meta),
        FilterHandler::OnlyVertex(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::InEdge(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::OutEdge(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::VertexTs(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::EdgeTs(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::MultiEdgeTs(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::VertexLabel(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::EdgeLabel(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::OnlyEdge(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::And(filter) => filter.filter_graph_meta(graph_meta),
        FilterHandler::Or(filter) => filter.filter_graph_meta(graph_meta),
        _ => true,
    }
}

pub fn drop_all_remaining(filter: &FilterHandler) -> bool {
    match filter {
        FilterHandler::SingleEdgeLimit(filter) => filter.drop_all_remaining(),
        FilterHandler::ComposedEdgeLimit(filter) => filter.drop_all_remaining(),
        _ => false,
    }
}

pub fn filter_graph_meta_by_ttl(ttl_dict: &TtlDict, graph_meta: &[u8], write_ts: u32) -> bool {
    let ttl = ttl_dict.get_ttl(get_label_from_graph_meta(graph_meta));
    if ttl < 0 {
        return true;
    }
    filter_by_ttl(write_ts, ttl as u32)
}

pub fn filter_graph_data_by_ttl(ttl_dict: &TtlDict, graph_data: &GraphData) -> bool {
    let ttl = ttl_dict.get_ttl(get_label(graph_data.second_key.graph_info));
    if ttl < 0 {
        return true;
    }
    let write_ts = graph_data.second_key.ts;
    filter_by_ttl(write_ts, ttl as u32)
}

fn filter_by_ttl(write_ts: u32, ttl: u32) -> bool {
    let cur_ts = time_now_as_secs() as u32;

    cur_ts - write_ts <= ttl
}

#[derive(Clone, Copy)]
pub struct EdgeLimit {
    pub out_edge_limit: u64,
    pub in_edge_limit: u64,
    pub is_single: bool,
}

impl Default for EdgeLimit {
    fn default() -> Self {
        Self {
            out_edge_limit: u64::MAX,
            in_edge_limit: u64::MAX,
            is_single: true,
        }
    }
}

impl EdgeLimit {
    pub fn new(out_edge_limit: u64, in_edge_limit: u64, is_single: bool) -> Self {
        Self {
            out_edge_limit,
            in_edge_limit,
            is_single,
        }
    }
}
