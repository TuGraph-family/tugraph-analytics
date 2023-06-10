/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.state.pushdown;

import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.EmptyGraphFilter;
import com.antgroup.geaflow.state.pushdown.limit.IEdgeLimit;
import com.antgroup.geaflow.state.pushdown.project.IProjector;
import java.util.Map;

public class StatePushDown<K, T, R> implements IStatePushDown {

    private IProjector<T, R> projector;
    private IFilter filter = EmptyGraphFilter.of();
    private Map<K, IFilter> filters;
    private IEdgeLimit edgeLimit;
    private EdgeAtom orderField;
    private PushDownType pushdownType = PushDownType.NORMAL;

    protected StatePushDown() {}

    public static StatePushDown of() {
        return new StatePushDown();
    }

    public StatePushDown withFilters(Map<K, IFilter> filters) {
        this.filters = filters;
        return this;
    }

    public StatePushDown withFilter(IFilter filter) {
        this.filter = filter;
        return this;
    }

    public StatePushDown withEdgeLimit(IEdgeLimit edgeLimit) {
        this.edgeLimit = edgeLimit;
        return this;
    }

    public StatePushDown withOrderField(EdgeAtom orderField) {
        this.orderField = orderField;
        return this;
    }

    public StatePushDown<K, T, R> withProjector(IProjector<T, R> projector) {
        this.projector = projector;
        this.pushdownType = PushDownType.PROJECT;
        return this;
    }

    @Override
    public IProjector<T, R> getProjector() {
        return projector;
    }

    @Override
    public IFilter getFilter() {
        return filter;
    }

    @Override
    public Map<K, IFilter> getFilters() {
        return filters;
    }

    @Override
    public IEdgeLimit getEdgeLimit() {
        return edgeLimit;
    }

    @Override
    public EdgeAtom getOrderField() {
        return orderField;
    }

    @Override
    public PushDownType getType() {
        return pushdownType;
    }
}
