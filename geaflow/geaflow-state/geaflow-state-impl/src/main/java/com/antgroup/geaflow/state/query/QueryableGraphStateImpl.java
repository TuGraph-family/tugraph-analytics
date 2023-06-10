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

package com.antgroup.geaflow.state.query;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.iterator.IteratorWithFn;
import com.antgroup.geaflow.state.iterator.StandardIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.FilterHelper;
import com.antgroup.geaflow.state.pushdown.inner.IFilterConverter;
import com.antgroup.geaflow.state.pushdown.limit.ComposedEdgeLimit;
import com.antgroup.geaflow.state.pushdown.limit.SingleEdgeLimit;
import com.antgroup.geaflow.state.pushdown.project.IProjector;
import com.antgroup.geaflow.state.pushdown.project.ProjectType;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryableGraphStateImpl<K, VV, EV, R> implements QueryableGraphState<K, VV, EV, R> {

    protected final QueryType<R> type;
    protected final IGraphManager<K, VV, EV> graphManager;
    protected IFilterConverter filterConverter;
    protected long version = -1;
    protected QueryCondition<K> queryCondition;

    public QueryableGraphStateImpl(QueryType<R> type, IGraphManager<K, VV, EV> graphManager) {
        this.type = type;
        this.graphManager = graphManager;
        this.filterConverter = this.graphManager.getFilterConverter();
    }

    public QueryableGraphStateImpl(QueryType<R> type, IGraphManager<K, VV, EV> graphManager,
                                   QueryCondition queryCondition) {
        this(type, graphManager);
        this.queryCondition = queryCondition;
    }

    public QueryableGraphStateImpl(Long version, QueryType<R> type,
                                   IGraphManager<K, VV, EV> graphManager) {
        this(type, graphManager);
        this.version = version;
    }

    @Override
    public <U> QueryableGraphState<K, VV, EV, U> select(IProjector<R, U> projector) {
        queryCondition.projector = projector;
        if (projector.projectType() == ProjectType.DST_ID || projector.projectType() == ProjectType.TIME) {
            QueryType<U> queryType = new QueryType<>(DataType.PROJECT_FIELD);
            return new QueryableGraphStateImpl<>(queryType, graphManager, queryCondition);
        } else {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
    }

    @Override
    public QueryableGraphState<K, VV, EV, R> limit(long out, long in) {
        boolean isSingleLimit = FilterHelper.isSingleLimit(queryCondition.stateFilters);
        queryCondition.limit = isSingleLimit ? new SingleEdgeLimit(out, in) : new ComposedEdgeLimit(out, in);
        return this;
    }

    @Override
    public QueryableGraphState<K, VV, EV, R> orderBy(EdgeAtom atom) {
        queryCondition.order = atom;
        return this;
    }

    @Override
    public List<R> asList() {
        return Lists.newArrayList(iterator());
    }

    private Map<K, IFilter> buildMapFilter() {
        Map<K, IFilter> mapFilters = new HashMap<>(queryCondition.stateFilters.length);
        Preconditions.checkArgument(queryCondition.stateFilters.length == queryCondition.queryIds.size());
        for (int i = 0; i < queryCondition.stateFilters.length; i++) {
            mapFilters.put(queryCondition.queryIds.get(i),
                filterConverter.convert(queryCondition.stateFilters[i]));
        }
        return mapFilters;
    }

    protected StatePushDown getPushDown() {
        StatePushDown pushDown = StatePushDown.of()
            .withEdgeLimit(queryCondition.limit)
            .withOrderField(queryCondition.order);

        if (queryCondition.stateFilters.length > 1) {
            pushDown.withFilters(buildMapFilter());
        } else {
            pushDown.withFilter(filterConverter.convert(queryCondition.stateFilters[0]));
        }
        return pushDown;
    }

    private Iterator<R> staticIterator() {
        StatePushDown condition = getPushDown();
        Iterator<R> it;

        switch (this.type.getType()) {
            case V:
                it = queryCondition.isFullScan
                     ? (Iterator<R>) this.graphManager.getStaticGraphTrait().getVertexIterator(condition)
                     : (Iterator<R>) this.graphManager.getStaticGraphTrait().getVertexIterator(queryCondition.queryIds, condition);
                break;
            case E:
                it = queryCondition.isFullScan
                     ? (Iterator<R>) this.graphManager.getStaticGraphTrait().getEdgeIterator(condition)
                     : (Iterator<R>) this.graphManager.getStaticGraphTrait().getEdgeIterator(queryCondition.queryIds, condition);
                break;
            case VE:
                it = queryCondition.isFullScan
                     ? (Iterator<R>) this.graphManager.getStaticGraphTrait().getOneDegreeGraphIterator(condition)
                     : (Iterator<R>) this.graphManager.getStaticGraphTrait().getOneDegreeGraphIterator(queryCondition.queryIds, condition);
                break;
            case PROJECT_FIELD:
                IStatePushDown<K, IEdge<K, EV>, R> projectCondition = condition.withProjector(queryCondition.projector);
                Iterator<Tuple<K, R>> res = queryCondition.isFullScan
                     ? this.graphManager.getStaticGraphTrait().getEdgeProjectIterator(projectCondition)
                     : this.graphManager.getStaticGraphTrait().getEdgeProjectIterator(queryCondition.queryIds, projectCondition);
                it = new IteratorWithFn<>(res, Tuple::getF1);
                break;
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
        return it;
    }

    private Iterator<R> dynamicIterator() {
        StatePushDown condition = getPushDown();
        Iterator<R> it;

        switch (this.type.getType()) {
            case V:
                it = queryCondition.isFullScan
                     ? (Iterator<R>) this.graphManager.getDynamicGraphTrait().getVertexIterator(version, condition)
                     : (Iterator<R>) this.graphManager.getDynamicGraphTrait().getVertexIterator(version, queryCondition.queryIds, condition);
                break;
            case E:
                it = queryCondition.isFullScan
                     ? (Iterator<R>) this.graphManager.getDynamicGraphTrait().getEdgeIterator(version, condition)
                     : (Iterator<R>) this.graphManager.getDynamicGraphTrait().getEdgeIterator(version, queryCondition.queryIds, condition);
                break;
            case VE:
                it = queryCondition.isFullScan
                     ? (Iterator<R>) this.graphManager.getDynamicGraphTrait().getOneDegreeGraphIterator(version, condition)
                     : (Iterator<R>) this.graphManager.getDynamicGraphTrait().getOneDegreeGraphIterator(version, queryCondition.queryIds, condition);
                break;
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
        return it;
    }

    @Override
    public Iterator<R> iterator() {
        if (queryCondition.isFullScan) {
            Preconditions.checkArgument(queryCondition.stateFilters.length <= 1,
                "full scan only support single or none filter now.");
        }
        Iterator<R> it = version < 0 ? staticIterator() : dynamicIterator();
        return new StandardIterator<>(it);
    }

    @Override
    public R get() {
        Iterator<R> it = iterator();
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    @Override
    public Map<K, Long> aggregate() {
        Preconditions.checkArgument(type.getType() == DataType.E, "only edge agg is supported now.");
        Preconditions.checkArgument(version < 0, "only static graph is supported now.");
        Preconditions.checkArgument(queryCondition.limit == null, "limit not supported now.");
        if (queryCondition.queryIds != null) {
            Preconditions.checkArgument(queryCondition.stateFilters.length == 1
                    || queryCondition.stateFilters.length == queryCondition.queryIds.size(),
                "filter number must be 1 or equal to key number.");
        }
        StatePushDown condition = getPushDown();
        return queryCondition.isFullScan
               ? this.graphManager.getStaticGraphTrait().getAggResult(condition)
               : this.graphManager.getStaticGraphTrait().getAggResult(queryCondition.queryIds, condition);
    }
}
