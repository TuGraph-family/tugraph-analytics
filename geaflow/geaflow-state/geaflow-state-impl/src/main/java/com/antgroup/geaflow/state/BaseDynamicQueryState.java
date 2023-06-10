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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.state.query.QueryCondition;
import com.antgroup.geaflow.state.query.QueryType;
import com.antgroup.geaflow.state.query.QueryableAllGraphState;
import com.antgroup.geaflow.state.query.QueryableAllGraphStateImpl;
import com.antgroup.geaflow.state.query.QueryableKeysGraphState;
import com.antgroup.geaflow.state.query.QueryableKeysGraphStateImpl;
import com.antgroup.geaflow.state.query.QueryableVersionGraphState;
import com.antgroup.geaflow.state.query.QueryableVersionGraphStateImpl;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public abstract class BaseDynamicQueryState<K, VV, EV, R> implements
    DynamicQueryableState<K, VV, EV, R> {

    protected final QueryType<R> queryType;
    protected final IGraphManager<K, VV, EV> graphManager;

    public BaseDynamicQueryState(QueryType<R> queryType, IGraphManager<K, VV, EV> graphManager) {
        this.queryType = queryType;
        this.graphManager = graphManager;
    }

    @Override
    public QueryableAllGraphState<K, VV, EV, R> query(long version) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = null;
        queryCondition.isFullScan = true;
        return new QueryableAllGraphStateImpl<>(version, queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableKeysGraphState<K, VV, EV, R> query(long version, K... ids) {
        return query(version, Arrays.asList(ids));
    }

    @Override
    public QueryableKeysGraphState<K, VV, EV, R> query(long version, List<K> ids) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = ids;
        queryCondition.isFullScan = false;
        return new QueryableKeysGraphStateImpl<>(version, queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableVersionGraphState<K, VV, EV, R> query(K id) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = Arrays.asList(id);
        return new QueryableVersionGraphStateImpl<>(queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableVersionGraphState<K, VV, EV, R> query(K id, Collection<Long> versions) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = Arrays.asList(id);
        queryCondition.versions = versions;
        return new QueryableVersionGraphStateImpl<>(queryType, graphManager, queryCondition);
    }

    @Override
    public Iterator<K> idIterator() {
        return this.graphManager.getDynamicGraphTrait().vertexIDIterator();
    }
}
