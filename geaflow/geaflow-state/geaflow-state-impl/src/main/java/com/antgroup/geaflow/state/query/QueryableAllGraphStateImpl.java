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

import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;

public class QueryableAllGraphStateImpl<K, VV, EV, R> extends QueryableGraphStateImpl<K, VV, EV, R>
    implements QueryableAllGraphState<K, VV, EV, R> {

    public QueryableAllGraphStateImpl(Long version, QueryType<R> type,
                                      IGraphManager<K, VV, EV> graphManager,
                                      QueryCondition<K> queryCondition) {
        super(version, type, graphManager);
        this.queryCondition = queryCondition;
    }

    public QueryableAllGraphStateImpl(QueryType<R> type,
                                      IGraphManager<K, VV, EV> graphManager,
                                      QueryCondition<K> queryCondition) {
        super(type, graphManager);
        this.queryCondition = queryCondition;
    }

    @Override
    public QueryableGraphState<K, VV, EV, R> by(IFilter filter) {
        queryCondition.stateFilters[0] = filter;
        return this;
    }
}
