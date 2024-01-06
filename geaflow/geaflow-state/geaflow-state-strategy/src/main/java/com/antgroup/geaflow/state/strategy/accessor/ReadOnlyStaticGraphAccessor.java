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

package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.graph.IGraphStore;
import java.util.List;

public class ReadOnlyStaticGraphAccessor<K, VV, EV> extends RWStaticGraphAccessor<K, VV, EV> {

    private final ReadOnlyGraph<K, VV, EV> readOnlyGraph = new ReadOnlyGraph<>();

    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.readOnlyGraph.init(context, storeBuilder);
    }

    protected List<ActionType> allowActionTypes() {
        return this.readOnlyGraph.allowActionTypes();
    }

    public void doStoreAction(int shard, ActionType actionType, ActionRequest request) {
        this.readOnlyGraph.doStoreAction(shard, actionType, request);
    }

    public IGraphStore<K, VV, EV> getStore() {
        return (IGraphStore<K, VV, EV>) this.readOnlyGraph.getStore();
    }
}