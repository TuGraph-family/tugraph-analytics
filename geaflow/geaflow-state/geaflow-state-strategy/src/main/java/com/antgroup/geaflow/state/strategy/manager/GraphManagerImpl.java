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

package com.antgroup.geaflow.state.strategy.manager;

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.graph.DynamicGraphTrait;
import com.antgroup.geaflow.state.graph.StaticGraphTrait;
import com.antgroup.geaflow.state.pushdown.inner.IFilterConverter;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.api.graph.IPushDownStore;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphManagerImpl<K, VV, EV> extends BaseStateManager implements IGraphManager<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphManagerImpl.class);

    private StaticGraphTrait<K, VV, EV> staticGraphTrait;
    private DynamicGraphTrait<K, VV, EV> dynamicGraphTrait;
    private IFilterConverter filterConverter;

    public GraphManagerImpl() {

    }

    @Override
    public void init(StateContext context) {
        super.init(context);
        if (this.accessorMap.values().size() > 0) {
            IBaseStore store = this.accessorMap.values().iterator().next().getStore();
            if (store instanceof IPushDownStore) {
                filterConverter = ((IPushDownStore) store).getFilterConverter();
            }
        }
    }

    @Override
    public StaticGraphTrait<K, VV, EV> getStaticGraphTrait() {
        if (staticGraphTrait == null) {
            staticGraphTrait = new StaticGraphManagerImpl<>(this.context, this.accessorMap);
        }
        return staticGraphTrait;
    }

    @Override
    public DynamicGraphTrait<K, VV, EV> getDynamicGraphTrait() {
        if (dynamicGraphTrait == null) {
            dynamicGraphTrait = new DynamicGraphManagerImpl<>(this.context, this.accessorMap);
        }
        return dynamicGraphTrait;
    }

    @Override
    public IFilterConverter getFilterConverter() {
        return Preconditions.checkNotNull(filterConverter);
    }
}
