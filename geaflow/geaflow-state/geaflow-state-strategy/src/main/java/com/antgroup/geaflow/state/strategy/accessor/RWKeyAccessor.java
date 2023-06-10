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

import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.descriptor.BaseKeyDescriptor;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RWKeyAccessor<K> extends BaseActionAccess implements IAccessor {

    protected IBaseStore store;

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.store = storeBuilder.getStore(context.getDataModel(), context.getConfig());

        BaseKeyDescriptor<K> desc = (BaseKeyDescriptor<K>) context.getDescriptor();

        StoreContext storeContext = new StoreContext(context.getName())
            .withConfig(context.getConfig())
            .withMetricGroup(context.getMetricGroup())
            .withShardId(context.getShardId())
            .withKeySerializer(desc.getKeySerializer());

        this.store.init(storeContext);
        initAction(this.store, context);
    }

    @Override
    public IBaseStore getStore() {
        return this.store;
    }

    protected List<ActionType> allowActionTypes() {
        return Stream.of(ActionType.values()).collect(Collectors.toList());
    }
}
