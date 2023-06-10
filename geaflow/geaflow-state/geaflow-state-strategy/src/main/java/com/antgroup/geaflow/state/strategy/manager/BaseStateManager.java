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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.action.hook.ActionHook;
import com.antgroup.geaflow.state.action.hook.ActionHookBuilder;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.strategy.accessor.AccessorBuilder;
import com.antgroup.geaflow.state.strategy.accessor.IAccessor;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseStateManager {

    protected StateContext context;
    protected Configuration config;
    protected MetricGroup metricGroup;
    protected KeyGroup keyGroup;
    protected Map<Integer, IAccessor> accessorMap = new HashMap<>();
    private List<ActionHook> actionHooks;

    public void init(StateContext context) {
        this.context = context;
        this.config = context.getConfig();
        this.metricGroup = context.getMetricGroup();
        this.keyGroup = context.getKeyGroup();
        IStoreBuilder storeBuilder = StoreBuilderFactory.build(context.getStoreType());
        this.context.withLocalStore(storeBuilder.getStoreDesc().isLocalStore());
        this.actionHooks = ActionHookBuilder.buildHooks(storeBuilder.getStoreDesc());

        for (int shardId = keyGroup.getStartKeyGroup(); shardId <= keyGroup.getEndKeyGroup(); shardId++) {
            IAccessor accessor = AccessorBuilder.getAccessor(context.getDataModel(), context.getStateMode());
            StateContext newContext = context.clone().withShardId(shardId);
            accessor.init(newContext, storeBuilder);

            this.accessorMap.put(shardId, accessor);
        }
        for (ActionHook hook: actionHooks) {
            hook.init(context, this.accessorMap);
        }
    }

    public void doStoreAction(ActionType actionType, ActionRequest request) {
        for (ActionHook hook: actionHooks) {
            hook.doStoreAction(actionType, request);
        }
        this.accessorMap.values().forEach(c -> c.doStoreAction(actionType, request));
    }
}
