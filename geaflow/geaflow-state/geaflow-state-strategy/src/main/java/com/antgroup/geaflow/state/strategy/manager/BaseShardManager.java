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

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.strategy.accessor.IAccessor;
import com.antgroup.geaflow.utils.keygroup.IKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseShardManager<K, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseShardManager.class);

    protected final int totalShardNum;
    protected Map<Integer, T> traitMap;
    protected KeyGroup shardGroup;
    protected IKeyGroupAssigner assigner;
    protected boolean mayScale;

    public BaseShardManager(StateContext context, Map<Integer, IAccessor> accessorMap) {
        this.shardGroup = context.getKeyGroup();
        this.assigner = context.getDescriptor().getAssigner();
        Preconditions.checkArgument(this.assigner != null, "The assigner must be not null");
        LOGGER.info("key group {}, key group num {}", this.shardGroup, this.assigner.getKeyGroupNumber());
        this.mayScale = context.isLocalStore();
        this.totalShardNum = this.assigner.getKeyGroupNumber();
        this.traitMap = new HashMap<>(accessorMap.size());
        for (Entry<Integer, IAccessor> entry: accessorMap.entrySet()) {
            this.traitMap.put(entry.getKey(), (T) entry.getValue());
        }
    }

    protected T getTraitByKey(K key) {
        return getTraitById(assigner.assign(key));
    }

    protected T getTraitById(int keyGroupId) {
        T trait = traitMap.get(keyGroupId);
        if (trait == null) {
            throw new GeaflowRuntimeException(
                "we have " + traitMap.keySet() + " need keyGroupId " + keyGroupId);

        }
        return trait;
    }
}
