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

package com.antgroup.geaflow.ha.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.redis.KVRedisStore;

public class RedisHAService extends AbstractHAService {

    @Override
    public void open(Configuration configuration) {
        super.open(configuration);
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.REDIS.name());
        this.kvStore = (KVRedisStore) builder.getStore(DataModel.KV, configuration);
        String namespace =
            configuration.getString(ExecutionConfigKeys.JOB_UNIQUE_ID) + TABLE_PREFIX;
        StoreContext storeContext = new StoreContext(namespace);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, null));
        storeContext.withConfig(configuration);
        this.kvStore.init(storeContext);
    }

    @Override
    public void register(String resourceId, ResourceData resourceData) {
        synchronized (kvStore) {
            kvStore.put(resourceId, resourceData);
        }
    }

    @Override
    protected ResourceData getResourceData(String resourceId) {
        synchronized (kvStore) {
            return kvStore.get(resourceId);
        }
    }

}
