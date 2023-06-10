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

package com.antgroup.geaflow.cluster.system;

import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.IKVStore;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.store.context.StoreContext;

public class MemoryClusterMetaKVStore<V> implements IClusterMetaKVStore<String, V> {

    private IKVStore<String, Object> kvStore;

    @Override
    public void init(StoreContext storeContext) {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.MEMORY.name());
        kvStore = (IKVStore<String, Object>) builder.getStore(DataModel.KV, storeContext.getConfig());
    }

    @Override
    public void flush() {

    }

    @Override
    public V get(String key) {
        return (V) kvStore.get(key);
    }

    @Override
    public void put(String key, V value) {
        kvStore.put(key, value);
    }

    @Override
    public void remove(String key) {
        kvStore.remove(key);
    }

    @Override
    public void close() {
        kvStore.close();
    }
}
