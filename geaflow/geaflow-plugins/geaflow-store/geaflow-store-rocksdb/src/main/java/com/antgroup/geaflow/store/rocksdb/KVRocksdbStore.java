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

package com.antgroup.geaflow.store.rocksdb;

import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.DEFAULT_CF;

import com.antgroup.geaflow.state.serializer.IKVSerializer;
import com.antgroup.geaflow.store.api.key.IKVStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;

public class KVRocksdbStore<K, V> extends BaseRocksdbStore implements IKVStore<K, V> {

    private IKVSerializer<K, V> kvSerializer;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.kvSerializer = (IKVSerializer<K, V>) Preconditions.checkNotNull(storeContext.getKeySerializer(),
            "keySerializer must be set");
    }

    @Override
    protected List<String> getCfList() {
        return Arrays.asList(DEFAULT_CF);
    }

    @Override
    public void put(K key, V value) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] valueArray = this.kvSerializer.serializeValue(value);
        this.rocksdbClient.write(DEFAULT_CF, keyArray, valueArray);
    }

    @Override
    public void remove(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        this.rocksdbClient.delete(DEFAULT_CF, keyArray);
    }

    @Override
    public V get(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] valueArray = this.rocksdbClient.get(DEFAULT_CF, keyArray);
        if (valueArray == null) {
            return null;
        }
        return this.kvSerializer.deserializeValue(valueArray);
    }
}
