/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.store.memory;

import com.antgroup.geaflow.store.IStatefulStore;
import com.antgroup.geaflow.store.api.key.IKVStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.HashMap;
import java.util.Map;

public class KVMemoryStore<K, V> implements IStatefulStore, IKVStore<K, V> {

    private Map<K, V> memoryStore = new HashMap<>();

    @Override
    public void put(K key, V value) {
        this.memoryStore.put(key, value);
    }

    @Override
    public void remove(K key) {
        this.memoryStore.remove(key);
    }

    @Override
    public V get(K key) {
        return this.memoryStore.get(key);
    }

    @Override
    public void init(StoreContext storeContext) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        this.memoryStore.clear();
    }

    @Override
    public void archive(long checkpointId) {

    }

    @Override
    public void recovery(long checkpointId) {

    }

    @Override
    public long recoveryLatest() {
        return 0;
    }

    @Override
    public void compact() {

    }

    @Override
    public void drop() {

    }
}
