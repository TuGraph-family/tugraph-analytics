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

package com.antgroup.geaflow.store.memory;

import com.antgroup.geaflow.store.IStatefulStore;
import com.antgroup.geaflow.store.api.key.IKMapStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KMapMemoryStore<K, UK, UV> implements IStatefulStore, IKMapStore<K, UK, UV> {

    private Map<K, Map<UK, UV>> memoryStore = new HashMap<>();

    @Override
    public Map<UK, UV> get(K key) {
        return memoryStore.getOrDefault(key, new HashMap<>());
    }

    @Override
    public List<UV> get(K key, UK... subKeys) {
        Map<UK, UV> map = get(key);
        List<UV> list = new ArrayList<>(subKeys.length);
        Arrays.stream(subKeys).forEach(c -> list.add(map.get(c)));
        return list;
    }

    @Override
    public void add(K key, UK subKey, UV value) {
        Map<UK, UV> map = memoryStore.computeIfAbsent(key, k -> new HashMap<>());
        map.put(subKey, value);
    }

    @Override
    public void add(K key, Map<UK, UV> map) {
        Map<UK, UV> tmp = memoryStore.computeIfAbsent(key, k -> new HashMap<>());
        tmp.putAll(map);
    }

    @Override
    public void remove(K key) {
        this.memoryStore.remove(key);
    }

    @Override
    public void remove(K key, UK... subKeys) {
        if (memoryStore.containsKey(key)) {
            Map<UK, UV> map = get(key);
            Arrays.stream(subKeys).forEach(map::remove);
        }
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
