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

import com.antgroup.geaflow.store.AbstractBaseStore;
import com.antgroup.geaflow.store.api.key.IKListStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KListMemoryStore<K, V> extends AbstractBaseStore implements IKListStore<K, V> {

    private Map<K, List<V>> memoryStore = new HashMap<>();

    @Override
    public void add(K key, V... value) {
        List<V> list = memoryStore.computeIfAbsent(key, k -> new ArrayList<>());
        list.addAll(Arrays.asList(value));
        this.memoryStore.put(key, list);
    }

    @Override
    public void remove(K key) {
        this.memoryStore.remove(key);
    }

    @Override
    public List<V> get(K key) {
        return this.memoryStore.getOrDefault(key, new ArrayList<>());
    }

    @Override
    public void init(StoreContext storeContext) {

    }

    @Override
    public void close() {
        this.memoryStore.clear();
    }

    @Override
    public void drop() {
        this.memoryStore = null;
    }
}
