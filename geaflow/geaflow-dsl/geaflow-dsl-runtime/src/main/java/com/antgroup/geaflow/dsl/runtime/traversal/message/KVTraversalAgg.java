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

package com.antgroup.geaflow.dsl.runtime.traversal.message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KVTraversalAgg<K, V> implements ITraversalAgg {

    private final Map<K, V> map;

    public KVTraversalAgg() {
        this.map = new HashMap<>();
    }

    public KVTraversalAgg(Map<K, V> map) {
        this.map = new HashMap<>(map);
    }

    public KVTraversalAgg(K key, V value) {
        this.map = Collections.singletonMap(key, value);
    }

    public Map<K, V> getMap() {
        return map;
    }

    public V get(K key) {
        return this.map.get(key);
    }

    public void clear() {
        this.map.clear();
    }

    public KVTraversalAgg<K, V> copy() {
        return new KVTraversalAgg<>(map);
    }

    public static <K, V> KVTraversalAgg<K, V> empty() {
        return new KVTraversalAgg();
    }

    @Override
    public String toString() {
        return "KVTraversalAgg{" + "map=" + map + '}';
    }
}
