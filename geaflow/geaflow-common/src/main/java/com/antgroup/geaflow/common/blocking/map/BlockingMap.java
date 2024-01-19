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

package com.antgroup.geaflow.common.blocking.map;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class BlockingMap<K, V> {
    private Map<K, ArrayBlockingQueue<V>> map = new ConcurrentHashMap<>();

    private BlockingQueue<V> getQueue(K key) {
        return map.computeIfAbsent(key, k -> new ArrayBlockingQueue<>(1));
    }

    public void put(K key, V value) {
        if (!getQueue(key).offer(value)) {
            throw new GeaflowRuntimeException(String.format("BlockingMap offer element (%s, %s) failed.", key, value));
        }
    }

    public V get(K key) throws InterruptedException {
        return getQueue(key).take();
    }

    public V get(K key, long timeout, TimeUnit unit) throws InterruptedException {
        return getQueue(key).poll(timeout, unit);
    }
}
