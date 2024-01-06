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

package com.antgroup.geaflow.store.iterator;

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class KeysIterator<K, VV, EV, R> implements CloseableIterator<R> {

    private final Iterator<K> iterator;
    private final BiFunction<K, IStatePushDown, R> fetchFun;
    private Function<K, IStatePushDown> pushdownFun;
    private R nextValue;

    public KeysIterator(List<K> keys,
        BiFunction<K, IStatePushDown, R> fetchFun,
        IStatePushDown pushdown) {
        this.fetchFun = fetchFun;
        this.iterator = keys.iterator();
        if (pushdown.getFilters() != null) {
            StatePushDown simpleKeyPushDown = StatePushDown.of()
                .withEdgeLimit(pushdown.getEdgeLimit())
                .withOrderFields(pushdown.getOrderFields());
            this.pushdownFun = k -> simpleKeyPushDown.withFilter(
                (IFilter) pushdown.getFilters().get(k));
        } else {
            this.pushdownFun = k -> pushdown;
        }
    }

    @Override
    public boolean hasNext() {
        while (iterator.hasNext()) {
            K key = iterator.next();
            IStatePushDown pushdown = pushdownFun.apply(key);
            nextValue = fetchFun.apply(key, pushdown);
            if (nextValue == null) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public R next() {
        return nextValue;
    }

    @Override
    public void close() {

    }
}
