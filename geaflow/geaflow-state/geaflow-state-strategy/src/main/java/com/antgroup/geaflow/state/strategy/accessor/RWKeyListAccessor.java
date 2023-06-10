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

package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.key.KeyListTrait;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.IKListStore;
import java.util.List;

public class RWKeyListAccessor<K, V> extends RWKeyAccessor<K> implements KeyListTrait<K, V> {

    private IKListStore<K, V> kListStore;

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        super.init(context, storeBuilder);
        this.kListStore = (IKListStore<K, V>) store;
    }

    @Override
    public List<V> get(K key) {
        return this.kListStore.get(key);
    }

    @Override
    public void add(K key, V... value) {
        this.kListStore.add(key, value);
    }

    @Override
    public void remove(K key) {
        this.kListStore.remove(key);
    }
}
