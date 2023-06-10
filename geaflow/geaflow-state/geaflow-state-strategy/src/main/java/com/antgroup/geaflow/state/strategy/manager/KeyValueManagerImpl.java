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

package com.antgroup.geaflow.state.strategy.manager;

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.key.KeyValueTrait;
import com.antgroup.geaflow.state.strategy.accessor.IAccessor;
import java.util.Map;

public class KeyValueManagerImpl<K, V> extends BaseShardManager<K, KeyValueTrait<K, V>> implements KeyValueTrait<K, V> {

    public KeyValueManagerImpl(StateContext context, Map<Integer, IAccessor> accessorMap) {
        super(context, accessorMap);
    }

    @Override
    public V get(K key) {
        return getTraitByKey(key).get(key);
    }

    @Override
    public void put(K key, V value) {
        getTraitByKey(key).put(key, value);
    }

    @Override
    public void remove(K key) {
        getTraitByKey(key).remove(key);
    }
}
