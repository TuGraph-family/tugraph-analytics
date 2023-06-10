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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.key.KeyListTrait;
import java.util.List;

public class KeyListStateImpl<K, V> extends BaseKeyStateImpl<K> implements KeyListState<K, V> {

    private final KeyListTrait<K, V> trait;

    public KeyListStateImpl(StateContext context, Class<V> valueClazz) {
        super(context);
        this.trait = this.keyStateManager.getKeyListTrait(valueClazz);
    }

    @Override
    public List<V> get(K key) {
        return this.trait.get(key);
    }

    @Override
    public void add(K key, V... value) {
        this.trait.add(key, value);
    }

    @Override
    public void remove(K key) {
        this.trait.remove(key);
    }

    @Override
    public void put(K key, List<V> list) {
        this.trait.remove(key);
        this.trait.add(key, (V[]) list.toArray(new Object[0]));
    }

}
