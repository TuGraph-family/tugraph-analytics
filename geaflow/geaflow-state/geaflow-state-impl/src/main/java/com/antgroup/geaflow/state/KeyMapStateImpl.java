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
import com.antgroup.geaflow.state.key.KeyMapTrait;
import java.util.List;
import java.util.Map;

public class KeyMapStateImpl<K, UK, UV> extends BaseKeyStateImpl<K> implements KeyMapState<K, UK, UV> {

    private final KeyMapTrait<K, UK, UV> trait;

    public KeyMapStateImpl(StateContext context, Class<UK> subKeyClazz, Class<UV> valueClazz) {
        super(context);
        this.trait = this.keyStateManager.getKeyMapTrait(subKeyClazz, valueClazz);
    }

    @Override
    public Map<UK, UV> get(K key) {
        return this.trait.get(key);
    }

    @Override
    public List<UV> get(K key, UK... subKeys) {
        return this.trait.get(key, subKeys);
    }

    @Override
    public void add(K key, UK subKey, UV value) {
        this.trait.add(key, subKey, value);
    }

    @Override
    public void add(K key, Map<UK, UV> map) {
        this.trait.add(key, map);
    }

    @Override
    public void put(K key, Map<UK, UV> map) {
        remove(key);
        this.trait.add(key, map);
    }

    @Override
    public void remove(K key) {
        this.trait.remove(key);
    }

    @Override
    public void remove(K key, UK... subKeys) {
        this.trait.remove(key, subKeys);
    }
}
