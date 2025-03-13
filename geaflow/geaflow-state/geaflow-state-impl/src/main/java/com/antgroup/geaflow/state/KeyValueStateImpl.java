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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.descriptor.KeyValueStateDescriptor;
import com.antgroup.geaflow.state.key.KeyValueTrait;

public class KeyValueStateImpl<K, V> extends BaseKeyStateImpl<K> implements KeyValueState<K, V> {

    private final KeyValueStateDescriptor<K, V> desc;
    private final KeyValueTrait<K, V> trait;

    public KeyValueStateImpl(StateContext context) {
        super(context);
        this.desc = (KeyValueStateDescriptor<K, V>) context.getDescriptor();
        trait = this.keyStateManager.getKeyValueTrait(desc.getValueClazz());
    }

    @Override
    public V get(K k) {
        V res = this.trait.get(k);
        if (res == null) {
            res = desc.getDefaultValue();
        }
        return res;
    }

    @Override
    public void put(K k, V value) {
        this.trait.put(k, value);
    }

    @Override
    public void remove(K key) {
        this.trait.remove(key);
    }
}
