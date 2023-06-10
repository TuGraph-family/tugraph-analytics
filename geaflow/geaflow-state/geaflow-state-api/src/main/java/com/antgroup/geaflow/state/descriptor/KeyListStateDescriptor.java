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

package com.antgroup.geaflow.state.descriptor;

import com.antgroup.geaflow.state.serializer.IKeySerializer;

public class KeyListStateDescriptor<K, V> extends BaseKeyDescriptor<K> {

    private Class<V> valueClazz;

    protected KeyListStateDescriptor(String name, String storeType) {
        super(name, storeType);
    }

    @Override
    public DescriptorType getDescriptorType() {
        return DescriptorType.KEY_LIST;
    }

    public static <K, V> KeyListStateDescriptor<K, V> build(String name, String storeType) {
        return new KeyListStateDescriptor<>(name, storeType);
    }

    public KeyListStateDescriptor<K, V> withTypeInfo(Class<K> keyClazz, Class<V> valueClazz) {
        this.keyClazz = keyClazz;
        this.valueClazz = valueClazz;
        return this;
    }

    public KeyListStateDescriptor<K, V> withKeySerializer(IKeySerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public Class<V> getValueClazz() {
        return valueClazz;
    }
}
