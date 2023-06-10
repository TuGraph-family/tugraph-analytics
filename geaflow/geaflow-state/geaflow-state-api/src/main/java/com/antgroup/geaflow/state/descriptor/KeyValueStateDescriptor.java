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

import com.antgroup.geaflow.state.serializer.IKVSerializer;
import java.util.function.Supplier;

public class KeyValueStateDescriptor<K, V> extends BaseKeyDescriptor<K> {

    private Supplier<V> defaultValueSupplier;
    private Class<V> valueClazz;

    protected KeyValueStateDescriptor(String name, String storeType) {
        super(name, storeType);
    }

    @Override
    public DescriptorType getDescriptorType() {
        return DescriptorType.KEY_VALUE;
    }

    public static <K, V> KeyValueStateDescriptor<K, V> build(String name, String storeType) {
        return new KeyValueStateDescriptor<>(name, storeType);
    }

    public KeyValueStateDescriptor<K, V> withDefaultValue(Supplier<V> valueSupplier) {
        this.defaultValueSupplier = valueSupplier;
        return this;
    }

    public KeyValueStateDescriptor<K, V> withTypeInfo(Class<K> keyClazz, Class<V> valueClazz) {
        this.keyClazz = keyClazz;
        this.valueClazz = valueClazz;
        return this;
    }

    public KeyValueStateDescriptor<K, V> withKVSerializer(IKVSerializer<K, V> kvSerializer) {
        this.keySerializer = kvSerializer;
        return this;
    }

    public V getDefaultValue() {
        if (defaultValueSupplier == null) {
            return null;
        }
        // It is better to get a default copy.
        return defaultValueSupplier.get();
    }

    public Class<V> getValueClazz() {
        return valueClazz;
    }
}
