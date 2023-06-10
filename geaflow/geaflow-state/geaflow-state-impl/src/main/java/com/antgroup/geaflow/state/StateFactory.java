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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyListStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyMapStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyValueStateDescriptor;
import com.antgroup.geaflow.state.serializer.DefaultKMapSerializer;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StateFactory implements Serializable {

    private static final List<StoreType> SUPPORTED_KEY_STORE_TYPES =
        Arrays.asList(StoreType.ROCKSDB, StoreType.MEMORY);

    public static <K, VV, EV> GraphState<K, VV, EV> buildGraphState(
        GraphStateDescriptor<K, VV, EV> descriptor, Configuration configuration) {
        if (descriptor.getDateModel() == null) {
            descriptor.withDataModel(DataModel.STATIC_GRAPH);
        }
        return new GraphStateImpl<>(new StateContext(descriptor, configuration));
    }

    public static <K, V> KeyValueState<K, V> buildKeyValueState(
        KeyValueStateDescriptor<K, V> descriptor, Configuration configuration) {
        Preconditions.checkArgument(SUPPORTED_KEY_STORE_TYPES.contains(
            StoreType.getEnum(descriptor.getStoreType())),
            "only support %s", SUPPORTED_KEY_STORE_TYPES);
        if (descriptor.getKeySerializer() == null) {
            descriptor.withKVSerializer(new DefaultKVSerializer<>(
                descriptor.getKeyClazz(), descriptor.getValueClazz()));
        }
        descriptor.withDataModel(DataModel.KV);
        return new KeyValueStateImpl<>(new StateContext(descriptor, configuration));
    }

    public static <K, V> KeyListState<K, V> buildKeyListState(
        KeyListStateDescriptor<K, V> descriptor, Configuration configuration) {
        Preconditions.checkArgument(SUPPORTED_KEY_STORE_TYPES.contains(
                StoreType.getEnum(descriptor.getStoreType())),
            "only support %s", SUPPORTED_KEY_STORE_TYPES);
        String storeType = descriptor.getStoreType();
        IStoreBuilder builder = StoreBuilderFactory.build(storeType);
        if (builder.supportedDataModel().contains(DataModel.KList)) {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(new DefaultKVSerializer<>(descriptor.getKeyClazz(),
                    descriptor.getValueClazz()));
            }
            descriptor.withDataModel(DataModel.KList);
            return new KeyListStateImpl<>(new StateContext(descriptor, configuration), descriptor.getValueClazz());
        } else {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(new DefaultKVSerializer<>(descriptor.getKeyClazz(), List.class));
            }
            descriptor.withDataModel(DataModel.KV);
            return new KeyValueListStateImpl<>(new StateContext(descriptor, configuration));
        }

    }

    public static <K, UK, UV> KeyMapState<K, UK, UV> buildKeyMapState(
        KeyMapStateDescriptor<K, UK, UV> descriptor, Configuration configuration) {
        Preconditions.checkArgument(SUPPORTED_KEY_STORE_TYPES.contains(
                StoreType.getEnum(descriptor.getStoreType())),
            "only support %s", SUPPORTED_KEY_STORE_TYPES);
        String storeType = descriptor.getStoreType();
        IStoreBuilder builder = StoreBuilderFactory.build(storeType);
        if (builder.supportedDataModel().contains(DataModel.KMap)) {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(new DefaultKMapSerializer<>(descriptor.getKeyClazz(),
                    descriptor.getSubKeyClazz(), descriptor.getValueClazz()));
            }
            descriptor.withDataModel(DataModel.KMap);
            return new KeyMapStateImpl<>(new StateContext(descriptor, configuration),
                descriptor.getSubKeyClazz(), descriptor.getValueClazz());
        } else {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(
                    new DefaultKVSerializer<>(descriptor.getKeyClazz(), Map.class));
            }
            descriptor.withDataModel(DataModel.KV);
            return new KeyValueMapStateImpl<>(new StateContext(descriptor, configuration));
        }
    }
}
