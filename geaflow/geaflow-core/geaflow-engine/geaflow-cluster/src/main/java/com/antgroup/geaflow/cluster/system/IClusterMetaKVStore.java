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

package com.antgroup.geaflow.cluster.system;

import com.antgroup.geaflow.state.key.KeyValueTrait;
import com.antgroup.geaflow.store.context.StoreContext;

public interface IClusterMetaKVStore<K, V> extends KeyValueTrait<K, V> {

    /**
     * Init cluster meta kv store.
     */
    void init(StoreContext storeContext);

    /**
     * Flush meta info into kv store.
     */
    void flush();

    /**
     * Close cluster meta kv store.
     */
    void close();

}
