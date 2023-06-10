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

package com.antgroup.geaflow.store.rocksdb.proxy;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.store.rocksdb.RocksdbClient;

public class ProxyBuilder {

    public static <K, VV, EV> IGraphRocksdbProxy<K, VV, EV> build(
        Configuration config, RocksdbClient rocksdbClient,
        IGraphKVEncoder<K, VV, EV> encoder) {
        if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
            return new AsyncGraphRocksdbProxy<>(rocksdbClient, encoder, config);
        } else {
            return new SyncGraphRocksdbProxy<>(rocksdbClient, encoder, config);
        }
    }

    public static <K, VV, EV> IGraphMultiVersionedRocksdbProxy<K, VV, EV> buildMultiVersioned(
        Configuration config, RocksdbClient rocksdbClient,
        IGraphKVEncoder<K, VV, EV> encoder) {
        if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
            return new AsyncGraphMultiVersionedProxy<>(rocksdbClient, encoder, config);
        } else {
            return new SyncGraphMultiVersionedProxy<>(rocksdbClient, encoder, config);
        }
    }
}
