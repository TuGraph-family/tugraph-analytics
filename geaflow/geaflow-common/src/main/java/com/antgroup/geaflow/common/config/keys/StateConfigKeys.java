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

package com.antgroup.geaflow.common.config.keys;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;
import java.io.Serializable;

public class StateConfigKeys implements Serializable {

    public static final byte[] DELIMITER = "\u0001\u0008".getBytes();

    public static final ConfigKey STATE_ARCHIVED_VERSION_NUM = ConfigKeys
        .key("geaflow.state.archived.version.num")
        .defaultValue(1)
        .description("state archived version number, default 1");

    public static final ConfigKey STATE_PARANOID_CHECK_ENABLE = ConfigKeys
        .key("geaflow.state.paranoid.check.enable")
        .defaultValue(false)
        .description("state paranoid check, default false");

    public static final ConfigKey STATE_WRITE_ASYNC_ENABLE = ConfigKeys
        .key("geaflow.state.write.async.enable")
        .defaultValue(true)
        .description("state async write, default true");

    public static final ConfigKey STATE_WRITE_BUFFER_DEEP_COPY = ConfigKeys
        .key("geaflow.state.write.buffer.deep.copy")
        .defaultValue(false)
        .description("state write buffer deep copy read, default false");

    public static final ConfigKey STATE_WRITE_BUFFER_NUMBER = ConfigKeys
        .key("geaflow.state.write.buffer.number")
        .defaultValue(3)
        .description("state write buffer number, default 3");

    public static final ConfigKey STATE_WRITE_BUFFER_SIZE = ConfigKeys
        .key("geaflow.state.write.buffer.size")
        .defaultValue(10000)
        .description("state write buffer size, default 10000");

    public static final ConfigKey STATE_KV_ENCODER_CLASS = ConfigKeys
        .key("geaflow.state.kv.encoder.class")
        .defaultValue("com.antgroup.geaflow.state.graph.encoder.GraphKVEncoder")
        .description("state kv encoder");

    public static final ConfigKey STATE_KV_ENCODER_EDGE_ORDER = ConfigKeys
        .key("geaflow.state.kv.encoder.edge.order")
        .defaultValue("")
        .description("state kv encoder edge atom order, splitter ,");
}
