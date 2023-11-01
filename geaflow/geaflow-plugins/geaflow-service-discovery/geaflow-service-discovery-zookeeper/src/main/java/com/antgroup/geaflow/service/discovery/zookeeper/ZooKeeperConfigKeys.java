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

package com.antgroup.geaflow.service.discovery.zookeeper;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;
import java.io.Serializable;

public class ZooKeeperConfigKeys implements Serializable {

    public static final ConfigKey ZOOKEEPER_BASE_NODE = ConfigKeys
        .key("geaflow.zookeeper.znode.parent")
        .noDefaultValue()
        .description("zookeeper base node");

    public static final ConfigKey ZOOKEEPER_QUORUM_SERVERS = ConfigKeys
        .key("geaflow.zookeeper.quorum.servers")
        .noDefaultValue()
        .description("zookeeper quorum servers");

    public static final ConfigKey ZOOKEEPER_SESSION_TIMEOUT = ConfigKeys
        .key("geaflow.zookeeper.session.timeout")
        .defaultValue(30 * 1000)
        .description("zookeeper session timeout");

    public static final ConfigKey ZOOKEEPER_RETRY = ConfigKeys
        .key("geaflow.zookeeper.retry.count")
        .defaultValue(5)
        .description("zookeeper retry count");

    public static final ConfigKey ZOOKEEPER_RETRY_INTERVAL_MILL = ConfigKeys
        .key("geaflow.zookeeper.retry.interval.mill")
        .defaultValue(1000)
        .description("zookeeper retry interval");

}
