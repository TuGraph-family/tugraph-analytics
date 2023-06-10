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

package com.antgroup.geaflow.store.redis;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class RedisConfigKeys {

    public static final ConfigKey REDIS_HOST = ConfigKeys
        .key("geaflow.store.redis.host")
        .noDefaultValue()
        .description("geaflow store redis server host");

    public static final ConfigKey REDIS_PORT = ConfigKeys
        .key("geaflow.store.redis.port")
        .defaultValue(6379)
        .description("geaflow store redis server port");

    public static final ConfigKey REDIS_RETRY_TIMES = ConfigKeys
        .key("geaflow.store.redis.retry.times")
        .defaultValue(10)
        .description("geaflow store redis retry times");

    public static final ConfigKey REDIS_RETRY_INTERVAL_MS = ConfigKeys
        .key("geaflow.store.redis.retry.interval.ms")
        .defaultValue(500)
        .description("geaflow store redis retry interval ms");

    public static final ConfigKey REDIS_PASSWORD = ConfigKeys.key("geaflow.store.redis.password")
        .defaultValue("")
        .description("redis connect password");

    public static final ConfigKey REDIS_CONNECT_TIMEOUT = ConfigKeys
        .key("geaflow.store.redis.connection.timeout")
        .defaultValue(5000)
        .description("redis connect timeout in ms");

}
