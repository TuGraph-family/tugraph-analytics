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

package com.antgroup.geaflow.console.core.model.plugin.config;

import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Jedis;

@Getter
@Setter
public class RedisPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.store.redis.host", comment = "i18n.key.host")
    @GeaflowConfigValue(required = true, defaultValue = "0.0.0.0")
    private String host;

    @GeaflowConfigKey(value = "geaflow.store.redis.port", comment = "i18n.key.port")
    @GeaflowConfigValue(required = true, defaultValue = "6379")
    private Integer port;

    @GeaflowConfigKey(value = "geaflow.store.redis.retry.times", comment = "i18n.key.retry.times")
    @GeaflowConfigValue(defaultValue = "10")
    private Integer retryTimes;

    public RedisPluginConfigClass() {
        super(GeaflowPluginType.REDIS);
    }

    @Override
    public void testConnection() {
        try (Jedis jedis = new Jedis(host, port)) {
            jedis.ping();

        } catch (Exception e) {
            throw new GeaflowIllegalException("Redis connection test failed, caused by {}", e.getMessage(), e);
        }
    }
}
