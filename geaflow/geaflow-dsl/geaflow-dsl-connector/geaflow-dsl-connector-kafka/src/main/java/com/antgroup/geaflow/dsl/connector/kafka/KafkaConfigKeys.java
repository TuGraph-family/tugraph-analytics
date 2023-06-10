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

package com.antgroup.geaflow.dsl.connector.kafka;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class KafkaConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_KAFKA_SERVERS = ConfigKeys
        .key("geaflow.dsl.kafka.servers")
        .noDefaultValue()
        .description("The kafka bootstrap servers list.");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_TOPIC = ConfigKeys
        .key("geaflow.dsl.kafka.topic")
        .noDefaultValue()
        .description("The kafka topic.");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_GROUP_ID = ConfigKeys
        .key("geaflow.dsl.kafka.group.id")
        .defaultValue("default-group-id")
        .description("The kafka group id, default is 'default-group-id'.");
}
