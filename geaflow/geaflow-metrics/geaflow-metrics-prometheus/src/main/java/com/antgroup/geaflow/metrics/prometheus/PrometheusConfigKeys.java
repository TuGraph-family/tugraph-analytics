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

package com.antgroup.geaflow.metrics.prometheus;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class PrometheusConfigKeys {

    public static final ConfigKey GATEWAY_URL = ConfigKeys
        .key("geaflow.metric.prometheus.gateway.url")
        .noDefaultValue()
        .description("prometheus gateway url");

    public static final ConfigKey USER = ConfigKeys
        .key("geaflow.metric.prometheus.auth.user")
        .noDefaultValue()
        .description("prometheus auth user");

    public static final ConfigKey PASSWORD = ConfigKeys
        .key("geaflow.metric.prometheus.auth.password")
        .noDefaultValue()
        .description("prometheus auth password");

    public static final ConfigKey JOB_NAME = ConfigKeys
        .key("geaflow.metric.prometheus.job.name")
        .defaultValue("DEFAULT")
        .description("prometheus job name format. The origin metric name will replace the '%s' "
            + "in the format.");
}
