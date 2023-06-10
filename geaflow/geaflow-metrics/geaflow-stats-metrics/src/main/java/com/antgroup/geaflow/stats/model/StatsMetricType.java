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

package com.antgroup.geaflow.stats.model;

public enum StatsMetricType {

    /** exception log. */
    Exception("_exception_"),

    /** runtime metrics. */
    Metrics("_metrics_"),

    /** runtime heartbeat map. */
    Heartbeat("_heartbeat_");

    private final String value;

    StatsMetricType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
