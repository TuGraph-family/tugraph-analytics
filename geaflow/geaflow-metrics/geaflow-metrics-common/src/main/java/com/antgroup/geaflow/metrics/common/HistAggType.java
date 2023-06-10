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

package com.antgroup.geaflow.metrics.common;

public enum HistAggType {

    /**
     * Default histogram aggregation types.
     */
    DEFAULT(new String[]{"max", "p999", "p99", "p95", "p50"}),

    /**
     * Default histogram aggregation MAX.
     */
    MAX(new String[]{"max"}),

    /**
     * Default histogram aggregation MIN.
     */
    MIN(new String[]{"min"}),

    /**
     * Default histogram aggregation p999.
     */
    P999(new String[]{"p999"}),

    /**
     * Default histogram aggregation p99.
     */
    P99(new String[]{"p99"}),

    /**
     * Default histogram aggregation p95.
     */
    P95(new String[]{"p95"}),

    /**
     * Default histogram aggregation p50.
     */
    P50(new String[]{"p50"});

    private String[] aggTypes;

    HistAggType(String[] aggTypes) {
        this.aggTypes = aggTypes;
    }

    public String[] getAggTypes() {
        return aggTypes;
    }

}
