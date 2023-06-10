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

package com.antgroup.geaflow.collector;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.metrics.common.api.Meter;

public abstract class AbstractCollector {

    protected int id;
    protected RuntimeContext runtimeContext;
    protected Meter outputMeter;

    public AbstractCollector(int id) {
        this.id = id;
    }

    public void setUp(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void setOutputMetric(Meter outputMeter) {
        this.outputMeter = outputMeter;
    }

    public int getId() {
        return id;
    }

    public void finish() {}

    public void close() {}
}
