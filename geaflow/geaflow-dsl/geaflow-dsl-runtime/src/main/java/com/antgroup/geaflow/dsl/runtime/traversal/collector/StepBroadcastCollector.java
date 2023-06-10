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

package com.antgroup.geaflow.dsl.runtime.traversal.collector;

import com.antgroup.geaflow.dsl.common.data.StepRecord;
import java.util.List;
import java.util.Objects;

public class StepBroadcastCollector<OUT extends StepRecord> implements StepCollector<OUT> {

    private final List<StepCollector<OUT>> collectors;

    public StepBroadcastCollector(List<StepCollector<OUT>> collectors) {
        this.collectors = Objects.requireNonNull(collectors);
    }

    @Override
    public void collect(OUT record) {
        for (StepCollector<OUT> collector : collectors) {
            collector.collect(record);
        }
    }
}
