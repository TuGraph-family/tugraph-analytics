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
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallQueryExpression.CallState;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallQueryProxy;

public class StepWaitCallQueryCollector<OUT extends StepRecord> implements StepCollector<OUT> {

    private final CallQueryProxy callQueryProxy;

    private final StepCollector<OUT> baseCollector;

    public StepWaitCallQueryCollector(CallQueryProxy callQueryProxy, StepCollector<OUT> baseCollector) {
        this.callQueryProxy = callQueryProxy;
        this.baseCollector = baseCollector;
    }

    @Override
    public void collect(OUT record) {
        // Only when the calling query has returned, we can collect the record to the next operator.
        if (callQueryProxy.getCallState() == CallState.RETURNING
            || callQueryProxy.getCallState() == CallState.FINISH) {
            baseCollector.collect(record);
        }
    }
}
