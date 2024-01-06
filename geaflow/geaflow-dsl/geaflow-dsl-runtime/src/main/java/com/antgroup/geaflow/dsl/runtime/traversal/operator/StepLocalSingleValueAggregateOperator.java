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

package com.antgroup.geaflow.dsl.runtime.traversal.operator;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import java.util.HashMap;
import java.util.Map;

public class StepLocalSingleValueAggregateOperator extends AbstractStepOperator<StepAggregateFunction, Row, Row> {

    private Map<ParameterRequest, Object> requestId2Accumulators;

    public StepLocalSingleValueAggregateOperator(long id, StepAggregateFunction function) {
        super(id, function);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        requestId2Accumulators = new HashMap<>();
    }

    @Override
    protected void processRecord(Row record) {
        ParameterRequest request = context.getRequest();
        Object accumulator = requestId2Accumulators.computeIfAbsent(request,
            r -> function.createAccumulator());
        function.add(record, accumulator);
    }

    @Override
    public void finish() {
        for (Map.Entry<ParameterRequest, Object> entry : requestId2Accumulators.entrySet()) {
            ParameterRequest request = entry.getKey();
            Object accumulator = entry.getValue();
            Row localResult = ObjectRow.create(accumulator);

            context.setRequest(request);
            collect(localResult);
        }
        requestId2Accumulators.clear();
        super.finish();
    }

    @Override
    public StepOperator<Row, Row> copyInternal() {
        return new StepLocalSingleValueAggregateOperator(id, function);
    }
}
