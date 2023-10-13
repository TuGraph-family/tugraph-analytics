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

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.data.SingleValue;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.message.KeyGroupMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageType;
import java.util.HashMap;
import java.util.Map;

public class StepGlobalSingleValueAggregateOperator extends AbstractStepOperator<StepAggregateFunction, VertexRecord, SingleValue> {

    private final IType<?> inputType;

    private Map<ParameterRequest, Object> requestId2Accumulators;

    public StepGlobalSingleValueAggregateOperator(long id, IType<?> inputType, StepAggregateFunction function) {
        super(id, function);
        this.inputType = inputType;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        requestId2Accumulators = new HashMap<>();
    }

    @Override
    protected void processRecord(VertexRecord record) {
        KeyGroupMessage groupMessage = context.getMessage(MessageType.KEY_GROUP);
        ParameterRequest request = context.getRequest();
        Object accumulator = requestId2Accumulators.computeIfAbsent(request,
            r -> function.createAccumulator());

        for (Row row : groupMessage.getGroupRows()) {
            Object inputAcc = row.getField(0, inputType);
            function.merge(accumulator, inputAcc);
        }
    }

    @Override
    public void finish() {
        for (Map.Entry<ParameterRequest, Object> entry : requestId2Accumulators.entrySet()) {
            ParameterRequest request = entry.getKey();
            Object accumulator = entry.getValue();
            SingleValue value = function.getValue(accumulator);
            context.setRequest(request);
            collect(value);
        }
        requestId2Accumulators.clear();
        super.finish();
    }

    @Override
    public StepOperator<VertexRecord, SingleValue> copyInternal() {
        return new StepGlobalSingleValueAggregateOperator(id, inputType, function);
    }
}
