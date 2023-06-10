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

import com.antgroup.geaflow.dsl.runtime.function.graph.StepNodeFilterFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;

public class StepNodeFilterOperator extends AbstractStepOperator<StepNodeFilterFunction,
    VertexRecord, VertexRecord> {

    public StepNodeFilterOperator(long id, StepNodeFilterFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(VertexRecord record) {
        if (function.filter(record.getVertex())) {
            collect(record);
        }
    }

    @Override
    public StepOperator<VertexRecord, VertexRecord> copyInternal() {
        return new StepNodeFilterOperator(id, function);
    }

}
