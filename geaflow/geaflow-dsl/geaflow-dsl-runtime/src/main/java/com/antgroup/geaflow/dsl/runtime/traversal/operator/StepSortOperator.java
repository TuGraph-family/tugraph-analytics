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

import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepSortFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;

public class StepSortOperator extends AbstractStepOperator<StepSortFunction, VertexRecord, Row> {

    public StepSortOperator(long id, StepSortFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(VertexRecord record) {
        for (Path path : record.getTreePath().toList()) {
            function.process(record.getVertex(), path);
        }
    }

    @Override
    public StepOperator<VertexRecord, Row> copyInternal() {
        return new StepSortOperator(id, function);
    }
}
