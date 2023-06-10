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
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepSortFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.message.KeyGroupMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageType;

public class StepGlobalSortOperator extends AbstractStepOperator<StepSortFunction, VertexRecord,
    VertexRecord> {

    private final IType<?> rowVertexType;
    private final IType<?> inputType;

    public StepGlobalSortOperator(long id, StepSortFunction function, IType<?> rowVertexType,
                                  IType<?> inputType) {
        super(id, function);
        this.rowVertexType = rowVertexType;
        this.inputType = inputType;
    }

    @Override
    protected void processRecord(VertexRecord record) {
        KeyGroupMessage groupMessage = context.getMessage(MessageType.KEY_GROUP);
        for (Row row : groupMessage.getGroupRows()) {
            RowVertex head = (RowVertex)row.getField(0, rowVertexType);
            Path path = (Path)row.getField(1, inputType);

            function.process(head, path);
        }
    }

    @Override
    public StepOperator<VertexRecord, VertexRecord> copyInternal() {
        return new StepGlobalSortOperator(id, function, rowVertexType, inputType);
    }
}
