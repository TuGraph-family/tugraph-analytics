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

import com.antgroup.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import com.antgroup.geaflow.dsl.runtime.util.StepFunctionUtil;

public class StepFilterOperator extends AbstractStepOperator<StepBoolFunction, StepRecordWithPath, StepRecordWithPath> {

    private final int[] refPathIndices;

    public StepFilterOperator(long id, StepBoolFunction function) {
        super(id, function);
        this.refPathIndices = StepFunctionUtil.getRefPathIndices(function);
    }

    @Override
    public void processRecord(StepRecordWithPath record) {
        StepRecordWithPath filterRecord = record.filter(path -> function.filter(withParameter(path)), refPathIndices);
        if (!filterRecord.isPathEmpty()) {
            collect(filterRecord);
        }
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepFilterOperator(id, function);
    }
}
