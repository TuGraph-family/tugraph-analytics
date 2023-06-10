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

import com.antgroup.geaflow.dsl.runtime.function.graph.StepMapFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;

public class StepMapOperator extends AbstractStepOperator<StepMapFunction, StepRecordWithPath, StepRecordWithPath> {

    public StepMapOperator(long id, StepMapFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        StepRecordWithPath mapRecord = record.mapPath(path -> function.map(withParameter(path)), null);
        collect(mapRecord);
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepMapOperator(id, function);
    }
}
