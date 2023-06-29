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
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import java.util.ArrayList;
import java.util.List;

public class StepMapOperator extends AbstractStepOperator<StepMapFunction, StepRecordWithPath, StepRecordWithPath> {

    private List<StepRecordWithPath> cacheRecords;
    private final boolean isGlobal;

    public StepMapOperator(long id, StepMapFunction function, boolean isGlobal) {
        super(id, function);
        this.isGlobal = isGlobal;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        if (isGlobal) {
            cacheRecords = new ArrayList<>();
        }
    }

    @Override
    public void finish() {
        if (isGlobal) {
            for (StepRecordWithPath record : cacheRecords) {
                collect(record);
            }
            cacheRecords.clear();
        }
        super.finish();
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        StepRecordWithPath mapRecord = record.mapPath(path -> function.map(withParameter(path)),
            null);
        if (isGlobal) {
            cacheRecords.add(mapRecord);
        } else {
            collect(mapRecord);
        }
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepMapOperator(id, function, isGlobal);
    }
}
