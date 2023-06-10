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
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepMapRowFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import java.util.List;

public class StepMapRowOperator extends AbstractStepOperator<StepMapRowFunction, StepRecord, Row> {

    public StepMapRowOperator(long id, StepMapRowFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.ROW) {
            Row result = function.map((Row) record);
            collect(result);
        } else {
            StepRecordWithPath recordWithPath = (StepRecordWithPath) record;
            List<Row> rows = recordWithPath.map(path -> function.map(withParameter(path)), null);
            if (rows != null) {
                for (Row row : rows) {
                    collect(row);
                }
            }
        }
    }

    @Override
    public StepOperator<StepRecord, Row> copyInternal() {
        return new StepMapRowOperator(id, function);
    }
}
