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
import com.antgroup.geaflow.dsl.common.data.RowKey;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType;
import com.antgroup.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepKeyRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepKeyRecordImpl;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.TreePaths;
import com.google.common.collect.Lists;
import java.util.List;

public class StepLocalExchangeOperator extends AbstractStepOperator<StepKeyFunction, StepRecord, StepRecord> {


    public StepLocalExchangeOperator(long id, StepKeyFunction function) {
        super(id, function);
    }

    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.ROW) {
            Row row = (Row) record;
            RowKey key = function.getKey(row);
            if (context.getRequest() != null) { // append requestId to the key.
                key = new DefaultRowKeyWithRequestId(context.getRequestId(), key);
            }
            StepKeyRecord keyRecord = new StepKeyRecordImpl(key, row);
            collect(keyRecord);
        } else {
            StepRecordWithPath recordWithPath = (StepRecordWithPath) record;
            for (ITreePath treePath : recordWithPath.getPaths()) {
                List<Path> paths = treePath.toList();
                for (Path path : paths) {
                    RowKey key = function.getKey(path);
                    RowVertex virtualVertex = IdOnlyVertex.of(key);
                    ITreePath virtualVertexPath = TreePaths.createTreePath(Lists.newArrayList(path));
                    collect(VertexRecord.of(virtualVertex, virtualVertexPath));
                }
            }
        }
    }

    @Override
    public StepOperator<StepRecord, StepRecord> copyInternal() {
        return new StepLocalExchangeOperator(id, function);
    }
}
