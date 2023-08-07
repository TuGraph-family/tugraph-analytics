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

import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.function.graph.FunctionSchemas;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import com.antgroup.geaflow.dsl.runtime.traversal.data.CallRequestId;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StepSubQueryStartOperator extends AbstractStepOperator<StepFunction, VertexRecord, VertexRecord> {

    private final String queryName;

    public StepSubQueryStartOperator(long id, String queryName) {
        super(id, SubQueryStartFunction.INSTANCE);
        this.queryName = Objects.requireNonNull(queryName);
    }

    @Override
    protected void processRecord(VertexRecord record) {
        Object requestId = context.getRequestId();
        if (requestId instanceof CallRequestId) {
            context.stashCallRequestId((CallRequestId) requestId);
        }
        collect(record);
    }

    @Override
    protected boolean hasReceivedAllEod(List<EndOfData> receiveEods) {
        return receiveEods.size() == numTasks;
    }

    @Override
    public StepOperator<VertexRecord, VertexRecord> copyInternal() {
        return new StepSubQueryStartOperator(id, queryName);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        str.append("(name=").append(queryName).append(")");
        return str.toString();
    }

    public String getQueryName() {
        return queryName;
    }

    private static class SubQueryStartFunction implements StepFunction {

        public static final SubQueryStartFunction INSTANCE = new SubQueryStartFunction();

        @Override
        public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

        }

        @Override
        public void finish(StepCollector<StepRecord> collector) {

        }

        @Override
        public List<Expression> getExpressions() {
            return Collections.emptyList();
        }

        @Override
        public StepFunction copy(List<Expression> expressions) {
            assert expressions.isEmpty();
            return new SubQueryStartFunction();
        }
    }
}
