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
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.function.graph.FunctionSchemas;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepUnionOperator.StepUnionFunction;
import com.antgroup.geaflow.dsl.runtime.util.SchemaUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StepUnionOperator extends AbstractStepOperator<StepUnionFunction, StepRecordWithPath, StepRecordWithPath> {

    private final Map<Long, PathType> inputSchemas = new HashMap<>();

    public StepUnionOperator(long id) {
        super(id, StepUnionFunction.INSTANCE);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        List<Long> inputIds = context.getTopology().getInputIds(id);
        for (long inputId : inputIds) {
            PathType pathType = context.getTopology().getOperator(inputId).getOutputPathSchema();
            this.inputSchemas.put(inputId, pathType);
        }
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        final long inputId = context.getInputOperatorId();
        PathType pathType = inputSchemas.get(inputId);
        StepRecordWithPath unionPath = record.mapPath(path ->
            SchemaUtil.alignToPathSchema(path, pathType, outputPathSchema), null);
        collect(unionPath);
    }

    @Override
    protected PathType concatInputPathType() {
        return this.getOutputPathSchema();
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepUnionOperator(id);
    }

    public static class StepUnionFunction implements StepFunction {

        public static final StepUnionFunction INSTANCE = new StepUnionFunction();

        private StepUnionFunction() {

        }

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
            return new StepUnionFunction();
        }
    }
}
