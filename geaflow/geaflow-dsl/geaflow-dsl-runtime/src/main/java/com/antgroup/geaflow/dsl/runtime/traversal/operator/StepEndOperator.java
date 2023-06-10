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
import java.util.Collections;
import java.util.List;

public class StepEndOperator extends AbstractStepOperator<StepFunction, StepRecord, StepRecord> {

    public StepEndOperator(long id) {
        super(id, EndStepFunction.INSTANCE);
    }

    @Override
    protected void processRecord(StepRecord record) {
        collect(record);
    }

    @Override
    public StepOperator<StepRecord, StepRecord> copyInternal() {
        return new StepEndOperator(id);
    }

    private static class EndStepFunction implements StepFunction {

        public static final StepFunction INSTANCE = new EndStepFunction();

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
            return new EndStepFunction();
        }
    }
}
