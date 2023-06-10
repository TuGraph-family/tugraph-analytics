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
import com.antgroup.geaflow.dsl.runtime.traversal.data.SingleValue;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepReturnOperator.StepReturnFunction;
import java.util.Collections;
import java.util.List;

public class StepReturnOperator extends AbstractStepOperator<StepReturnFunction, SingleValue, SingleValue> {

    public StepReturnOperator(long id) {
        super(id, StepReturnFunction.INSTANCE);
    }

    @Override
    protected void processRecord(SingleValue record) {
        collect(record);
    }

    @Override
    public StepOperator<SingleValue, SingleValue> copyInternal() {
        return new StepReturnOperator(id);
    }

    static class StepReturnFunction implements StepFunction {

        public static final StepReturnFunction INSTANCE = new StepReturnFunction();

        private StepReturnFunction() {

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
            assert expressions.isEmpty();
            return new StepReturnFunction();
        }
    }
}
