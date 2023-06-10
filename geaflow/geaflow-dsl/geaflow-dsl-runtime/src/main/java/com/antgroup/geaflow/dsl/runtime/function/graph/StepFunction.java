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

package com.antgroup.geaflow.dsl.runtime.function.graph;

import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.function.FunctionContext;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallQueryProxy;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.ICallQuery;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public interface StepFunction extends Serializable {

    void open(TraversalRuntimeContext context, FunctionSchemas schemas);

    void finish(StepCollector<StepRecord> collector);

    List<Expression> getExpressions();

    StepFunction copy(List<Expression> expressions);

    static void openExpression(Expression expression, TraversalRuntimeContext context) {
        if (expression instanceof ICallQuery) {
            ((ICallQuery) expression).open(context);
        } else {
            expression.open(FunctionContext.of(context.getConfig()));
        }
    }

    default List<CallQueryProxy> getCallQueryProxies() {
        return getExpressions().stream()
            .filter(exp -> exp instanceof CallQueryProxy)
            .map(exp -> (CallQueryProxy) exp)
            .collect(Collectors.toList());
    }
}
