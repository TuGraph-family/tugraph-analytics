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

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.data.impl.DefaultPath;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.core.JoinRelType;

public class StepJoinFunctionImpl implements StepJoinFunction {

    private final JoinRelType joinType;

    private final IType<?>[] leftTypes;

    private final IType<?>[] rightTypes;

    public StepJoinFunctionImpl(JoinRelType joinType, IType<?>[] leftTypes, IType<?>[] rightTypes) {
        this.joinType = joinType;
        this.leftTypes = Objects.requireNonNull(leftTypes);
        this.rightTypes = Objects.requireNonNull(rightTypes);
    }

    @Override
    public Path join(Path left, Path right) {
        switch (joinType) {
            case INNER:
                if (left == null || right == null) {
                    return null;
                }
                return joinPath(left, right);
            case LEFT:
                if (left == null) {
                    return null;
                }
                if (right == null) {
                    right = new DefaultPath(new Row[rightTypes.length]);
                }
                return joinPath(left, right);
            case RIGHT:
                if (right == null) {
                    return null;
                }
                if (left == null) {
                    left = new DefaultPath(new Row[leftTypes.length]);
                }
                return joinPath(left, right);
            default:
                throw new GeaFlowDSLException("JoinType: " + joinType + " is not support");
        }
    }

    @Override
    public JoinRelType getJoinType() {
        return joinType;
    }

    private Path joinPath(Path left, Path right) {
        Row[] joinPaths = new Row[leftTypes.length + rightTypes.length];
        int i;
        for (i = 0; i < leftTypes.length; i++) {
            joinPaths[i] = left.getField(i, leftTypes[i]);
        }
        for (; i < leftTypes.length + rightTypes.length; i++) {
            int rightIndex = i - leftTypes.length;
            joinPaths[i] = right.getField(rightIndex, rightTypes[rightIndex]);
        }
        return new DefaultPath(joinPaths);
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
        return new StepJoinFunctionImpl(joinType, leftTypes, rightTypes);
    }
}
