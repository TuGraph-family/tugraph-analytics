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
import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class StepSourceOperator extends AbstractStepOperator<StepFunction, VertexRecord, StepRecord> {

    private final Set<StartId> startIds;

    public StepSourceOperator(long id, Set<StartId> startIds) {
        super(id, SourceStepFunction.INSTANCE);
        this.startIds = Sets.newHashSet(Objects.requireNonNull(startIds));
    }

    @Override
    protected void processRecord(VertexRecord record) {
        collect(record);
    }

    @Override
    public PathType getOutputPathSchema() {
        return PathType.EMPTY;
    }

    @Override
    protected boolean hasReceivedAllEod(List<EndOfData> receiveEods) {
        // For source operator, the input is empty, so if it has received eod,
        // it will trigger the onReceiveAllEOD.
        return !receiveEods.isEmpty();
    }

    @Override
    public StepOperator<VertexRecord, StepRecord> copyInternal() {
        return new StepSourceOperator(id, Sets.newHashSet(startIds));
    }

    private static class SourceStepFunction implements StepFunction {

        public static final StepFunction INSTANCE = new SourceStepFunction();

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
            return new SourceStepFunction();
        }
    }

    public Set<StartId> getStartIds() {
        return startIds;
    }

    public void addStartIds(Collection<StartId> ids) {
        this.startIds.addAll(ids);
    }

    public void unionStartId(Collection<StartId> ids) {
        if (ids.isEmpty()) {
            // If same branch need traversal all, the startIds should be empty.
            this.startIds.clear();
        } else {
            if (!this.startIds.isEmpty()) {
                this.startIds.addAll(ids);
            }
        }
    }

    public void joinStartId(Collection<StartId> ids) {
        if (ids.isEmpty()) { // empty start id list means traversal all.
            return;
        }
        if (this.startIds.isEmpty()) {
            this.startIds.addAll(ids);
        }

        Set<StartId> intersections = this.startIds.stream()
            .filter(ids::contains)
            .collect(Collectors.toSet());
        this.startIds.clear();
        this.startIds.addAll(intersections);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        String startId = StringUtils.join(startIds, ",");
        str.append("(").append(startId).append(")");
        return str.toString();
    }

    public interface StartId extends Serializable {

    }

    public static class ConstantStartId implements StartId {

        private final Object value;

        public ConstantStartId(Object value) {
            this.value = value;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return Objects.toString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ConstantStartId)) {
                return false;
            }
            ConstantStartId that = (ConstantStartId) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    public static class ParameterStartId implements StartId {

        private final Expression idExpression;

        public ParameterStartId(Expression idExpression) {
            this.idExpression = idExpression;
        }


        public Expression getIdExpression() {
            return idExpression;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParameterStartId)) {
                return false;
            }
            ParameterStartId that = (ParameterStartId) o;
            return Objects.equals(idExpression, that.idExpression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(idExpression);
        }

        @Override
        public String toString() {
            return "ParameterStartId{"
                + "startIdExpression=" + idExpression.showExpression()
                + '}';
        }
    }
}
