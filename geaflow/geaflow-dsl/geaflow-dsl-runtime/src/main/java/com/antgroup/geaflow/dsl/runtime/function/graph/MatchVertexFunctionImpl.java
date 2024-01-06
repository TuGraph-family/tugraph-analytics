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

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import com.antgroup.geaflow.state.pushdown.filter.EmptyFilter;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexLabelFilter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class MatchVertexFunctionImpl implements MatchVertexFunction {

    private final Set<BinaryString> vertexTypes;

    private final String label;

    private IFilter vertexFilter;

    private final boolean isOptionalMatchVertex;

    public MatchVertexFunctionImpl(Set<BinaryString> vertexTypes, String label, IFilter<?> vertexFilter) {
        this(vertexTypes, false, label, vertexFilter);
    }

    public MatchVertexFunctionImpl(Set<BinaryString> vertexTypes, boolean isOptionalMatchVertex,
                                   String label, IFilter<?> vertexFilter) {
        this.vertexTypes = vertexTypes;
        this.label = label;
        this.vertexFilter = vertexFilter;
        this.isOptionalMatchVertex = isOptionalMatchVertex;
    }

    public MatchVertexFunctionImpl(Set<BinaryString> vertexTypes, String label,
                                   IFilter ... pushDownFilters) {
        this(vertexTypes, false, label, pushDownFilters);
    }

    public MatchVertexFunctionImpl(Set<BinaryString> vertexTypes, boolean isOptionalMatchVertex,
                                   String label, IFilter ... pushDownFilters) {
        this.vertexTypes = Objects.requireNonNull(vertexTypes);
        this.isOptionalMatchVertex = isOptionalMatchVertex;
        this.label = label;
        if (!vertexTypes.isEmpty()) {
            this.vertexFilter = new VertexLabelFilter(
                vertexTypes.stream().map(BinaryString::toString)
                    .collect(Collectors.toSet()));
        } else {
            this.vertexFilter = EmptyFilter.of();
        }
        for (IFilter filter : pushDownFilters) {
            this.vertexFilter = this.vertexFilter == null ? filter : this.vertexFilter.and(filter);
        }
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Set<BinaryString> getVertexTypes() {
        return vertexTypes;
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    public boolean isOptionalMatchVertex() {
        return isOptionalMatchVertex;
    }

    @Override
    public IFilter getVertexFilter() {
        return vertexFilter;
    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.isEmpty();
        return new MatchVertexFunctionImpl(vertexTypes, label, vertexFilter);
    }
}
