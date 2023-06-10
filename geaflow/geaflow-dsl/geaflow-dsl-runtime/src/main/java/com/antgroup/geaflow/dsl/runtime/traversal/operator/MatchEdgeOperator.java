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

import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Histogram;
import java.util.HashMap;
import java.util.Map;

public class MatchEdgeOperator extends AbstractStepOperator<MatchEdgeFunction, VertexRecord, EdgeGroupRecord>
    implements LabeledStepOperator {

    private Histogram loadEdgeHg;
    private Histogram loadEdgeRt;

    public MatchEdgeOperator(long id, MatchEdgeFunction function) {
        super(id, function);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        this.loadEdgeHg = metricGroup.histogram(MetricNameFormatter.loadEdgeCountRtName(getName()));
        this.loadEdgeRt = metricGroup.histogram(MetricNameFormatter.loadEdgeTimeRtName(getName()));
    }

    @Override
    public void processRecord(VertexRecord vertex) {
        long startTs = System.currentTimeMillis();
        EdgeGroup loadEdges = context.loadEdges(function.getEdgesFilter());
        loadEdgeRt.update(System.currentTimeMillis() - startTs);
        loadEdges = loadEdges.map(this::alignToOutputSchema);
        // filter by edge types if exists.
        EdgeGroup edgeGroup = loadEdges;
        if (!function.getEdgeTypes().isEmpty()) {
            edgeGroup = loadEdges.filter(edge ->
                function.getEdgeTypes().contains(edge.getBinaryLabel()));
        }
        Map<Object, ITreePath> targetTreePaths = new HashMap<>();
        // generate new paths.
        if (needAddToPath) {
            int numEdge = 0;
            for (RowEdge edge : edgeGroup) {
                // add edge to path.
                if (!targetTreePaths.containsKey(edge.getTargetId())) {
                    ITreePath newPath = vertex.getTreePath().extendTo(edge);
                    targetTreePaths.put(edge.getTargetId(), newPath);
                } else {
                    ITreePath treePath = targetTreePaths.get(edge.getTargetId());
                    treePath.getEdgeSet().addEdge(edge);
                }
                numEdge++;
            }
            loadEdgeHg.update(numEdge);
        } else {
            if (!vertex.isPathEmpty()) { // inherit input path.
                int numEdge = 0;
                for (RowEdge edge : edgeGroup) {
                    targetTreePaths.put(edge.getTargetId(), vertex.getTreePath());
                    numEdge++;
                }
                loadEdgeHg.update(numEdge);
            }
        }
        EdgeGroupRecord edgeGroupRecord = EdgeGroupRecord.of(edgeGroup, targetTreePaths);
        collect(edgeGroupRecord);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public StepOperator<VertexRecord, EdgeGroupRecord> copyInternal() {
        return new MatchEdgeOperator(id, function);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        EdgeDirection direction = getFunction().getDirection();
        str.append("(").append(direction).append(")");
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
