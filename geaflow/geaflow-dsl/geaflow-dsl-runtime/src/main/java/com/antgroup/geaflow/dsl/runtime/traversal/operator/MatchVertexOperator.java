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
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType;
import com.antgroup.geaflow.dsl.common.data.VirtualId;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Histogram;

public class MatchVertexOperator extends AbstractStepOperator<MatchVertexFunction, StepRecord,
    VertexRecord> implements LabeledStepOperator {

    private Histogram loadVertexRt;

    public MatchVertexOperator(long id, MatchVertexFunction function) {
        super(id, function);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        loadVertexRt = metricGroup.histogram(MetricNameFormatter.loadVertexTimeRtName(getName()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.VERTEX) {
            processVertex((VertexRecord) record);
        } else {
            EdgeGroupRecord edgeGroupRecord = (EdgeGroupRecord) record;
            processEdgeGroup(edgeGroupRecord);
        }
    }

    private void processVertex(VertexRecord vertexRecord) {
        RowVertex vertex = vertexRecord.getVertex();
        if (vertex instanceof IdOnlyVertex && needLoadVertex(vertex.getId())) {
            long startTs = System.currentTimeMillis();
            vertex = context.loadVertex(vertex.getId(),
                function.getVertexFilter(),
                graphSchema,
                addingVertexFieldTypes);
            loadVertexRt.update(System.currentTimeMillis() - startTs);
            if (vertex == null) { // load a non-exists vertex, just skip.
                return;
            }
        }

        if (!function.getVertexTypes().isEmpty()
            && !function.getVertexTypes().contains(vertex.getBinaryLabel())) {
            // filter by the vertex types.
            return;
        }
        vertex = alignToOutputSchema(vertex);

        ITreePath currentPath;
        if (needAddToPath) {
            currentPath = vertexRecord.getTreePath().extendTo(vertex);
        } else {
            currentPath = vertexRecord.getTreePath();
        }
        collect(VertexRecord.of(vertex, currentPath));
    }

    private void processEdgeGroup(EdgeGroupRecord edgeGroupRecord) {
        EdgeGroup edgeGroup = edgeGroupRecord.getEdgeGroup();
        for (RowEdge edge : edgeGroup) {
            Object targetId = edge.getTargetId();
            // load targetId.
            RowVertex vertex = context.loadVertex(targetId, function.getVertexFilter(), graphSchema, addingVertexFieldTypes);
            if (vertex != null) {
                ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                // set current vertex.
                context.setVertex(vertex);
                // process new vertex.
                processVertex(VertexRecord.of(vertex, treePath));
            }
        }
    }

    private boolean needLoadVertex(Object vertexId) {
        // skip load virtual id.
        return !(vertexId instanceof VirtualId);
    }

    @Override
    public void close() {

    }

    @Override
    public StepOperator<StepRecord, VertexRecord> copyInternal() {
        return new MatchVertexOperator(id, function);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
