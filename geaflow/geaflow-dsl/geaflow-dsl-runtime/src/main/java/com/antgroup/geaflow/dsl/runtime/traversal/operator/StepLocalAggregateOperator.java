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

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowKey;
import com.antgroup.geaflow.dsl.common.data.impl.DefaultPath;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepAggExpressionFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.TreePaths;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class StepLocalAggregateOperator
    extends AbstractStepOperator<StepAggregateFunction, StepRecordWithPath, StepRecordWithPath> {

    private final StepKeyFunction groupByFunction;
    private Map<ParameterRequest, Map<RowKey, Row>> requestId2Path;
    private Map<ParameterRequest, Map<RowKey, Object>> requestId2Accumulators;
    private final int[] pathPruneIndices;
    private final IType<?>[] pathPruneTypes;
    private final IType<?>[] inputPathTypes;
    boolean isVertexType;
    private final IType<?>[] aggregateNodeTypes;

    public StepLocalAggregateOperator(long id, StepKeyFunction keyFunction, StepAggregateFunction function) {
        super(id, function);
        this.groupByFunction = Objects.requireNonNull(keyFunction);
        this.pathPruneIndices = ((StepAggExpressionFunctionImpl)function).getPathPruneIndices();
        this.pathPruneTypes = ((StepAggExpressionFunctionImpl)function).getPathPruneTypes();
        this.inputPathTypes = ((StepAggExpressionFunctionImpl)function).getInputPathTypes();
        assert pathPruneIndices.length > 0 && pathPruneTypes.length == pathPruneIndices.length;
        this.isVertexType = pathPruneTypes[0] instanceof VertexType;
        this.aggregateNodeTypes = isVertexType
                                  ? ((VertexType)inputPathTypes[pathPruneIndices[0]]).getValueTypes()
                                  : ((EdgeType)inputPathTypes[pathPruneIndices[0]]).getValueTypes();
    }


    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        requestId2Path = new HashMap<>();
        requestId2Accumulators = new HashMap<>();
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        ParameterRequest request = context.getRequest();
        if (!requestId2Accumulators.containsKey(request)) {
            requestId2Path.put(request, new HashMap<>());
            requestId2Accumulators.put(request, new HashMap<>());
        }
        record.mapPath(path -> {
            RowKey key = groupByFunction.getKey(path);
            Map<RowKey, Row> key2Path = requestId2Path.get(request);
            Map<RowKey, Object> key2Acc = requestId2Accumulators.get(request);
            if (!key2Acc.containsKey(key)) {
                key2Acc.put(key, function.createAccumulator());
                key2Path.put(key, path);
            }
            Object accumulator = key2Acc.get(key);
            function.add(path, accumulator);
            key2Acc.put(key, accumulator);
            return path;
        }, null);

    }

    @Override
    public void finish() {
        for (Map.Entry<ParameterRequest, Map<RowKey, Object>> entry : requestId2Accumulators.entrySet()) {
            ParameterRequest request = entry.getKey();
            context.setRequest(request);
            Map<RowKey, Object> key2Acc = entry.getValue();
            Map<RowKey, Row> key2Path = requestId2Path.get(request);
            for (Entry<RowKey, Object> rowKeyObjectEntry : key2Acc.entrySet()) {
                RowKey rowKey = rowKeyObjectEntry.getKey();
                Path path = (Path)key2Path.get(rowKey);
                Row[] values = new Row[inputPathTypes.length];
                for (int i = 0; i < inputPathTypes.length; i++) {
                    values[i] = path.getField(i, inputPathTypes[i]);
                }
                Row aggregateNodeValue;
                int aggregateNodeIndex = pathPruneIndices[0];
                if (isVertexType) {
                    aggregateNodeValue = ((IVertex<Object, Row>)values[aggregateNodeIndex]).getValue();
                } else {
                    aggregateNodeValue = ((IEdge<Object, Row>)values[aggregateNodeIndex]).getValue();
                }
                //The last offset of aggregate node is accumulator
                Object[] aggregateNodeValues =
                    new Object[aggregateNodeTypes.length + 1];
                for (int j = 0; j < aggregateNodeTypes.length; j++) {
                    aggregateNodeValues[j] = aggregateNodeValue.getField(j, aggregateNodeTypes[j]);
                }
                Object accumulator = rowKeyObjectEntry.getValue();
                aggregateNodeValues[aggregateNodeValues.length - 1] = accumulator;
                if (isVertexType) {
                    values[aggregateNodeIndex] = (Row)((IVertex<Object, Row>)values[aggregateNodeIndex])
                        .withValue(ObjectRow.create(aggregateNodeValues));
                } else {
                    values[aggregateNodeIndex] = (Row)((IEdge<Object, Row>)values[aggregateNodeIndex])
                        .withValue(ObjectRow.create(aggregateNodeValues));
                }
                ITreePath localAggPath = TreePaths.singletonPath(new DefaultPath(values));
                collect(VertexRecord.of(IdOnlyVertex.of(rowKey), localAggPath));
            }
        }
        requestId2Accumulators.clear();
        requestId2Path.clear();
        super.finish();
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepLocalAggregateOperator(id, groupByFunction, function);
    }
}
