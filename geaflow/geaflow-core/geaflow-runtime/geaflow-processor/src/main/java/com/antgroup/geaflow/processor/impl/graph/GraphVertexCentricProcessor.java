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

package com.antgroup.geaflow.processor.impl.graph;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.record.BatchRecord;
import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.model.record.RecordArgs.GraphRecordNames;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphAggregateOp;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphTraversalOp;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import com.antgroup.geaflow.operator.impl.iterator.IteratorOperator;
import com.antgroup.geaflow.processor.impl.AbstractWindowProcessor;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphVertexCentricProcessor<U, OP extends IGraphVertexCentricOp & IteratorOperator>
    extends AbstractWindowProcessor<BatchRecord<U>, Void, OP> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphVertexCentricProcessor.class);

    private long iterations;

    public GraphVertexCentricProcessor(OP graphVertexCentricOp) {
        super(graphVertexCentricOp);
    }

    @Override
    public void open(List<ICollector> collectors, RuntimeContext runtimeContext) {
        super.open(collectors, runtimeContext);
    }

    @Override
    public void init(long batchId) {
        this.iterations = batchId;
    }

    @Override
    public Void process(BatchRecord batchRecord) {
        if (batchRecord != null) {
            this.operator.initIteration(iterations);
            if (this.operator.getMaxIterationCount() >= iterations) {
                RecordArgs recordArgs = batchRecord.getRecordArgs();
                GraphRecordNames graphRecordName = GraphRecordNames.valueOf(recordArgs.getName());
                if (graphRecordName == GraphRecordNames.Vertex) {
                    final Iterator<IVertex> vertexIterator = batchRecord.getMessageIterator();
                    while (vertexIterator.hasNext()) {
                        IVertex vertex = vertexIterator.next();
                        this.operator.addVertex(vertex);
                    }
                } else if (graphRecordName == GraphRecordNames.Edge) {
                    final Iterator<IEdge> edgeIterator = batchRecord.getMessageIterator();
                    while (edgeIterator.hasNext()) {
                        this.operator.addEdge(edgeIterator.next());
                    }
                } else if (graphRecordName == GraphRecordNames.Message) {
                    final Iterator<IGraphMessage> graphMessageIterator =
                        batchRecord.getMessageIterator();
                    while (graphMessageIterator.hasNext()) {
                        this.operator.processMessage(graphMessageIterator.next());
                    }
                } else if (graphRecordName == GraphRecordNames.Request) {
                    final Iterator<ITraversalRequest> requestIterator =
                        batchRecord.getMessageIterator();
                    while (requestIterator.hasNext()) {
                        ((IGraphTraversalOp) this.operator).addRequest(requestIterator.next());
                    }
                } else if (graphRecordName == GraphRecordNames.Aggregate) {
                    final Iterator<ITraversalRequest> requestIterator =
                        batchRecord.getMessageIterator();
                    while (requestIterator.hasNext()) {
                        ((IGraphAggregateOp) this.operator).processAggregateResult(requestIterator.next());
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void finish(long batchId) {
        if (batchId > 0) {
            this.operator.finishIteration(iterations);
        } else {
            this.operator.finish();
        }
    }

}
