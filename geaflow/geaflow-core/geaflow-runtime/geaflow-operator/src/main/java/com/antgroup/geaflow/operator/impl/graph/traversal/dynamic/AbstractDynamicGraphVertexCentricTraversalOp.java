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

package com.antgroup.geaflow.operator.impl.graph.traversal.dynamic;

import com.antgroup.geaflow.api.function.iterator.RichIteratorFunction;
import com.antgroup.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalHistoricalGraph;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.model.graph.message.DefaultGraphMessage;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import com.antgroup.geaflow.model.record.RecordArgs.GraphRecordNames;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.operator.OpArgs.OpType;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphTraversalOp;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncGraphContextImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncHistoricalGraph;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.TraversalIncHistoricalGraph;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox.MsgProcessFunc;
import com.antgroup.geaflow.operator.impl.graph.compute.dynamic.AbstractDynamicGraphVertexCentricOp;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDynamicGraphVertexCentricTraversalOp<K, VV, EV, M, R,
    FUNC extends IncVertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractDynamicGraphVertexCentricOp<K, VV, EV, M,
    AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC>>
    implements IGraphTraversalOp<K, VV, EV, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractDynamicGraphVertexCentricTraversalOp.class);

    protected IncGraphVCTraversalCtxImpl graphVCTraversalCtx;
    protected IncVertexCentricTraversalFunction<K, VV, EV, M, R> incVcTraversalFunction;

    protected Set<K> invokeVIds;
    protected List<ITraversalResponse<R>> responses;

    protected ICollector<ITraversalResponse<R>> responseCollector;

    protected final List<ITraversalRequest<K>> traversalRequests;

    public AbstractDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> incVertexCentricTraversal) {
        super(graphViewDesc, incVertexCentricTraversal);
        opArgs.setOpType(OpType.INC_VERTEX_CENTRIC_TRAVERSAL);
        this.traversalRequests = new ArrayList<>();
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);

        this.incVcTraversalFunction = this.function.getIncTraversalFunction();
        this.graphVCTraversalCtx = new IncGraphVCTraversalCtxImpl(messageCollector);
        this.incVcTraversalFunction.open(this.graphVCTraversalCtx);

        this.invokeVIds = new HashSet<>();
        this.responses = new ArrayList<>();

        for (ICollector collector : this.collectors) {
            if (!collector.getTag().equals(GraphRecordNames.Message.name())
                && !collector.getTag().equals(GraphRecordNames.Aggregate.name())) {
                responseCollector = collector;
            }
        }
    }

    @Override
    public void doFinishIteration(long iterations) {
        // Compute.
        if (iterations == 1L) {
            // Evolve.
            Set<K> vIds = temporaryGraphCache.getAllEvolveVId();
            this.invokeVIds.addAll(vIds);
            for (K vId : vIds) {
                this.graphVCTraversalCtx.init(iterations, vId);
                this.incVcTraversalFunction.evolve(vId, this.graphVCTraversalCtx.getTemporaryGraph());
            }
            traversalByRequest();
        } else {
            this.graphMsgBox.processInMessage(new MsgProcessFunc<K, M>() {
                @Override
                public void process(K vertexId, List<M> messages) {
                    graphVCTraversalCtx.init(iterations, vertexId);
                    incVcTraversalFunction.compute(vertexId, messages.iterator());
                }
            });
            this.graphMsgBox.clearInBox();
        }
        if (this.incVcTraversalFunction instanceof RichIteratorFunction) {
            ((RichIteratorFunction) this.incVcTraversalFunction).finishIteration(iterations);
        }
        // Emit message.
        this.graphMsgBox.processOutMessage(new MsgProcessFunc<K, M>() {
            @Override
            public void process(K vertexId, List<M> messages) {
                // Collect message.
                int size = messages.size();
                for (int i = 0; i < size; i++) {
                    messageCollector.partition(vertexId, new DefaultGraphMessage<>(vertexId, messages.get(i)));
                }
            }
        });
        messageCollector.finish();
        this.graphMsgBox.clearOutBox();
    }

    protected void traversalByRequest() {
        Iterator<ITraversalRequest<K>> iterator = getTraversalRequests();
        while (iterator.hasNext()) {
            ITraversalRequest<K> traversalRequest = iterator.next();
            K vertexId = traversalRequest.getVId();
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.incVcTraversalFunction.init(traversalRequest);
        }
    }

    @Override
    public void finish() {
        LOGGER.info("current batch invokeIds size:{}", this.invokeVIds.size());
        for (K vertexId : this.invokeVIds) {
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.incVcTraversalFunction.finish(vertexId, this.graphVCTraversalCtx.getMutableGraph());
        }
        this.temporaryGraphCache.clear();
        this.invokeVIds.clear();
        this.traversalRequests.clear();
        this.temporaryGraphCache.clear();

        for (ITraversalResponse<R> response : this.responses) {
            responseCollector.partition(response.getResponseId(), response);
        }
        responseCollector.finish();
        responses.clear();
        checkpoint();
    }

    @Override
    public void close() {
        super.close();
        this.responses.clear();
    }

    class IncGraphVCTraversalCtxImpl extends IncGraphContextImpl<K, VV, EV, M>
        implements IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> {

        private final ICollector<IGraphMessage<K, M>> messageCollector;

        private final TraversalHistoricalGraph<K, VV, EV> traversalHistoricalGraph;

        protected IncGraphVCTraversalCtxImpl(ICollector<IGraphMessage<K, M>> messageCollector) {
            super(opContext, runtimeContext, graphState, temporaryGraphCache, graphMsgBox, maxIterations);
            this.messageCollector = messageCollector;
            this.traversalHistoricalGraph = new TraversalIncHistoricalGraph<>(
                (IncHistoricalGraph<K, VV, EV>) super.getHistoricalGraph());
        }

        @Override
        public void activeRequest(ITraversalRequest<K> request) {

        }

        @Override
        public void takeResponse(ITraversalResponse<R> response) {
            responses.add(response);
        }

        @Override
        public void broadcast(IGraphMessage<K, M> message) {
            messageCollector.broadcast(message);
        }

        @Override
        public TraversalHistoricalGraph<K, VV, EV> getHistoricalGraph() {
            return traversalHistoricalGraph;
        }
    }

    public void addRequest(ITraversalRequest<K> request) {
        LOGGER.info("add request:{}", request);
        traversalRequests.add(request);
    }

    public Iterator<ITraversalRequest<K>> getTraversalRequests() {
        return traversalRequests.iterator();
    }
}
