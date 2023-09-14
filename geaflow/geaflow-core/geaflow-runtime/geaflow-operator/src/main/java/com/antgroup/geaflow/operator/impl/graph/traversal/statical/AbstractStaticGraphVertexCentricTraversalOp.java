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

package com.antgroup.geaflow.operator.impl.graph.traversal.statical;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.iterator.RichIteratorFunction;
import com.antgroup.geaflow.api.graph.base.algo.AbstractVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.VertexCentricTraversalFuncContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.model.graph.message.DefaultGraphMessage;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import com.antgroup.geaflow.model.record.RecordArgs.GraphRecordNames;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.operator.OpArgs.OpType;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphTraversalOp;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.DynamicTraversalEdgeQueryImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.DynamicTraversalVertexQueryImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.statical.StaticGraphContextImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.statical.StaticTraversalEdgeQueryImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.statical.StaticTraversalVertexQueryImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox.MsgProcessFunc;
import com.antgroup.geaflow.operator.impl.graph.compute.statical.AbstractStaticGraphVertexCentricOp;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.view.graph.GraphSnapshotDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStaticGraphVertexCentricTraversalOp<K, VV, EV, M, R,
    FUNC extends VertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractStaticGraphVertexCentricOp<K, VV, EV, M, AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC>>
    implements IGraphTraversalOp<K, VV, EV, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractStaticGraphVertexCentricTraversalOp.class);

    protected GraphVCTraversalCtxImpl graphVCTraversalCtx;
    protected VertexCentricTraversalFunction<K, VV, EV, M, R> vcTraversalFunction;

    protected List<ITraversalResponse<R>> responses;

    protected ICollector<ITraversalResponse<R>> responseCollector;

    protected final List<ITraversalRequest<K>> traversalRequests;

    public AbstractStaticGraphVertexCentricTraversalOp(GraphViewDesc graphViewDesc,
                                                       AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
        opArgs.setOpType(OpType.VERTEX_CENTRIC_TRAVERSAL);
        this.traversalRequests = new ArrayList<>();
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.vcTraversalFunction = this.function.getTraversalFunction();
        this.graphVCTraversalCtx = new GraphVCTraversalCtxImpl(
            opContext, this.runtimeContext, this.graphState,
            this.graphMsgBox, this.maxIterations, this.messageCollector);
        this.vcTraversalFunction.open(this.graphVCTraversalCtx);

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
            traversalByRequest(iterations);
        } else {
            this.graphMsgBox.processInMessage(new MsgProcessFunc<K, M>() {
                @Override
                public void process(K vertexId, List<M> messages) {
                    graphVCTraversalCtx.init(iterations, vertexId);
                    vcTraversalFunction.compute(vertexId, messages.iterator());
                }
            });
            this.graphMsgBox.clearInBox();
        }
        if (vcTraversalFunction instanceof RichIteratorFunction) {
            ((RichIteratorFunction) vcTraversalFunction).finishIteration(iterations);
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

    protected void traversalByRequest(long iterations) {
        Iterator<ITraversalRequest<K>> iterator = getTraversalRequests();
        while (iterator.hasNext()) {
            ITraversalRequest<K> traversalRequest = iterator.next();
            K vertexId = traversalRequest.getVId();
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.vcTraversalFunction.init(traversalRequest);
        }
    }

    @Override
    public void finish() {
        vcTraversalFunction.finish();
        for (ITraversalResponse<R> response : this.responses) {
            responseCollector.partition(response.getResponseId(), response);
        }
        responseCollector.finish();
        traversalRequests.clear();
        responses.clear();
    }

    @Override
    public void close() {
        this.vcTraversalFunction.close();
        super.close();
        this.responses.clear();
    }

    class GraphVCTraversalCtxImpl extends StaticGraphContextImpl<K, VV, EV, M>
        implements VertexCentricTraversalFuncContext<K, VV, EV, M, R> {

        private final ICollector<IGraphMessage<K, M>> messageCollector;

        public GraphVCTraversalCtxImpl(OpContext opContext,
                                       RuntimeContext runtimeContext,
                                       GraphState<K, VV, EV> graphState,
                                       IGraphMsgBox<K, M> graphMsgBox,
                                       long maxIteration,
                                       ICollector<IGraphMessage<K, M>> messageCollector) {
            super(opContext, runtimeContext, graphState, graphMsgBox, maxIteration);
            this.messageCollector = messageCollector;
        }

        @Override
        public void takeResponse(ITraversalResponse response) {
            responses.add(response);
        }

        @Override
        public TraversalVertexQuery<K, VV> vertex() {
            if (graphViewDesc instanceof GraphSnapshotDesc) {
                return new DynamicTraversalVertexQueryImpl<>(vertexId, 0L, graphState);
            }
            return new StaticTraversalVertexQueryImpl<>(vertexId, graphState);
        }

        @Override
        public TraversalEdgeQuery<K, EV> edges() {
            if (graphViewDesc instanceof GraphSnapshotDesc) {
                return new DynamicTraversalEdgeQueryImpl<>(vertexId, 0L, graphState);
            }
            return new StaticTraversalEdgeQueryImpl<>(vertexId, graphState);
        }

        @Override
        public void broadcast(IGraphMessage<K, M> message) {
            messageCollector.broadcast(message);
        }
    }

    public void addRequest(ITraversalRequest<K> request) {
        traversalRequests.add(request);
    }

    public Iterator<ITraversalRequest<K>> getTraversalRequests() {
        return traversalRequests.iterator();
    }
}
