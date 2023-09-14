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

package com.antgroup.geaflow.operator.impl.graph.compute.dynamic;

import com.antgroup.geaflow.api.function.iterator.RichIteratorFunction;
import com.antgroup.geaflow.api.graph.base.algo.AbstractIncVertexCentricComputeAlgo;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction.IncGraphComputeContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.model.graph.message.DefaultGraphMessage;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.record.RecordArgs.GraphRecordNames;
import com.antgroup.geaflow.operator.OpArgs;
import com.antgroup.geaflow.operator.OpArgs.OpType;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncGraphContextImpl;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox.MsgProcessFunc;
import com.antgroup.geaflow.operator.impl.iterator.IteratorOperator;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicGraphVertexCentricComputeOp<K, VV, EV, M, FUNC extends IncVertexCentricComputeFunction<K, VV, EV, M>>
    extends AbstractDynamicGraphVertexCentricOp<K, VV, EV, M, AbstractIncVertexCentricComputeAlgo<K, VV, EV, M, FUNC>>
    implements IGraphVertexCentricOp<K, VV, EV, M>, IteratorOperator  {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicGraphVertexCentricComputeOp.class);

    protected IncGraphComputeContextImpl graphIncVCComputeCtx;
    protected IncVertexCentricComputeFunction<K, VV, EV, M> incVCComputeFunction;

    private Set<K> invokeVIds;

    private ICollector<IVertex<K, VV>> vertexCollector;

    public DynamicGraphVertexCentricComputeOp(GraphViewDesc graphViewDesc,
                                              AbstractIncVertexCentricComputeAlgo<K, VV, EV, M, FUNC> incVCAlgorithm) {
        super(graphViewDesc, incVCAlgorithm);
        opArgs.setOpType(OpType.INC_VERTEX_CENTRIC_COMPUTE);
        opArgs.setChainStrategy(OpArgs.ChainStrategy.NEVER);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.incVCComputeFunction = this.function.getIncComputeFunction();
        this.graphIncVCComputeCtx = new IncGraphComputeContextImpl();
        this.incVCComputeFunction.init(this.graphIncVCComputeCtx);

        this.invokeVIds = new HashSet<>();

        for (ICollector collector : this.collectors) {
            if (!collector.getTag().equals(GraphRecordNames.Message.name())
                && !collector.getTag().equals(GraphRecordNames.Aggregate.name())) {
                vertexCollector = collector;
            }
        }
    }

    @Override
    public void doFinishIteration(long iterations) {
        LOGGER.info("finish iteration:{}", iterations);
        //compute
        if (this.iterations == 1L) {
            Set<K> vIds = temporaryGraphCache.getAllEvolveVId();
            this.invokeVIds.addAll(vIds);
            for (K vId : vIds) {
                this.graphIncVCComputeCtx.init(iterations, vId);
                this.incVCComputeFunction.evolve(vId,
                    this.graphIncVCComputeCtx.getTemporaryGraph());
            }
        } else {
            this.graphMsgBox.processInMessage(new MsgProcessFunc<K, M>() {
                @Override
                public void process(K vertexId, List<M> ms) {
                    graphIncVCComputeCtx.init(iterations, vertexId);
                    invokeVIds.add(vertexId);
                    incVCComputeFunction.compute(vertexId, ms.iterator());
                }
            });
            this.graphMsgBox.clearInBox();
        }
        if (incVCComputeFunction instanceof RichIteratorFunction) {
            ((RichIteratorFunction) incVCComputeFunction).finishIteration(iterations);
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

        this.messageCollector.finish();
        this.graphMsgBox.clearOutBox();
    }

    @Override
    public void finish() {
        LOGGER.info("current batch invokeIds:{}", this.invokeVIds);
        for (K vertexId : this.invokeVIds) {
            this.graphIncVCComputeCtx.init(iterations, vertexId);
            this.incVCComputeFunction.finish(vertexId, this.graphIncVCComputeCtx.getMutableGraph());
        }
        this.invokeVIds.clear();
        this.temporaryGraphCache.clear();
        vertexCollector.finish();
        checkpoint();
    }

    class IncGraphComputeContextImpl extends IncGraphContextImpl<K, VV, EV, M> implements IncGraphComputeContext<K, VV, EV, M> {

        public IncGraphComputeContextImpl() {
            super(opContext, runtimeContext, graphState, temporaryGraphCache, graphMsgBox, maxIterations);
        }

        @Override
        public void collect(IVertex vertex) {
            vertexCollector.partition(vertex.getId(), vertex);
        }
    }
}
