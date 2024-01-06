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

package com.antgroup.geaflow.runtime.core.scheduler;

import static com.antgroup.geaflow.plan.PipelinePlanBuilder.ITERATION_AGG_VERTEX_ID;

import com.antgroup.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import com.antgroup.geaflow.processor.impl.graph.GraphVertexCentricProcessor;
import com.antgroup.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.io.CycleResultManager;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class SchedulerGraphAggregateProcessor<ITERM, AGG, RESULT> {

    private VertexCentricAggregateFunction.IGraphAggregateFunction<ITERM, AGG, RESULT> function;
    private AGG aggregator;
    private RESULT result;
    private int expectedCount;
    private int processedCount;
    private ExecutionNodeCycle cycle;
    private AbstractCycleSchedulerContext schedulerContext;

    public SchedulerGraphAggregateProcessor(ExecutionNodeCycle cycle,
                                            AbstractCycleSchedulerContext context,
                                            CycleResultManager resultManager) {
        this.cycle = cycle;
        this.schedulerContext = context;
        this.expectedCount = cycle.getCycleTails().size();
        this.processedCount = 0;

        Preconditions.checkArgument(cycle.getVertexGroup().getVertexMap().size() == 2,
            String.format("Vertex group should only contains an iteration vertex "
                    + "and an aggregation vertex, current vertex size is %s",
                cycle.getVertexGroup().getVertexMap().size()));

        ExecutionVertex aggVertex = cycle.getVertexGroup().getVertexMap().get(ITERATION_AGG_VERTEX_ID);
        Preconditions.checkArgument(aggVertex != null, "aggregation vertex id should be 0");

        AbstractGraphVertexCentricOp operator =
            (AbstractGraphVertexCentricOp) ((GraphVertexCentricProcessor) aggVertex.getProcessor()).getOperator();
        ((GraphAggregationAlgo) (operator.getFunction())).getAggregateFunction().getPartialAggregation();
        this.function =
            ((GraphAggregationAlgo) (operator.getFunction())).getAggregateFunction().getGlobalAggregation();

        Optional<Integer> edgeId = cycle.getVertexGroup().getEdgeMap().values().stream()
            .filter(e -> e.getType() == CollectType.RESPONSE && e.getSrcId() == ITERATION_AGG_VERTEX_ID)
            .map(e -> e.getEdgeId()).findFirst();
        Preconditions.checkArgument(edgeId.isPresent(),
            "An edge from aggregation vertex to iteration vertex should build");
        this.aggregator = (AGG) function.create(new GlobalAggregateContext(edgeId.get(), resultManager));
    }

    public void aggregate(List<ITERM> input) {
        for (ITERM iterm : input) {
            result = function.aggregate(iterm, aggregator);
            if (++processedCount == expectedCount) {
                function.finish(result);
                processedCount = 0;
            }
        }
    }

    private class GlobalAggregateContext<RESULT>
        implements VertexCentricAggregateFunction.IGlobalGraphAggContext<RESULT> {

        private int edgeId;
        private CycleResultManager resultManager;

        public GlobalAggregateContext(int edgeId, CycleResultManager resultManager) {
            this.resultManager = resultManager;
            this.edgeId = edgeId;
        }

        @Override
        public long getIteration() {
            return schedulerContext.getCurrentIterationId();
        }

        @Override
        public void broadcast(RESULT result) {
            resultManager.register(edgeId, new ResponseResult(edgeId, CollectType.RESPONSE, Arrays.asList(result)));
        }

        @Override
        public void terminate() {
            schedulerContext.setTerminateIterationId(getIteration());
        }
    }
}
