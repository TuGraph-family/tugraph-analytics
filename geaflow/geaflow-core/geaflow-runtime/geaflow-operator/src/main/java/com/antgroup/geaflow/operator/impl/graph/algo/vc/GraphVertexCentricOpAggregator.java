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

package com.antgroup.geaflow.operator.impl.graph.algo.vc;

import com.antgroup.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import com.antgroup.geaflow.api.graph.base.algo.VertexCentricAlgo;
import com.antgroup.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import com.antgroup.geaflow.collector.AbstractCollector;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.metrics.common.MetricConstants;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.BlackHoleMetricGroup;
import com.antgroup.geaflow.metrics.common.api.Meter;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.model.record.RecordArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Do graph aggregate following by the graph vertex centric operator.
 * @param <K> The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M> The message type during iterations.
 * @param <I> The type of aggregate input iterm.
 * @param <PA> The type of partial aggregator.
 * @param <PR> The type of partial aggregate result.
 * @param <GA> The type of global aggregator.
 * @param <GR> The type of global aggregate result.
 * @param <FUNC> The type of algo function in operator.
 */
public class GraphVertexCentricOpAggregator<K, VV, EV, M, I, PA, PR, GA, GR,
    FUNC extends VertexCentricAlgo<K, VV, EV, M> & GraphAggregationAlgo<I, PA, PR, GA, GR>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphVertexCentricOpAggregator.class);

    private AbstractGraphVertexCentricOp<K, VV, EV, M, FUNC> operator;
    private long iteration;

    protected VertexCentricAggregateFunction.IPartialGraphAggFunction<I, PA, PR> partialGraphAggFunction;
    protected PartialAggContextImpl partialAggContextImpl;

    protected PA partialAgg;
    protected PR partialResult;
    protected GR globalResult;

    private ICollector<PR> aggregateCollector;

    public GraphVertexCentricOpAggregator(AbstractGraphVertexCentricOp<K, VV, EV, M, FUNC> operator) {
        this.operator = operator;
    }

    public void open(VertexCentricAggContextFunction<I, GR> aggFunction) {
        // Partial agg function.
        this.partialGraphAggFunction = operator.getFunction().getAggregateFunction().getPartialAggregation();
        this.partialAggContextImpl = new PartialAggContextImpl();
        this.partialAgg = this.partialGraphAggFunction.create(this.partialAggContextImpl);

        VertexCentricAggContextImpl aggContext = new VertexCentricAggContextImpl();
        aggFunction.initContext(aggContext);

        boolean enableDetailMetric = Configuration.getBoolean(ExecutionConfigKeys.ENABLE_DETAIL_METRIC,
            this.operator.getOpArgs().getConfig());
        MetricGroup metricGroup = enableDetailMetric
            ? MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_FRAMEWORK)
            : BlackHoleMetricGroup.INSTANCE;
        Meter aggMeter = metricGroup.meter(
            MetricNameFormatter.iterationAggMetricName(this.getClass(), operator.getOpArgs().getOpId()));

        this.aggregateCollector = operator.collectorMap.get(RecordArgs.GraphRecordNames.Aggregate.name());
        if (this.aggregateCollector instanceof AbstractCollector) {
            ((AbstractCollector) this.aggregateCollector).setOutputMetric(aggMeter);
        }
    }

    public void initIteration(long iteration) {
        this.iteration = iteration;
    }

    public void finishIteration(long iteration) {
        if (partialResult != null) {
            this.partialGraphAggFunction.finish(this.partialResult);
            LOGGER.info("iterationId:{} partial result :{}", iteration, partialResult);
            aggregateCollector.finish();
            this.partialResult = null;
        }
    }

    public void processAggregateResult(GR result) {
        this.globalResult = result;
        this.partialAgg = this.partialGraphAggFunction.create(this.partialAggContextImpl);
    }

    class VertexCentricAggContextImpl implements VertexCentricAggContextFunction.VertexCentricAggContext<I, GR> {

        @Override
        public GR getAggregateResult() {
            return globalResult;
        }

        @Override
        public void aggregate(I i) {
            partialResult = partialGraphAggFunction.aggregate(i, partialAgg);
        }
    }

    class PartialAggContextImpl implements VertexCentricAggregateFunction.IPartialAggContext<PR> {

        @Override
        public long getIteration() {
            return iteration;
        }

        @Override
        public void collect(PR result) {
            aggregateCollector.partition(result);
        }
    }
}
