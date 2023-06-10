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

package com.antgroup.geaflow.operator.impl.graph.source;


import com.antgroup.geaflow.api.function.io.GraphSourceFunction;
import com.antgroup.geaflow.api.function.io.GraphSourceFunction.GraphSourceContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Meter;
import com.antgroup.geaflow.model.graph.GraphRecord;
import com.antgroup.geaflow.model.graph.GraphRecord.ViewType;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.operator.impl.io.WindowSourceOperator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphSourceOperator<K, VV, EV> extends WindowSourceOperator<GraphRecord<IVertex<K, VV>,
    IEdge<K, EV>>> {

    public static final String EDGE_TAG = "edge";
    public static final String VERTEX_TAG = "vertex";
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphSourceOperator.class);
    private GraphSourceContext<K, VV, EV> sourceCxt;
    private GraphSourceFunction<K, VV, EV> sourceFunction;
    // Metrics.
    private long edgeCnt;
    private long vertexCnt;
    private long filteredVertexCnt;
    private boolean isDedupEnabled;
    private Set<K> vertexIdSet;
    protected Meter vertexTps;
    protected Meter edgeTps;


    public GraphSourceOperator() {
    }

    public GraphSourceOperator(GraphSourceFunction<K, VV, EV> sourceFunction) {
        super();
        this.sourceFunction = sourceFunction;
        this.vertexIdSet = new HashSet<>();
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.vertexTps = this.metricGroup.meter(
            MetricNameFormatter.vertexTpsMetricName(this.getClass(), this.opArgs.getOpId()));
        this.edgeTps = this.metricGroup.meter(
            MetricNameFormatter.edgeTpsMetricName(this.getClass(), this.opArgs.getOpId()));
        this.sourceCxt = new DefaultGraphSourceContext();

    }

    public void emitRecord(long batchId) {
        try {

            this.sourceFunction.fetch(batchId, sourceCxt);
            this.vertexIdSet.clear();
            LOGGER.info("totalVertex: {}, filteredVertex: {}", vertexCnt, filteredVertexCnt);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    private boolean filterVertex(K vertexId) {
        if (vertexIdSet.contains(vertexId)) {
            filteredVertexCnt++;
            return true;
        } else {
            vertexIdSet.add(vertexId);
            return false;
        }
    }

    class DefaultGraphSourceContext implements GraphSourceContext<K, VV, EV> {

        private final List<ICollector> vertexCollectors;
        private final List<ICollector> edgeCollectors;

        public DefaultGraphSourceContext() {
            this.vertexCollectors = new ArrayList<>();
            this.edgeCollectors = new ArrayList<>();
            filterCollectors(collectors, vertexCollectors, edgeCollectors);
        }

        @Override
        public void collectVertex(IVertex<K, VV> vertex) throws Exception {
            if (isDedupEnabled && filterVertex(vertex.getId())) {
                return;
            }
            collect(new GraphRecord<>(vertex));
        }

        @Override
        public void collectEdge(IEdge<K, EV> edge) throws Exception {
            collect(new GraphRecord<>(edge));
        }

        private void filterCollectors(List<ICollector> collectors,
                                      List<ICollector> vertexCollectors,
                                      List<ICollector> edgeCollectors) {
            for (ICollector collector : collectors) {
                int collectorId = collector.getId();
                String outputTag = outputTags.get(collectorId);
                if (VERTEX_TAG.equals(outputTag)) {
                    vertexCollectors.add(collector);
                } else if (EDGE_TAG.equals(outputTag)) {
                    edgeCollectors.add(collector);
                } else {
                    throw new GeaflowRuntimeException("unrecognized tag: " + outputTag);
                }
            }
        }

        @Override
        public boolean collect(GraphRecord<IVertex<K, VV>, IEdge<K, EV>> element) throws Exception {
            if (element.getViewType() == ViewType.vertex) {
                for (ICollector collector : vertexCollectors) {
                    collector.partition(element.getVertex().getId(), element.getVertex());
                    vertexCnt++;
                    vertexTps.mark();
                }
            } else {
                for (ICollector collector : edgeCollectors) {
                    collector.partition(element.getEdge().getSrcId(), element.getEdge());
                    edgeCnt++;
                    edgeTps.mark();
                }
            }
            return true;
        }
    }

}
