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

package com.antgroup.geaflow.store.memory;

import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.property.EmptyProperty;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.IDVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.StatePushDown;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.store.api.graph.IGraphStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.PropertyConfigurator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Fork(1)
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class StringMapGraphJMH {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringMapGraphJMH.class);

    IStatePushDown pushdown = StatePushDown.of();
    IGraphStore<String, Object, Object> store;
    StoreContext storeContext = new StoreContext("test").withDataSchema(
        new GraphDataSchema(new GraphMeta(
            new GraphMetaType(StringType.INSTANCE, IDVertex.class, IDVertex::new, EmptyProperty.class,
                IDEdge.class, IDEdge::new, EmptyProperty.class))));

    @Setup
    public void setUp() {
        Properties prop = new Properties();
        prop.setProperty("log4j.rootLogger", "INFO, stdout");
        prop.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        prop.setProperty("log4j.appender.stdout.Target", "System.out");
        prop.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.stdout.layout.ConversionPattern",
            "%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{1}:%L - %m%n");
        PropertyConfigurator.configure(prop);

        store = new GraphMemoryStore<>();
        composeGraph();
    }

    @Benchmark
    public void composeGraph() {
        store.init(storeContext);
        for (int i = 0; i < 1000000; i++) {
            IDVertex<String> vertex = new IDVertex<>(Integer.toString(i));
            store.addVertex(vertex);
            IDEdge<String> edge = new IDEdge<>(Integer.toString(i), Integer.toString(i + 1));
            edge.setDirect(EdgeDirection.IN);
            store.addEdge(edge);
        }
        store.flush();
    }

    @Benchmark
    public void getVertex() {
        for (int i = 0; i < 100000; i++) {
            store.getVertex(Integer.toString(i * 10), pushdown);
        }
    }

    @Benchmark
    public void getEdges() {
        for (int i = 0; i < 100000; i++) {
            store.getEdges(Integer.toString(i * 10), pushdown);
        }
    }

    @Benchmark
    public void getOneGraph() {
        for (int i = 0; i < 100000; i++) {
            store.getOneDegreeGraph(Integer.toString(i * 10), pushdown);
        }
    }

    @Benchmark
    public void getVertexIterator() {
        Iterator<IVertex<String, Object>> it = store.getVertexIterator(pushdown);
        while (it.hasNext()) {
            it.next();
        }
    }

    @Benchmark
    public void getEdgeIterator() {
        Iterator<IEdge<String, Object>> it =
            store.getEdgeIterator(pushdown);
        while (it.hasNext()) {
            it.next();
        }
    }

    @Benchmark
    public void getOneGraphIterator() {
        Iterator<OneDegreeGraph<String, Object, Object>> it =
            store.getOneDegreeGraphIterator(
                pushdown);
        while (it.hasNext()) {
            it.next();
        }
    }

    @Benchmark
    public void memoryUsage() {
        storeContext = null;
        System.gc();
        SleepUtils.sleepSecond(1);
        MemoryMXBean mm = ManagementFactory.getMemoryMXBean();
        LOGGER.info("map(MB): {}", mm.getHeapMemoryUsage().getUsed() / 1024 / 1024);
    }
}
