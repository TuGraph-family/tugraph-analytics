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

package com.antgroup.geaflow.state.jmh;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.JOB_MAX_PARALLEL;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.StateFactory;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(1)
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 3, time = 1)
@State(Scope.Benchmark)
public class StaticGraphStateWriteJMH10 extends JMHParameter {

    GraphState<Integer, Integer, Integer> graphState;

    @Benchmark
    public void composeGraph() {
        GraphStateDescriptor<Integer, Integer, Integer> desc = GraphStateDescriptor.build(
            "StaticGraphStateJMH", storeType);
        GraphMetaType tag = new GraphMetaType(Types.INTEGER, ValueVertex.class,
            Integer.class, ValueLabelTimeEdge.class, Integer.class);
        desc.withGraphMeta(new GraphMeta(tag)).withKeyGroup(new KeyGroup(0, 0));
        Map<String, String> config = new HashMap<>();
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "StaticGraphStateJMH");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);
        for (int i = 0; i < vNum; i++) {
            IVertex<Integer, Integer> vertex = new ValueVertex<>(i, i);
            graphState.staticGraph().V().add(vertex);
            for (int j = 1 ; j < outE; j++) {
                IEdge<Integer, Integer> edge = new ValueLabelTimeEdge<>(i, j, i,
                    i % 2 == 0 ? EdgeDirection.IN : EdgeDirection.OUT,
                    Integer.toString(i % 10), i + 10000000);
                graphState.staticGraph().E().add(edge);
            }
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

}
