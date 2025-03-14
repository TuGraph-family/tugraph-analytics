/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.memory.jmh;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.memory.MemoryGroupManger;
import com.antgroup.geaflow.memory.MemoryManager;
import com.antgroup.geaflow.memory.MemoryView;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class AllocationGeaflowBenchmark extends JMHParameter {

    @Setup
    public void setUp() {
        Map<String, String> config = Maps.newHashMap();
        config.put("max.direct.memory.size.mb", "512");
        config.put("off.heap.memory.chunkSize.MB", "16");
        MemoryManager.build(new Configuration(config));
    }

    @Benchmark
    public void allocateAndFree() {
        MemoryView view = MemoryManager.getInstance()
            .requireMemory(allocateBytes, MemoryGroupManger.DEFAULT);
        view.close();
    }

    @TearDown
    public void finish() {
        MemoryManager.getInstance().dispose();
    }
}
