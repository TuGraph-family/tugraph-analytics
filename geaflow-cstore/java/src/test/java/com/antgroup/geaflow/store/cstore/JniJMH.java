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

package com.antgroup.geaflow.store.cstore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@Fork(1)
@Threads(1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 2, time = 1)
@State(Scope.Benchmark)

public class JniJMH {

    private NativeGraphStore store;
    private byte[] bytes = new byte[10000];

    @Setup
    public void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put("hello", "world");
        config.put("foo", "bar");
        this.store = new NativeGraphStore("cstore", 0, config);
    }

    @Benchmark
    public void addVertex() throws Exception {
        store.addVertex(new VertexContainer(bytes, 0, "foo", bytes));
    }

    @Benchmark
    public void getVertex() throws Exception {
        Iterator<VertexContainer> it = store.scanVertex();
        while (it.hasNext()) {
            it.next();
        }
    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
            // 导入要测试的类
            .include(JniJMH.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }
}
