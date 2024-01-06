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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepEndCollector;
import com.antgroup.geaflow.dsl.runtime.traversal.message.KVTraversalAgg;
import java.util.Map.Entry;
import java.util.Objects;

public class GeaFlowKVTraversalAggregateFunction implements VertexCentricAggregateFunction
    <KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>,
        KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>,
        KVTraversalAgg<String, Integer>> {

    private final int parallelism;

    public GeaFlowKVTraversalAggregateFunction(int parallelism) {
        assert parallelism > 0 : "GeaFlowKVTraversalAggregateFunction parallelism <= 0";
        this.parallelism = parallelism;
    }

    @Override
    public IPartialGraphAggFunction<KVTraversalAgg<String, Integer>,
        KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>> getPartialAggregation() {
        return new IPartialGraphAggFunction<KVTraversalAgg<String, Integer>,
            KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>>() {

            private IPartialAggContext<KVTraversalAgg<String, Integer>> partialAggContext;

            @Override
            public KVTraversalAgg<String, Integer> create(
                IPartialAggContext<KVTraversalAgg<String, Integer>> partialAggContext) {
                this.partialAggContext = Objects.requireNonNull(partialAggContext);
                return KVTraversalAgg.empty();
            }


            @Override
            public KVTraversalAgg<String, Integer> aggregate(KVTraversalAgg<String, Integer> iterm,
                                                             KVTraversalAgg<String, Integer> result) {
                if (iterm == null) {
                    return result;
                } else if (result == null) {
                    return iterm;
                }
                for (Entry<String, Integer> entryObj : iterm.getMap().entrySet()) {
                    String key = entryObj.getKey();
                    if (result.getMap().containsKey(key)) {
                        result.getMap().put(key, result.getMap().get(key) + entryObj.getValue());
                    } else {
                        result.getMap().put(key, entryObj.getValue());
                    }
                }
                return result;
            }

            @Override
            public void finish(KVTraversalAgg<String, Integer> result) {
                assert partialAggContext != null;
                if (result != null) {
                    KVTraversalAgg<String, Integer> tmpResult = result.copy();
                    partialAggContext.collect(tmpResult);
                }
            }
        };
    }

    @Override
    public IGraphAggregateFunction<KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>,
        KVTraversalAgg<String, Integer>> getGlobalAggregation() {
        return new IGraphAggregateFunction<KVTraversalAgg<String, Integer>,
            KVTraversalAgg<String, Integer>, KVTraversalAgg<String, Integer>>() {

            private IGlobalGraphAggContext<KVTraversalAgg<String, Integer>> globalGraphAggContext;

            @Override
            public KVTraversalAgg<String, Integer> create(
                IGlobalGraphAggContext<KVTraversalAgg<String, Integer>> globalGraphAggContext) {
                this.globalGraphAggContext = Objects.requireNonNull(globalGraphAggContext);
                return KVTraversalAgg.empty();
            }

            @Override
            public KVTraversalAgg<String, Integer> aggregate(KVTraversalAgg<String, Integer> iterm,
                                                             KVTraversalAgg<String, Integer> result) {
                if (iterm == null) {
                    return result;
                } else if (result == null) {
                    return iterm;
                }
                for (Entry<String, Integer> entryObj : iterm.getMap().entrySet()) {
                    String key = entryObj.getKey();
                    if (result.getMap().containsKey(key)) {
                        result.getMap().put(key, result.getMap().get(key) + entryObj.getValue());
                    } else {
                        result.getMap().put(key, entryObj.getValue());
                    }
                }
                return result;
            }

            @Override
            public void finish(KVTraversalAgg<String, Integer> value) {
                assert globalGraphAggContext != null;
                if (value != null && value.getMap().containsKey(StepEndCollector.TRAVERSAL_FINISH)
                    && value.get(StepEndCollector.TRAVERSAL_FINISH) >= parallelism) {
                    globalGraphAggContext.terminate();
                }
            }
        };
    }
}
