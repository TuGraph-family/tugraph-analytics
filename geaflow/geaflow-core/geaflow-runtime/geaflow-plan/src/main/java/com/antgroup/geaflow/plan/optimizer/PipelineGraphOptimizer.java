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

package com.antgroup.geaflow.plan.optimizer;

import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.plan.optimizer.strategy.ChainCombiner;
import com.antgroup.geaflow.plan.optimizer.strategy.SingleWindowGroupRule;

import java.io.Serializable;

public class PipelineGraphOptimizer implements Serializable {

    public void optimizePipelineGraph(PipelineGraph pipelineGraph) {
        // Enforce chain combiner opt.
        ChainCombiner chainCombiner = new ChainCombiner();
        chainCombiner.combineVertex(pipelineGraph);

        // Enforce single window rule.
        SingleWindowGroupRule groupRule = new SingleWindowGroupRule();
        groupRule.apply(pipelineGraph);
    }
}
