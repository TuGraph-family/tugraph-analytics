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

package com.antgroup.geaflow.dsl.runtime.plan;

import com.antgroup.geaflow.dsl.rel.GraphScan;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;

public class PhysicGraphScanRelNode extends GraphScan implements PhysicRelNode<RuntimeGraph> {

    public PhysicGraphScanRelNode(RelOptCluster cluster,
                                  RelTraitSet traitSet,
                                  RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public RuntimeGraph translate(QueryContext context) {
        GeaFlowGraph graph = table.unwrap(GeaFlowGraph.class);
        RuntimeGraph runtimeGraph = context.getRuntimeGraph(graph.getName());
        if (runtimeGraph == null) {
            context.addReferSourceGraph(graph);
            runtimeGraph = context.getEngineContext().createRuntimeGraph(context, graph);
            context.putRuntimeGraph(graph.getName(), runtimeGraph);
        }
        return runtimeGraph;
    }

    @Override
    public PhysicGraphScanRelNode copy(RelTraitSet traitSet, RelOptTable table) {
        return new PhysicGraphScanRelNode(getCluster(), traitSet, table);
    }

    @Override
    public String showSQL() {
        return table.unwrap(GeaFlowGraph.class).toString();
    }
}
