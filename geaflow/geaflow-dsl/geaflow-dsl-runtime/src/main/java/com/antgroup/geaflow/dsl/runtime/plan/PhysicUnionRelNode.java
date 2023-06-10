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

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RDataView;
import com.antgroup.geaflow.dsl.runtime.RDataView.ViewType;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * UNION.
 */
public class PhysicUnionRelNode extends Union implements PhysicRelNode<RuntimeTable> {

    public PhysicUnionRelNode(RelOptCluster cluster,
                              RelTraitSet traits,
                              List<RelNode> inputs,
                              boolean all) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new PhysicUnionRelNode(super.getCluster(), traitSet, inputs, all);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        List<RDataView> dataViews = new ArrayList<>();
        for (RelNode input : getInputs()) {
            dataViews.add(((PhysicRelNode<?>) input).translate(context));
            if (dataViews.get(dataViews.size() - 1).getType() != ViewType.TABLE) {
                throw new GeaFlowDSLException("DataView: "
                    + dataViews.get(dataViews.size() - 1).getType() + " cannot support SQL union");
            }
        }
        if (!dataViews.isEmpty()) {
            RuntimeTable output = (RuntimeTable) dataViews.get(0);
            for (int i = 1; i < dataViews.size(); i++) {
                output = output.union((RuntimeTable) dataViews.get(i));
            }
            return output;
        } else {
            throw new GeaFlowDSLException("Union inputs cannot be empty.");
        }
    }

    @Override
    public String showSQL() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getInputs().size(); i++) {
            if (i > 0) {
                sb.append(" ").append(kind.name()).append(" ");
            }
            sb.append(((PhysicRelNode) getInputs().get(i)).showSQL());
        }
        return sb.toString();
    }

}
