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

package com.antgroup.geaflow.dsl.rel.logical;

import com.antgroup.geaflow.dsl.rel.ParameterizedRelNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class LogicalParameterizedRelNode extends ParameterizedRelNode {

    public LogicalParameterizedRelNode(RelOptCluster cluster, RelTraitSet traitSet,
                                       RelNode parameter, RelNode query) {
        super(cluster, traitSet, parameter, query);
    }

    @Override
    public ParameterizedRelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return new LogicalParameterizedRelNode(getCluster(), traitSet, inputs.get(0), inputs.get(1));
    }

    public static LogicalParameterizedRelNode create(RelOptCluster cluster,
                                                     RelTraitSet traitSet,
                                                     RelNode parameter, RelNode query) {
        return new LogicalParameterizedRelNode(cluster, traitSet, parameter, query);
    }
}
