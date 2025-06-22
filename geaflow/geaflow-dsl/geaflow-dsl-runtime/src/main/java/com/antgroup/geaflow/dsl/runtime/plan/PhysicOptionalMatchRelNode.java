/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.runtime.plan;

// import com.antgroup.geaflow.api.function.base.KeySelector;

// import com.antgroup.geaflow.dsl.common.data.Path;
// import com.antgroup.geaflow.dsl.common.function.FunctionContext;

// import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;

import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;

import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;

// import com.antgroup.geaflow.dsl.sqlnode.SqlMatchPattern;

// import com.antgroup.geaflow.dsl.runtime.engine.GeaFlowRuntimeGraph;

import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
// import com.antgroup.geaflow.dsl.runtime.function.graph.StepFunction;
// import com.antgroup.geaflow.dsl.runtime.function.graph.StepJoinFunction;
// import java.util.Collections;
// import java.util.List;
// import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
// import org.apache.calcite.rel.SingleRel;
// import org.apache.calcite.rel.core.Join;
// import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
// import org.apache.calcite.rex.RexCall;
// import org.apache.calcite.rex.RexInputRef;
// import org.apache.calcite.rex.RexNode;
// import org.apache.calcite.sql.SqlKind;

public class PhysicOptionalMatchRelNode extends GraphMatch implements PhysicRelNode<RuntimeTable> {

    private final boolean isCaseSensitive;

    // private final IMatchNode optionalMatchPattern;

    public PhysicOptionalMatchRelNode(RelOptCluster cluster, RelTraitSet traits, RelNode input,
            IMatchNode pathPattern, RelDataType rowType, boolean isCaseSensitive) {
        super(cluster, traits, input, pathPattern, rowType);
        // this.pathPattern = pathPattern;
        // this.rowType = rowType;
        this.isCaseSensitive = isCaseSensitive;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RuntimeTable translate(QueryContext context) {
        // 先执行子节点的translate
        PhysicRelNode<RuntimeGraph> input = (PhysicRelNode<RuntimeGraph>) getInput();
        RuntimeGraph inputGraph = input.translate(context);

        // 这里打印日志，证明执行到了PhysicOptionalMatchRelNode的translate
        System.out.println("[LOG] PhysicOptionalMatchRelNode.translate() 被调用");

        // 先抛异常，后续你实现真正的OptionalMatch逻辑时替换这里
        throw new UnsupportedOperationException("OPTIONAL MATCH 物理执行逻辑尚未实现");

        // 理论上会调用类似下面的代码：
        // return inputGraph.traversal(this);
    }

    // @Override
    // public RuntimeTable translate(QueryContext context) {
    // System.out.println("\n--- 步骤 2/8: PhysicOptionalMatchRelNode.translate() 已被调用
    // ---\n");
    // RuntimeGraph inputGraph = ((PhysicRelNode<RuntimeGraph>)
    // getInput()).translate(context);
    // // 关键：将 IMatchNode 传递下去
    // // return inputGraph.optionalMatch(this.pathPattern, this.isCaseSensitive);
    // }

    @Override
    public GraphMatch copy(RelTraitSet traitSet, RelNode input, IMatchNode pathPattern, RelDataType rowType) {
        return new PhysicOptionalMatchRelNode(getCluster(), traitSet, input, pathPattern, rowType,
                this.isCaseSensitive);
    }

    @Override
    public String showSQL() {
        // 这里可以拼成对应的Optional Match表达的SQL或Cypher，暂时返回null即可
        return null;
    }
}