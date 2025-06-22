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

package com.antgroup.geaflow.dsl.rel.logical;

import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Represents a logical relational expression node for an OPTIONAL MATCH
 * operation.
 * This node in the logical plan explicitly represents an optional graph
 * matching pattern.
 * Its input can be:
 * <ol>
 * <li>A LogicalGraphScan: When OPTIONAL MATCH is the first clause in a query.
 * <li>Another GraphMatch (like LogicalGraphMatch or LogicalOptionalMatch): When
 * it
 * follows other MATCH clauses.
 * </ol>
 * In a subsequent optimization phase, a dedicated optimization rule (e.g.,
 * OptionalMatchToJoinRule) will convert this node into a standard LogicalJoin
 * (of type LEFT OUTER JOIN).
 */
public class LogicalOptionalMatch extends GraphMatch {

    /**
     * Constructs a LogicalOptionalMatch.
     *
     * @param cluster     Cluster environment, managed by Calcite.
     * @param traits      Trait set.
     * @param input       Input relational expression node.
     * @param pathPattern IMatchNode tree representing the optional match path.
     * @param rowType     Output row type.
     */

    private final boolean isCaseSensitive;

    // 2. 修改构造函数，接收 isCaseSensitive 参数
    protected LogicalOptionalMatch(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, IMatchNode pathPattern, RelDataType rowType,
            boolean isCaseSensitive) {
        super(cluster, traits, input, pathPattern, rowType);
        this.isCaseSensitive = isCaseSensitive;
    }

    // 3. 添加一个 getter 方法.
    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    // 4. 修改 copy 方法，将 isCaseSensitive 传递下去.
    @Override
    public LogicalOptionalMatch copy(RelTraitSet traitSet, RelNode input, IMatchNode pathPattern, RelDataType rowType) {
        return new LogicalOptionalMatch(getCluster(), traitSet, input, pathPattern, rowType, this.isCaseSensitive);
    }

    // 5. 修改静态 create 工厂方法，接收 isCaseSensitive 参数.
    public static LogicalOptionalMatch create(RelOptCluster cluster, RelNode input,
            IMatchNode pathPattern, RelDataType rowType,
            boolean isCaseSensitive) {
        RelTraitSet traitSet = cluster.traitSet();
        return new LogicalOptionalMatch(cluster, traitSet, input, pathPattern, rowType, isCaseSensitive);
    }
}
