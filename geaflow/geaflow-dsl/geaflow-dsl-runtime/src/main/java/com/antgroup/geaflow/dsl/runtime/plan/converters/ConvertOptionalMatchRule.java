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

package com.antgroup.geaflow.dsl.runtime.plan.converters;

import com.antgroup.geaflow.dsl.rel.logical.LogicalOptionalMatch;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicOptionalMatchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a {@link LogicalOptionalMatch} to a
 * {@link PhysicOptionalMatchNode}.
 */
public class ConvertOptionalMatchRule extends ConverterRule {

    public static final ConvertOptionalMatchRule INSTANCE = new ConvertOptionalMatchRule();

    private ConvertOptionalMatchRule() {
        super(LogicalOptionalMatch.class, Convention.NONE,
                PhysicConvention.INSTANCE, ConvertOptionalMatchRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalOptionalMatch optionalMatch = (LogicalOptionalMatch) rel;
        RelTraitSet relTraitSet = optionalMatch.getTraitSet().replace(PhysicConvention.INSTANCE);

        RelNode convertedInput = convert(optionalMatch.getInput(),
                optionalMatch.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        // 在这里，从逻辑节点中获取 isCaseSensitive 的值
        boolean isCaseSensitive = optionalMatch.isCaseSensitive();

        // 将其传递给物理节点的构造函数
        return new PhysicOptionalMatchRelNode(
                optionalMatch.getCluster(),
                relTraitSet,
                convertedInput,
                optionalMatch.getPathPattern(),
                optionalMatch.getRowType(),
                isCaseSensitive // 传递值
        );
    }
}