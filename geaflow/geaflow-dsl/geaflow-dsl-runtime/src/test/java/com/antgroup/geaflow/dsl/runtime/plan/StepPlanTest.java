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

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.calcite.GraphRecordType;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.common.types.VoidType;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepJoinFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepKeyFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlan;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlanSet;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import com.antgroup.geaflow.state.pushdown.filter.EmptyFilter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.calcite.rel.core.JoinRelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StepPlanTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StepPlanTest.class);

    @Test
    public void testLogicalPlan() {
        StepLogicalPlan logicalPlan =
            StepLogicalPlan.start()
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "a", EmptyFilter.of()))
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
                .edgeMatch(new MatchEdgeFunctionImpl(EdgeDirection.OUT, Sets.newHashSet(), "e"))
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "b", EmptyFilter.of()))
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
            ;
        String planDesc = logicalPlan.getPlanDesc();
        LOGGER.info("Logical plan:\n{}", planDesc);
        Assert.assertEquals(planDesc,
            "digraph G {\n"
                + "2 -> 3 [label= \"chain = false\"]\n"
                + "1 -> 2 [label= \"chain = false\"]\n"
                + "0 -> 1 [label= \"chain = false\"]\n"
                + "3 [label= \"MatchVertex-3 [b]\"]\n"
                + "2 [label= \"MatchEdge-2(OUT) [e]\"]\n"
                + "1 [label= \"MatchVertex-1 [a]\"]\n"
                + "0 [label= \"StepSource-0()\"]\n"
                + "}");
    }

    @Test
    public void testMultiOutputLogicalPlan() {
        StepLogicalPlan logicalPlan =
            StepLogicalPlan.start()
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "a"))
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
                .edgeMatch(new MatchEdgeFunctionImpl(EdgeDirection.OUT, Sets.newHashSet(), "e"))
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "b"))
                    .withInputPathSchema(PathType.EMPTY)
                    .withOutputPathSchema(PathType.EMPTY)
                    .withOutputType(VoidType.INSTANCE)
                    .withGraphSchema(createGraph())
            ;

        StepLogicalPlan leftPlan =
            logicalPlan.edgeMatch(new MatchEdgeFunctionImpl(EdgeDirection.OUT, Sets.newHashSet(), "f"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
            .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                "person")), "c"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
        ;

        StepLogicalPlan rightPlan =
            logicalPlan.edgeMatch(new MatchEdgeFunctionImpl(EdgeDirection.OUT, Sets.newHashSet(), "g"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
            .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                "person")), "d"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
        ;
        StepKeyFunction keyFunction = new StepKeyFunctionImpl(new int[]{}, new IType[]{});
        StepLogicalPlan joinPlan = leftPlan.join(rightPlan, keyFunction, keyFunction,
            new StepJoinFunctionImpl(JoinRelType.INNER, new IType[]{}, new IType[]{}),
                PathType.EMPTY, false)
            .withOutputPathSchema(new PathType());

        StepLogicalPlanSet logicalPlanSet = new StepLogicalPlanSet(joinPlan);
        logicalPlanSet.markChainable();
        String planDesc = logicalPlanSet.getPlanSetDesc();
        LOGGER.info("Logical plan:\n{}", planDesc);
        Assert.assertEquals(planDesc,
            "digraph G {\n" + "10 -> 11 [label= \"\"]\n" + "8 -> 10 [label= \"chain = false\"]\n"
                + "5 -> 8 [label= \"\"]\n" + "4 -> 5 [label= \"chain = false\"]\n"
                + "3 -> 4 [label= \"\"]\n" + "2 -> 3 [label= \"chain = false\"]\n"
                + "1 -> 2 [label= \"\"]\n" + "0 -> 1 [label= \"\"]\n"
                + "9 -> 10 [label= \"chain = false\"]\n" + "7 -> 9 [label= \"\"]\n"
                + "6 -> 7 [label= \"chain = false\"]\n" + "3 -> 6 [label= \"\"]\n"
                + "11 [label= \"StepEnd-11\"]\n" + "10 [label= \"StepJoin-10\"]\n"
                + "8 [label= \"StepExchange-8\"]\n" + "5 [label= \"MatchVertex-5 [c]\"]\n"
                + "4 [label= \"MatchEdge-4(OUT) [f]\"]\n" + "3 [label= \"MatchVertex-3 [b]\"]\n"
                + "2 [label= \"MatchEdge-2(OUT) [e]\"]\n" + "1 [label= \"MatchVertex-1 [a]\"]\n"
                + "0 [label= \"StepSource-0()\"]\n" + "9 [label= \"StepExchange-9\"]\n"
                + "7 [label= \"MatchVertex-7 [d]\"]\n" + "6 [label= \"MatchEdge-6(OUT) [g]\"]\n"
                + "}"
        );
    }

    private GraphSchema createGraph() {
        TableField idField = new TableField("id", Types.of("Long"), false);
        VertexTable vTable = new VertexTable("default", "testV", Collections.singletonList(idField),"id");
        GeaFlowGraph graph = new GeaFlowGraph("default", "test", Lists.newArrayList(vTable),
            new ArrayList<>(), new HashMap<>(), new HashMap<>(), false, false);
        GraphRecordType graphRecordType = (GraphRecordType) graph.getRowType(GQLJavaTypeFactory.create());
        return (GraphSchema) SqlTypeUtil.convertType(graphRecordType);
    }

    @BeforeMethod
    public void setup() {
        StepLogicalPlan.clearCounter();
    }
}
