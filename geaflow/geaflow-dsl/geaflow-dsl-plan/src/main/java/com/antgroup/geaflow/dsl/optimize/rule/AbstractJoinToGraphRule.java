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

package com.antgroup.geaflow.dsl.optimize.rule;

import com.antgroup.geaflow.dsl.calcite.MetaFieldType;
import com.antgroup.geaflow.dsl.calcite.MetaFieldType.MetaField;
import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.rel.match.EdgeMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.MatchFilter;
import com.antgroup.geaflow.dsl.rel.match.SingleMatchNode;
import com.antgroup.geaflow.dsl.rel.match.VertexMatch;
import com.antgroup.geaflow.dsl.rex.PathInputRef;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

public abstract class AbstractJoinToGraphRule extends RelOptRule {

    public AbstractJoinToGraphRule(RelOptRuleOperand operand) {
        super(operand);
    }

    protected static IMatchNode addRemainFilter(LogicalJoin join, IMatchNode matchNode) {
        JoinInfo joinInfo = join.analyzeCondition();
        RexNode remainFilter = joinInfo.getRemaining(join.getCluster().getRexBuilder());
        if (remainFilter != null && !remainFilter.isAlwaysTrue()) {
            matchNode = MatchFilter.create(matchNode, remainFilter, matchNode.getPathSchema());
        }
        return matchNode;
    }

    protected static GraphJoinType getJoinType(LogicalJoin join) {
        JoinInfo joinInfo = join.analyzeCondition();
        // only support inner join and equal-join currently.
        if (!joinInfo.isEqui() || join.getJoinType() != JoinRelType.INNER) {
            return GraphJoinType.NONE_GRAPH_JOIN;
        }

        List<Integer> leftKeys = new ArrayList<>(joinInfo.leftKeys);
        List<Integer> rightKeys = new ArrayList<>(joinInfo.rightKeys);
        RelDataType leftType = join.getLeft().getRowType();
        RelDataType rightType = join.getRight().getRowType();

        GraphJoinType graphJoinType = GraphJoinType.NONE_GRAPH_JOIN;
        for (int i = 0; i < leftKeys.size(); i++) {
            Integer leftKey = leftKeys.get(i);
            Integer rightKey = rightKeys.get(i);
            GraphJoinType currentJoinType = getJoinType(leftKey, leftType, rightKey, rightType);
            if (currentJoinType != GraphJoinType.NONE_GRAPH_JOIN) {
                if (graphJoinType == GraphJoinType.NONE_GRAPH_JOIN) {
                    graphJoinType = currentJoinType;
                } else if (graphJoinType != currentJoinType) {
                    // contain multi join pattern, can not translate to graph, just return
                    return GraphJoinType.NONE_GRAPH_JOIN;
                }
            }
        }
        return graphJoinType;
    }

    private static GraphJoinType getJoinType(int leftIndex, RelDataType leftType,
                                             int rightIndex, RelDataType rightType) {

        RelDataType leftKeyType = leftType.getFieldList().get(leftIndex).getType();
        RelDataType rightKeyType = rightType.getFieldList().get(rightIndex).getType();

        if (leftKeyType instanceof MetaFieldType
            && rightKeyType instanceof MetaFieldType) {
            MetaField leftMetaField = ((MetaFieldType) leftKeyType).getMetaField();
            MetaField rightMetaField = ((MetaFieldType) rightKeyType).getMetaField();

            switch (leftMetaField) {
                case VERTEX_ID:
                    if (rightMetaField == MetaField.EDGE_SRC_ID) {
                        return GraphJoinType.VERTEX_JOIN_EDGE;
                    } else if (rightMetaField == MetaField.EDGE_TARGET_ID) {
                        return GraphJoinType.EDGE_JOIN_VERTEX;
                    }
                    break;
                case EDGE_SRC_ID:
                    if (rightMetaField == MetaField.VERTEX_ID) {
                        return GraphJoinType.VERTEX_JOIN_EDGE;
                    }
                    break;
                case EDGE_TARGET_ID:
                    if (rightMetaField == MetaField.VERTEX_ID) {
                        return GraphJoinType.EDGE_JOIN_VERTEX;
                    }
                    break;
                default:
            }
        }
        return GraphJoinType.NONE_GRAPH_JOIN;
    }

    protected void processMatchTableJoinToGraphMatch(RelOptRuleCall call,
                                                     LogicalJoin join,
                                                     LogicalGraphMatch graphMatch,
                                                     LogicalProject project,
                                                     LogicalTableScan tableScan) {
        GeaFlowTable geaflowTable = tableScan.getTable().unwrap(GeaFlowTable.class);
        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph currentGraph = typeFactory.getCurrentGraph();
        if (!currentGraph.containTable(geaflowTable)) {
            if (geaflowTable instanceof VertexTable || geaflowTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    geaflowTable.getName());
            }
            return;
        }
        GraphJoinType graphJoinType = getJoinType(join);
        RelDataType tableType = tableScan.getRowType();
        SingleMatchNode matchNode = (SingleMatchNode) graphMatch.getPathPattern();
        IMatchNode newPathPattern;
        boolean isEdgeReverse = false;
        switch (graphJoinType) {
            case EDGE_JOIN_VERTEX:
                if (geaflowTable instanceof VertexTable) {
                    if (!(GQLRelUtil.getLatestMatchNode(matchNode) instanceof EdgeMatch)) {
                        return;
                    }
                    VertexTable vertexTable = (VertexTable) geaflowTable;
                    String nodeName = vertexTable.getName();
                    PathRecordType pathRecordType = matchNode.getPathSchema()
                        .addField(nodeName, tableType, false);
                    newPathPattern = VertexMatch.create(join.getCluster(), matchNode, nodeName,
                        Collections.singletonList(nodeName), tableType, pathRecordType);
                } else if (geaflowTable instanceof EdgeTable) {
                    if (!(GQLRelUtil.getLatestMatchNode(matchNode) instanceof VertexMatch)) {
                        return;
                    }
                    EdgeTable edgeTable = (EdgeTable) geaflowTable;
                    String edgeName = edgeTable.getName();
                    PathRecordType pathRecordType = matchNode.getPathSchema()
                        .addField(edgeName, tableType, false);
                    isEdgeReverse = true;
                    newPathPattern = EdgeMatch.create(join.getCluster(), matchNode, edgeName,
                        Collections.singletonList(edgeName), EdgeDirection.IN, tableType, pathRecordType);
                } else {
                    return;
                }
                break;
            case VERTEX_JOIN_EDGE:
                if (geaflowTable instanceof VertexTable) {
                    if (!(GQLRelUtil.getLatestMatchNode(matchNode) instanceof EdgeMatch)) {
                        return;
                    }
                    VertexTable vertexTable = (VertexTable) geaflowTable;
                    String nodeName = vertexTable.getName();
                    PathRecordType pathRecordType = matchNode.getPathSchema()
                        .addField(nodeName, tableType, false);
                    newPathPattern = VertexMatch.create(join.getCluster(), matchNode, nodeName,
                        Collections.singletonList(nodeName), tableType, pathRecordType);
                } else if (geaflowTable instanceof EdgeTable) {
                    if (!(GQLRelUtil.getLatestMatchNode(matchNode) instanceof VertexMatch)) {
                        return;
                    }
                    EdgeTable edgeTable = (EdgeTable) geaflowTable;
                    String edgeName = edgeTable.getName();
                    PathRecordType pathRecordType = matchNode.getPathSchema()
                        .addField(edgeName, tableType, false);
                    newPathPattern = EdgeMatch.create(join.getCluster(), matchNode, edgeName,
                        Collections.singletonList(edgeName), EdgeDirection.OUT, tableType, pathRecordType);
                } else {
                    return;
                }
                break;
            default:
                return;
        }
        newPathPattern = addRemainFilter(join, newPathPattern);
        GraphMatch newGraphMatch = graphMatch.copy(newPathPattern);
        // add project to graph match
        List<RexNode> newProjects = new ArrayList<>(project.getProjects());

        RelDataTypeField newField = newGraphMatch.getPathPattern().getPathSchema().lastField().get();
        PathInputRef inputRef = new PathInputRef(newField.getName(), newField.getIndex(), newField.getType());
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        for (int i = 0; i < tableType.getFieldCount(); i++) {
            RexNode fieldAccess = rexBuilder.makeFieldAccess(inputRef, i);
            newProjects.add(fieldAccess);
        }

        //In the case of reverse matching in the IN direction, the positions of the source
        // vertex and the destination vertex are swapped.
        if (isEdgeReverse) {
            int edgeSrcIdIndex = tableType.getFieldList().stream().filter(
                f -> f.getType() instanceof MetaFieldType
                    && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_SRC_ID))
                .collect(Collectors.toList()).get(0).getIndex();
            int edgeTargetIdIndex = tableType.getFieldList().stream().filter(
                f -> f.getType() instanceof MetaFieldType
                    && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_TARGET_ID))
                .collect(Collectors.toList()).get(0).getIndex();
            int baseOffset = project.getRowType().getFieldCount();
            Collections.swap(newProjects, baseOffset + edgeSrcIdIndex, baseOffset + edgeTargetIdIndex);
        }
        LogicalProject newProject = LogicalProject.create(newGraphMatch, newProjects, join.getRowType());
        call.transformTo(newProject);
    }

    public enum GraphJoinType {
        /**
         * Vertex join edge src id.
         */
        VERTEX_JOIN_EDGE,
        /**
         * Edge target id join vertex.
         */
        EDGE_JOIN_VERTEX,
        /**
         * None graph match type.
         */
        NONE_GRAPH_JOIN
    }
}
