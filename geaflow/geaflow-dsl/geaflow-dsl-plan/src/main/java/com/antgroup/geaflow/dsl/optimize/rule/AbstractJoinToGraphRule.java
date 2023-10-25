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

import com.antgroup.geaflow.dsl.calcite.EdgeRecordType;
import com.antgroup.geaflow.dsl.calcite.GraphRecordType;
import com.antgroup.geaflow.dsl.calcite.MetaFieldType;
import com.antgroup.geaflow.dsl.calcite.MetaFieldType.MetaField;
import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.calcite.VertexRecordType;
import com.antgroup.geaflow.dsl.common.descriptor.EdgeDescriptor;
import com.antgroup.geaflow.dsl.common.descriptor.GraphDescriptor;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.PathModify.PathModifyExpression;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.rel.match.EdgeMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.MatchAggregate;
import com.antgroup.geaflow.dsl.rel.match.MatchExtend;
import com.antgroup.geaflow.dsl.rel.match.MatchFilter;
import com.antgroup.geaflow.dsl.rel.match.MatchJoin;
import com.antgroup.geaflow.dsl.rel.match.SingleMatchNode;
import com.antgroup.geaflow.dsl.rel.match.VertexMatch;
import com.antgroup.geaflow.dsl.rex.MatchAggregateCall;
import com.antgroup.geaflow.dsl.rex.PathInputRef;
import com.antgroup.geaflow.dsl.rex.RexObjectConstruct;
import com.antgroup.geaflow.dsl.rex.RexObjectConstruct.VariableInfo;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import com.antgroup.geaflow.dsl.util.GQLRexUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public abstract class AbstractJoinToGraphRule extends RelOptRule {

    public AbstractJoinToGraphRule(RelOptRuleOperand operand) {
        super(operand);
    }

    /**
     * Determine if a Join in SQL can be converted to node matching or edge matching in GQL.
     */
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

    private static GraphJoinType getJoinType(int leftIndex, RelDataType leftType, int rightIndex,
                                             RelDataType rightType) {

        RelDataType leftKeyType = leftType.getFieldList().get(leftIndex).getType();
        RelDataType rightKeyType = rightType.getFieldList().get(rightIndex).getType();

        if (leftKeyType instanceof MetaFieldType && rightKeyType instanceof MetaFieldType) {
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

    /**
     * Determine if an SQL logical RelNode belongs to a single-chain and can be rewritten as a GQL RelNode.
     */
    private static boolean isSingleChain(RelNode relNode) {
        return relNode instanceof LogicalFilter || relNode instanceof LogicalProject
            || relNode instanceof LogicalAggregate;
    }

    /**
     * Determine if an SQL logical RelNode is downstream of a TableScan and if all the RelNodes
     * on its input chain can be rewritten as GQL.
     */
    protected static boolean isSingleChainFromLogicalTableScan(RelNode node) {
        RelNode relNode = GQLRelUtil.toRel(node);
        if (isSingleChain(relNode)) {
            return relNode.getInputs().size() == 1 && isSingleChainFromLogicalTableScan(
                relNode.getInput(0));
        }
        return relNode instanceof LogicalTableScan;
    }

    /**
     * Determine if an SQL logical RelNode is downstream of a GraphMatch and if all the RelNodes
     * on its input chain can be rewritten as GQL.
     */
    protected static boolean isSingleChainFromGraphMatch(RelNode node) {
        RelNode relNode = GQLRelUtil.toRel(node);
        if (isSingleChain(relNode)) {
            return relNode.getInputs().size() == 1 && isSingleChainFromGraphMatch(
                relNode.getInput(0));
        }
        return relNode instanceof GraphMatch;
    }

    /**
     * Convert the RelNodes on the single chain from "from" to "to" into GQL and push them
     * into the input. Rebuild a MatchNode, and sequentially place all the accessible fields
     * into the returned rexNodeMap.
     */
    protected IMatchNode concatToMatchNode(RelBuilder builder, RelNode from, RelNode to,
                                           IMatchNode input, List<RexNode> rexNodeMap) {
        if (from instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) from;
            List<RexNode> inputRexNode2RexInfo = new ArrayList<>();
            IMatchNode filterInput = from == to ? input : concatToMatchNode(builder,
                GQLRelUtil.toRel(filter.getInput()), to, input, inputRexNode2RexInfo);
            int lastNodeIndex = filterInput.getPathSchema().getFieldCount() - 1;
            if (lastNodeIndex < 0) {
                throw new GeaFlowDSLException("Need at least 1 node in the path to rewrite.");
            }
            RexNode newCondition = filter.getCondition();
            if (!inputRexNode2RexInfo.isEmpty()) {
                newCondition = GQLRexUtil.replace(filter.getCondition(), rexNode -> {
                    if (rexNode instanceof RexInputRef) {
                        return inputRexNode2RexInfo.get(((RexInputRef) rexNode).getIndex());
                    }
                    return rexNode;
                });
                rexNodeMap.addAll(inputRexNode2RexInfo);
            } else {
                String lastNodeLabel = filterInput.getPathSchema().getFieldList().get(lastNodeIndex)
                    .getName();
                RelDataType oldType = filterInput.getPathSchema().getFieldList().get(lastNodeIndex)
                    .getType();
                newCondition = GQLRexUtil.replace(newCondition, rex -> {
                    if (rex instanceof RexInputRef) {
                        return builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(lastNodeLabel, lastNodeIndex, oldType),
                            ((RexInputRef) rex).getIndex());
                    }
                    return rex;
                });
                rexNodeMap.addAll(inputRexNode2RexInfo);
            }
            return MatchFilter.create(filterInput, newCondition, filterInput.getPathSchema());
        } else if (from instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) from;
            List<RexNode> inputRexNode2RexInfo = new ArrayList<>();
            IMatchNode projectInput = from == to ? input : concatToMatchNode(builder,
                GQLRelUtil.toRel(project.getInput()), to, input, inputRexNode2RexInfo);

            int lastNodeIndex = projectInput.getPathSchema().getFieldCount() - 1;
            if (lastNodeIndex < 0) {
                throw new GeaFlowDSLException("Need at least 1 node in the path to rewrite.");
            }
            String lastNodeLabel = projectInput.getPathSchema().getFieldList().get(lastNodeIndex)
                .getName();
            RelDataType oldType = projectInput.getPathSchema().getFieldList().get(lastNodeIndex)
                .getType();
            List<RexNode> replacedProjects = new ArrayList<>();
            //Rewrite the projects by the rex mapping table returned through input reconstructing.
            if (!inputRexNode2RexInfo.isEmpty()) {
                replacedProjects.addAll(project.getProjects().stream().map(prj -> GQLRexUtil.replace(prj, rexNode -> {
                    if (rexNode instanceof RexInputRef) {
                        return inputRexNode2RexInfo.get(((RexInputRef) rexNode).getIndex());
                    }
                    return rexNode;
                })).collect(Collectors.toList()));
            } else {
                replacedProjects.addAll(project.getProjects().stream().map(prj -> GQLRexUtil.replace(prj, rex -> {
                    if (rex instanceof RexInputRef) {
                        return builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(lastNodeLabel, lastNodeIndex, oldType),
                            ((RexInputRef) rex).getIndex());
                    }
                    return rex;
                })).collect(Collectors.toList()));
            }
            rexNodeMap.addAll(replacedProjects);

            List<RexNode> metaFieldProjects = replacedProjects.stream()
                .filter(rex -> rex instanceof RexFieldAccess).collect(Collectors.toList());
            List<RexNode> addFieldProjects = replacedProjects.stream()
                .filter(rex -> !metaFieldProjects.contains(rex)).collect(Collectors.toList());
            List<Integer> addFieldIndices = addFieldProjects.stream().map(replacedProjects::indexOf)
                .collect(Collectors.toList());

            EdgeRecordType edgeNewType;
            String newEdgeName;
            VertexRecordType vertexNewType;
            String newVertexName;
            PathRecordType newPathRecordType = ((PathRecordType) projectInput.getRowType());
            int extendNodeIndex;
            String extendNodeLabel;
            List<String> addFieldNames;
            List<RexNode> operands = new ArrayList<>();
            Map<RexNode, VariableInfo> rex2VariableInfo = new HashMap<>();
            RexBuilder rexBuilder = builder.getRexBuilder();
            if (addFieldProjects.size() > 0) {
                if (oldType instanceof VertexRecordType) {
                    //Paths ending with a vertex, add an edge and a vertex extension.
                    edgeNewType = EdgeRecordType.emptyEdgeType(
                        ((VertexRecordType) oldType).getIdField().getType(),
                        builder.getTypeFactory());
                    addFieldNames = getNewFieldNames(addFieldProjects.size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    for (int i = 0; i < addFieldNames.size(); i++) {
                        edgeNewType = edgeNewType.add(addFieldNames.get(i),
                            addFieldProjects.get(i).getType(), true);
                    }
                    vertexNewType = (VertexRecordType) oldType;
                    newEdgeName = getNewFieldNames(1,
                        new HashSet<>(newPathRecordType.getFieldNames())).get(0);
                    newPathRecordType = newPathRecordType.addField(newEdgeName, edgeNewType, true);
                    newVertexName = getNewFieldNames(1,
                        new HashSet<>(newPathRecordType.getFieldNames())).get(0);
                    newPathRecordType = newPathRecordType.addField(newVertexName, vertexNewType,
                        true);
                    extendNodeIndex = newPathRecordType.getField(newEdgeName, true, false)
                        .getIndex();
                    extendNodeLabel = newPathRecordType.getFieldList().get(extendNodeIndex)
                        .getName();

                    PathInputRef refPathInput = new PathInputRef(lastNodeLabel, lastNodeIndex,
                        oldType);
                    RelDataType idType = ((MetaFieldType) ((VertexRecordType) oldType).getIdField()
                        .getType()).getType();
                    RexNode srcIdRefRex = builder.getRexBuilder()
                        .makeCast(MetaFieldType.edgeSrcId(idType, builder.getTypeFactory()),
                            builder.getRexBuilder()
                                .makeFieldAccess(refPathInput, VertexType.ID_FIELD_POSITION));
                    operands.add(srcIdRefRex);
                    rex2VariableInfo.put(srcIdRefRex,
                        new VariableInfo(false, edgeNewType.getSrcIdField().getName()));
                    RexNode targetIdRefRex = builder.getRexBuilder()
                        .makeCast(MetaFieldType.edgeTargetId(idType, builder.getTypeFactory()),
                            builder.getRexBuilder().makeCast(idType, builder.getRexBuilder()
                                .makeFieldAccess(refPathInput, VertexType.ID_FIELD_POSITION)));
                    operands.add(targetIdRefRex);
                    rex2VariableInfo.put(targetIdRefRex,
                        new VariableInfo(false, edgeNewType.getTargetIdField().getName()));

                    RelDataType labelType =
                        ((MetaFieldType) ((VertexRecordType) oldType).getLabelField()
                        .getType()).getType();
                    RexNode labelRefRex = builder.getRexBuilder()
                        .makeCast(MetaFieldType.edgeType(labelType, builder.getTypeFactory()),
                            builder.getRexBuilder()
                                .makeFieldAccess(refPathInput, VertexType.LABEL_FIELD_POSITION));
                    operands.add(labelRefRex);
                    rex2VariableInfo.put(labelRefRex,
                        new VariableInfo(false, edgeNewType.getLabelField().getName()));
                    PathInputRef leftRex = new PathInputRef(extendNodeLabel, extendNodeIndex,
                        edgeNewType);
                    for (RelDataTypeField field : leftRex.getType().getFieldList()) {
                        VariableInfo variableInfo;
                        RexNode operand;
                        if (addFieldNames.contains(field.getName())) {
                            // cast right expression to field type.
                            int indexOfAddFields = addFieldNames.indexOf(field.getName());
                            operand = builder.getRexBuilder()
                                .makeCast(field.getType(), addFieldProjects.get(indexOfAddFields));
                            variableInfo = new VariableInfo(false, field.getName());
                            operands.add(operand);
                            rex2VariableInfo.put(operand, variableInfo);
                            rexNodeMap.set(addFieldIndices.get(indexOfAddFields),
                                rexBuilder.makeFieldAccess(leftRex, field.getIndex()));
                        }
                    }
                    // Construct RexObjectConstruct for dynamic field append expression.
                    RexObjectConstruct rightRex = new RexObjectConstruct(edgeNewType, operands,
                        rex2VariableInfo);
                    List<PathModifyExpression> pathModifyExpressions = new ArrayList<>();
                    pathModifyExpressions.add(new PathModifyExpression(leftRex, rightRex));

                    PathInputRef leftRex2 = new PathInputRef(newVertexName,
                        newPathRecordType.getFieldList().size() - 1, vertexNewType);
                    Map<RexNode, VariableInfo> vertexRex2VariableInfo = new HashMap<>();
                    List<RexNode> vertexOperands = refPathInput.getType().getFieldList().stream()
                        .map(f -> {
                            RexNode operand = builder.getRexBuilder()
                                .makeFieldAccess(refPathInput, f.getIndex());
                            VariableInfo variableInfo = new VariableInfo(false, f.getName());
                            vertexRex2VariableInfo.put(operand, variableInfo);
                            return operand;
                        }).collect(Collectors.toList());
                    RexObjectConstruct rightRex2 = new RexObjectConstruct(leftRex2.getType(),
                        vertexOperands, vertexRex2VariableInfo);
                    pathModifyExpressions.add(new PathModifyExpression(leftRex2, rightRex2));
                    GQLJavaTypeFactory gqlJavaTypeFactory =
                        (GQLJavaTypeFactory) builder.getTypeFactory();
                    GeaFlowGraph currentGraph = gqlJavaTypeFactory.getCurrentGraph();
                    GraphRecordType graphSchema = (GraphRecordType) currentGraph.getRowType(
                        gqlJavaTypeFactory);
                    return MatchExtend.create(projectInput, pathModifyExpressions,
                        newPathRecordType, graphSchema);
                } else {
                    //Paths ending with an edge, add fields on the edge and add an edge extension.
                    edgeNewType = (EdgeRecordType) oldType;
                    addFieldNames = getNewFieldNames(addFieldProjects.size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    for (int i = 0; i < addFieldNames.size(); i++) {
                        edgeNewType = edgeNewType.add(addFieldNames.get(i),
                            addFieldProjects.get(i).getType(), true);
                    }
                    extendNodeIndex = lastNodeIndex;
                    extendNodeLabel = newPathRecordType.getFieldList().get(extendNodeIndex)
                        .getName();
                    PathInputRef leftRex = new PathInputRef(extendNodeLabel, extendNodeIndex,
                        edgeNewType);
                    newPathRecordType = newPathRecordType.addField(
                        newPathRecordType.getFieldNames().get(lastNodeIndex), edgeNewType, true);

                    for (RelDataTypeField field : leftRex.getType().getFieldList()) {
                        VariableInfo variableInfo;
                        RexNode operand;
                        if (addFieldNames.contains(field.getName())) {
                            // cast right expression to field type.
                            int indexOfAddFields = addFieldNames.indexOf(field.getName());
                            operand = builder.getRexBuilder().makeCast(field.getType(),
                                addFieldProjects.get(addFieldNames.indexOf(field.getName())));
                            rexNodeMap.set(addFieldIndices.get(indexOfAddFields),
                                rexBuilder.makeFieldAccess(leftRex, field.getIndex()));
                        } else {
                            operand = builder.getRexBuilder()
                                .makeFieldAccess(leftRex, field.getIndex());
                        }
                        variableInfo = new VariableInfo(false, field.getName());
                        operands.add(operand);
                        rex2VariableInfo.put(operand, variableInfo);
                    }
                    // Construct RexObjectConstruct for dynamic field append expression.
                    RexObjectConstruct rightRex = new RexObjectConstruct(leftRex.getType(),
                        operands, rex2VariableInfo);
                    List<PathModifyExpression> pathModifyExpressions = new ArrayList<>();
                    pathModifyExpressions.add(new PathModifyExpression(leftRex, rightRex));

                    edgeNewType = EdgeRecordType.emptyEdgeType(
                        ((EdgeRecordType) oldType).getSrcIdField().getType(),
                        builder.getTypeFactory());
                    newEdgeName = getNewFieldNames(1,
                        new HashSet<>(newPathRecordType.getFieldNames())).get(0);
                    newPathRecordType = newPathRecordType.addField(newEdgeName, edgeNewType, true);
                    RexNode srcIdRefRex = builder.getRexBuilder()
                        .makeFieldAccess(leftRex, EdgeType.SRC_ID_FIELD_POSITION);
                    List<RexNode> extendOperands = new ArrayList<>();
                    Map<RexNode, VariableInfo> extendRex2VariableInfo = new HashMap<>();
                    extendOperands.add(srcIdRefRex);
                    extendRex2VariableInfo.put(srcIdRefRex,
                        new VariableInfo(false, edgeNewType.getSrcIdField().getName()));
                    RexNode targetIdRefRex = builder.getRexBuilder()
                        .makeFieldAccess(leftRex, EdgeType.TARGET_ID_FIELD_POSITION);
                    extendOperands.add(targetIdRefRex);
                    extendRex2VariableInfo.put(targetIdRefRex,
                        new VariableInfo(false, edgeNewType.getTargetIdField().getName()));
                    RexNode labelRefRex = builder.getRexBuilder()
                        .makeFieldAccess(leftRex, EdgeType.LABEL_FIELD_POSITION);
                    extendOperands.add(labelRefRex);
                    extendRex2VariableInfo.put(labelRefRex,
                        new VariableInfo(false, edgeNewType.getLabelField().getName()));
                    if (edgeNewType.getTimestampField().isPresent()) {
                        RexNode timestampRex = builder.getRexBuilder()
                            .makeFieldAccess(leftRex, EdgeType.TIME_FIELD_POSITION);
                        extendOperands.add(timestampRex);
                        extendRex2VariableInfo.put(timestampRex, new VariableInfo(false,
                            edgeNewType.getTimestampField().get().getName()));
                    }
                    extendNodeIndex++;
                    PathInputRef leftRex2 = new PathInputRef(newEdgeName, extendNodeIndex,
                        edgeNewType);
                    RexObjectConstruct rightRex2 = new RexObjectConstruct(edgeNewType,
                        extendOperands, extendRex2VariableInfo);
                    pathModifyExpressions.add(new PathModifyExpression(leftRex2, rightRex2));

                    GQLJavaTypeFactory gqlJavaTypeFactory =
                        (GQLJavaTypeFactory) builder.getTypeFactory();
                    GeaFlowGraph currentGraph = gqlJavaTypeFactory.getCurrentGraph();
                    GraphRecordType graphSchema = (GraphRecordType) currentGraph.getRowType(
                        gqlJavaTypeFactory);
                    return MatchExtend.create(projectInput, pathModifyExpressions,
                        newPathRecordType, graphSchema);
                }
            } else {
                return projectInput;
            }
        } else if (from instanceof LogicalAggregate) {
            LogicalAggregate aggregate = (LogicalAggregate) from;
            List<RexNode> inputRexNode2RexInfo = new ArrayList<>();
            IMatchNode aggregateInput = from == to ? input : concatToMatchNode(builder,
                GQLRelUtil.toRel(aggregate.getInput()), to, input, inputRexNode2RexInfo);
            int lastNodeIndex = aggregateInput.getPathSchema().getFieldCount() - 1;
            if (lastNodeIndex < 0) {
                throw new GeaFlowDSLException("Need at least 1 node in the path to rewrite.");
            }
            //MatchAggregate needs to reference path fields, not using indices for referencing,
            // but using RexNode instead.
            List<MatchAggregateCall> adjustAggCalls;
            List<RexNode> adjustGroupList;
            Set<String> prunePathLabels = new HashSet<>();
            if (!inputRexNode2RexInfo.isEmpty()) {
                adjustGroupList = aggregate.getGroupSet().asList().stream()
                    .map(inputRexNode2RexInfo::get).collect(Collectors.toList());
                adjustAggCalls = aggregate.getAggCallList().stream().map(
                    aggCall -> {
                        List<RexNode> newArgList = aggCall.getArgList().stream()
                            .map(inputRexNode2RexInfo::get).collect(Collectors.toList());
                        return new MatchAggregateCall(aggCall.getAggregation(), aggCall.isDistinct(),
                            aggCall.isApproximate(), newArgList, aggCall.filterArg,
                            aggCall.getCollation(), aggCall.getType(), aggCall.getName());
                    }).collect(Collectors.toList());
            } else {
                String lastNodeLabel = aggregateInput.getPathSchema().getFieldList()
                    .get(lastNodeIndex).getName();
                prunePathLabels.add(lastNodeLabel);
                RelDataType oldType = aggregateInput.getPathSchema().getFieldList()
                    .get(lastNodeIndex).getType();
                adjustGroupList = aggregate.getGroupSet().asList().stream().map(idx -> builder.getRexBuilder()
                    .makeFieldAccess(new PathInputRef(lastNodeLabel, lastNodeIndex, oldType),
                        idx)).collect(Collectors.toList());
                adjustAggCalls = aggregate.getAggCallList().stream().map(
                    aggCall -> new MatchAggregateCall(aggCall.getAggregation(),
                        aggCall.isDistinct(), aggCall.isApproximate(),
                        aggCall.getArgList().stream().map(idx -> builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(lastNodeLabel, lastNodeIndex, oldType), idx)).collect(Collectors.toList()), aggCall.filterArg, aggCall.getCollation(),
                        aggCall.getType(), aggCall.getName())).collect(Collectors.toList());
            }

            //Get the pruning path.
            for (RelDataTypeField field : aggregateInput.getPathSchema().getFieldList()) {
                if (field.getType() instanceof VertexRecordType && adjustGroupList.stream()
                    .anyMatch(rexNode -> ((PathInputRef) ((RexFieldAccess) rexNode)
                        .getReferenceExpr()).getLabel().equals(field.getName())
                        && rexNode.getType() instanceof MetaFieldType
                        && ((MetaFieldType) rexNode.getType()).getMetaField()
                        .equals(MetaField.VERTEX_ID))) {
                    //The condition for preserving vertex in the after aggregating path is that the
                    // vertex ID appears in the Group.
                    prunePathLabels.add(field.getName());
                } else if (field.getType() instanceof EdgeRecordType) {
                    //The condition for preserving edges in the after aggregating path is that
                    // the edge srcId, targetId, and timestamp appear in the Group.
                    //todo: Since the GQL inferred from SQL does not have a Union, we will
                    // temporarily not consider Labels here.
                    boolean groupBySrcId = adjustGroupList.stream().anyMatch(rexNode ->
                        ((PathInputRef) ((RexFieldAccess) rexNode)
                            .getReferenceExpr()).getLabel().equals(field.getName())
                            && rexNode.getType() instanceof MetaFieldType
                            && ((MetaFieldType) rexNode.getType()).getMetaField()
                            .equals(MetaField.EDGE_SRC_ID));
                    boolean groupByTargetId = adjustGroupList.stream().anyMatch(rexNode ->
                        ((PathInputRef) ((RexFieldAccess) rexNode)
                            .getReferenceExpr()).getLabel().equals(field.getName())
                            && rexNode.getType() instanceof MetaFieldType
                            && ((MetaFieldType) rexNode.getType()).getMetaField()
                            .equals(MetaField.EDGE_TARGET_ID));
                    boolean groupByTs = true;
                    if (((EdgeRecordType) field.getType()).getTimestampField().isPresent()) {
                        groupByTs = adjustGroupList.stream().anyMatch(rexNode ->
                            ((PathInputRef) ((RexFieldAccess) rexNode)
                                .getReferenceExpr()).getLabel().equals(field.getName())
                                && rexNode.getType() instanceof MetaFieldType
                                && ((MetaFieldType) rexNode.getType()).getMetaField()
                                .equals(MetaField.EDGE_TS));
                    }
                    if (groupBySrcId && groupByTargetId && groupByTs) {
                        prunePathLabels.add(field.getName());
                    }
                }
            }
            if (prunePathLabels.isEmpty()) {
                prunePathLabels.add(aggregateInput.getPathSchema().firstFieldName().get());
            }

            PathRecordType aggPathType;
            if (adjustGroupList.size() > 0 || aggregate.getAggCallList().size() > 0) {
                PathRecordType pathType = aggregateInput.getPathSchema();
                PathRecordType prunePathType = new PathRecordType(pathType.getFieldList().stream()
                    .filter(f -> prunePathLabels.contains(f.getName()))
                    .collect(Collectors.toList()));
                //Prune the path, and add the aggregated value to the beginning of the path.
                RelDataType firstNodeType = prunePathType.firstField().get().getType();
                String firstNodeName = prunePathType.firstField().get().getName();
                int offset;
                if (firstNodeType instanceof VertexRecordType) {
                    VertexRecordType vertexNewType = (VertexRecordType) firstNodeType;
                    List<String> addFieldNames = getNewFieldNames(adjustGroupList.size(),
                        new HashSet<>(vertexNewType.getFieldNames()));
                    offset = vertexNewType.getFieldCount();
                    for (int i = 0; i < adjustGroupList.size(); i++) {
                        RelDataType dataType = adjustGroupList.get(i).getType();
                        vertexNewType = vertexNewType.add(addFieldNames.get(i), dataType, true);
                    }
                    addFieldNames = getNewAggFieldNames(aggregate.getAggCallList().size(),
                        new HashSet<>(vertexNewType.getFieldNames()));
                    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                        vertexNewType = vertexNewType.add(addFieldNames.get(i),
                            aggregate.getAggCallList().get(i).getType(), true);
                    }
                    for (int i = 0; i < adjustGroupList.size() + aggregate.getAggCallList().size(); i++) {
                        rexNodeMap.add(builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(firstNodeName, 0, vertexNewType), offset + i));
                    }
                    aggPathType = new PathRecordType(new ArrayList<>())
                        .addField(firstNodeName, vertexNewType, true);
                    aggPathType = aggPathType.concat(prunePathType, true);
                } else if (firstNodeType instanceof EdgeRecordType) {
                    EdgeRecordType edgeNewType = (EdgeRecordType) firstNodeType;
                    List<String> addFieldNames = getNewFieldNames(adjustGroupList.size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    offset = edgeNewType.getFieldCount();
                    for (int i = 0; i < adjustGroupList.size(); i++) {
                        RelDataType dataType = adjustGroupList.get(i).getType();
                        edgeNewType = edgeNewType.add(addFieldNames.get(i), dataType, true);
                    }
                    addFieldNames = getNewAggFieldNames(aggregate.getAggCallList().size(),
                        new HashSet<>(edgeNewType.getFieldNames()));
                    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                        edgeNewType = edgeNewType.add(addFieldNames.get(i),
                            aggregate.getAggCallList().get(i).getType(), true);
                    }
                    for (int i = 0; i < adjustGroupList.size() + aggregate.getAggCallList().size(); i++) {
                        rexNodeMap.add(builder.getRexBuilder().makeFieldAccess(
                            new PathInputRef(firstNodeName, 0, edgeNewType), offset + i));
                    }
                    aggPathType = new PathRecordType(new ArrayList<>())
                        .addField(firstNodeName, edgeNewType, true);
                    aggPathType = aggPathType.concat(prunePathType, true);
                } else {
                    throw new GeaFlowDSLException("Path node should be vertex or edge.");
                }
                return MatchAggregate.create(aggregateInput, aggregate.indicator, adjustGroupList,
                    adjustAggCalls, aggPathType);
            } else {
                return aggregateInput;
            }
        }
        return input;
    }

    /**
     * Generate n non-repeating field names with the format "f[$index_number]".
     */
    protected List<String> getNewFieldNames(int n, Set<String> exists) {
        if (n <= 0) {
            return Collections.emptyList();
        }
        List<String> validNames = new ArrayList<>(n);
        int i = 0;
        while (validNames.size() < n) {
            String newName = "f" + i;
            if (!exists.contains(newName)) {
                validNames.add(newName);
            }
            i++;
        }
        return validNames;
    }

    /**
     * Generate n non-repeating aggregate field names with the format "agg[$index_number]".
     */
    protected List<String> getNewAggFieldNames(int n, Set<String> exists) {
        if (n <= 0) {
            return Collections.emptyList();
        }
        List<String> validNames = new ArrayList<>(n);
        int i = 0;
        while (validNames.size() < n) {
            String newName = "agg" + i;
            if (!exists.contains(newName)) {
                validNames.add(newName);
            }
            i++;
        }
        return validNames;
    }

    /**
     * Replace the references to path nodes in the leftRexNodes with references to
     * the newGraphMatch directly, based on the label of the reference.
     */
    protected List<RexNode> adjustLeftRexNodes(List<RexNode> leftRexNodes, GraphMatch newGraphMatch,
                                               RelBuilder builder) {
        return leftRexNodes.stream().map(prj -> GQLRexUtil.replace(prj, rexNode -> {
            if (rexNode instanceof RexFieldAccess
                && ((RexFieldAccess) rexNode).getReferenceExpr() instanceof PathInputRef) {
                PathInputRef pathInputRef =
                    (PathInputRef) ((RexFieldAccess) rexNode).getReferenceExpr();
                String label = pathInputRef.getLabel();
                int index = newGraphMatch.getRowType().getField(label, true, false).getIndex();
                RelDataType type = newGraphMatch.getRowType().getField(label, true, false)
                    .getType();
                PathInputRef vertexRef = new PathInputRef(label, index, type);
                return builder.getRexBuilder().makeFieldAccess(vertexRef, ((RexFieldAccess) rexNode).getField().getIndex());

            }
            return rexNode;
        })).collect(Collectors.toList());
    }

    /**
     * Replace the references to path nodes in the leftRexNodes with references to
     * the newGraphMatch, based on the label of the reference. If the label of the reference
     * exists in the left branch, automatically add an offset to make it a reference to the same
     * name Node in the right branch.
     */
    protected List<RexNode> adjustRightRexNodes(List<RexNode> rightRexNodes,
                                                GraphMatch newGraphMatch, RelBuilder builder,
                                                IMatchNode leftPathPattern,
                                                IMatchNode rightPathPattern) {
        final IMatchNode finalLeft = leftPathPattern;
        final IMatchNode finalRight = rightPathPattern;
        return rightRexNodes.stream().map(prj -> GQLRexUtil.replace(prj, rexNode -> {
            if (rexNode instanceof RexFieldAccess
                && ((RexFieldAccess) rexNode).getReferenceExpr() instanceof PathInputRef) {
                PathInputRef pathInputRef =
                    (PathInputRef) ((RexFieldAccess) rexNode).getReferenceExpr();
                String label = pathInputRef.getLabel();
                boolean isConflictLabel = leftPathPattern.getPathSchema().getFieldNames()
                    .contains(label);
                if (isConflictLabel) {
                    int index = finalRight.getRowType().getField(label, true, false).getIndex();
                    index += finalLeft.getRowType().getFieldCount();
                    label = newGraphMatch.getRowType().getFieldList().get(index).getName();
                    RelDataType type = newGraphMatch.getRowType().getFieldList().get(index)
                        .getType();
                    PathInputRef vertexRef = new PathInputRef(label, index, type);
                    return builder.getRexBuilder().makeFieldAccess(vertexRef,
                        ((RexFieldAccess) rexNode).getField().getIndex());
                } else {
                    int index = newGraphMatch.getRowType().getField(label, true, false).getIndex();
                    RelDataType type = newGraphMatch.getRowType().getFieldList().get(index)
                        .getType();
                    PathInputRef vertexRef = new PathInputRef(label, index, type);
                    return builder.getRexBuilder().makeFieldAccess(vertexRef,
                        ((RexFieldAccess) rexNode).getField().getIndex());
                }
            }
            return rexNode;
        })).collect(Collectors.toList());
    }

    /**
     * Handling the transformation of SQL RelNodes for the MatchJoinTableToGraphMatch
     * and TableJoinMatchToGraphMatch rules into GQL matches.
     */
    protected RelNode processGraphMatchJoinTable(RelOptRuleCall call, LogicalJoin join,
                                                 LogicalGraphMatch graphMatch,
                                                 LogicalProject project,
                                                 LogicalTableScan tableScan,
                                                 RelNode leftInput, RelNode leftHead,
                                                 RelNode rightInput, RelNode rightHead,
                                                 boolean isMatchInLeft) {
        GeaFlowTable geaflowTable = tableScan.getTable().unwrap(GeaFlowTable.class);
        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph currentGraph = typeFactory.getCurrentGraph();
        if (!currentGraph.containTable(geaflowTable)) {
            if (geaflowTable instanceof VertexTable || geaflowTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    geaflowTable.getName());
            }
            return null;
        }
        GraphJoinType graphJoinType = getJoinType(join);
        RelDataType tableType = tableScan.getRowType();
        IMatchNode matchNode = graphMatch.getPathPattern();
        RelBuilder relBuilder = call.builder();
        List<RexNode> rexLeftNodeMap = new ArrayList<>();
        List<RexNode> rexRightNodeMap = new ArrayList<>();
        IMatchNode concatedLeftMatch;
        IMatchNode newPathPattern;
        boolean isEdgeReverse = false;
        switch (graphJoinType) {
            case EDGE_JOIN_VERTEX:
            case VERTEX_JOIN_EDGE:
                if (geaflowTable instanceof VertexTable) { // graph match join vertex table
                    if (matchNode instanceof SingleMatchNode && GQLRelUtil.getLatestMatchNode(
                        (SingleMatchNode) matchNode) instanceof EdgeMatch) {
                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                             : concatToMatchNode(relBuilder, rightInput, rightHead,
                                                 matchNode, rexRightNodeMap);
                        String nodeName = geaflowTable.getName();
                        PathRecordType pathRecordType =
                            concatedLeftMatch.getPathSchema().getFieldNames().contains(nodeName)
                            ? concatedLeftMatch.getPathSchema()
                            : concatedLeftMatch.getPathSchema().addField(nodeName, tableType, false);
                        newPathPattern = VertexMatch.create(concatedLeftMatch.getCluster(),
                            (SingleMatchNode) concatedLeftMatch, nodeName,
                            Collections.singletonList(nodeName), tableType, pathRecordType);
                        newPathPattern =
                            isMatchInLeft ? concatToMatchNode(relBuilder, rightInput, rightHead,
                                newPathPattern, rexRightNodeMap)
                                             : concatToMatchNode(relBuilder, leftInput, leftHead,
                                                 newPathPattern, rexLeftNodeMap);
                    } else {
                        String nodeName = geaflowTable.getName();
                        assert currentGraph.getVertexTables().stream()
                            .anyMatch(t -> t.getName().equalsIgnoreCase(geaflowTable.getName()));
                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                             : concatToMatchNode(relBuilder, rightInput, rightHead,
                                                 matchNode, rexRightNodeMap);
                        RelDataType vertexRelType = geaflowTable.getRowType(
                            relBuilder.getTypeFactory());
                        PathRecordType rightPathType = PathRecordType.EMPTY.addField(geaflowTable.getName(),
                            vertexRelType, false);
                        VertexMatch rightVertexMatch = VertexMatch.create(concatedLeftMatch.getCluster(), null,
                            nodeName, Collections.singletonList(geaflowTable.getName()),
                            vertexRelType, rightPathType);
                        IMatchNode matchJoinRight =
                            isMatchInLeft ? concatToMatchNode(relBuilder, rightInput, rightHead,
                                rightVertexMatch, rexRightNodeMap)
                                             : concatToMatchNode(relBuilder, leftInput, leftHead,
                                                 rightVertexMatch, rexLeftNodeMap);
                        MatchJoin matchJoin = MatchJoin.create(concatedLeftMatch.getCluster(),
                            concatedLeftMatch.getTraitSet(), concatedLeftMatch, matchJoinRight,
                            relBuilder.getRexBuilder().makeLiteral(true), JoinRelType.INNER);

                        PathInputRef vertexRef = new PathInputRef(nodeName,
                            matchJoin.getRowType().getField(nodeName, true, false).getIndex(),
                            matchJoin.getRowType().getField(nodeName, true, false).getType());
                        RexNode operand1 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        RelDataTypeField field = matchJoin.getRowType().getFieldList().get(
                            matchJoin.getRowType().getFieldCount() - rightVertexMatch.getRowType()
                                .getFieldCount());
                        vertexRef = new PathInputRef(field.getName(), field.getIndex(),
                            field.getType());
                        RexNode operand2 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        SqlOperator equalsOperator = SqlStdOperatorTable.EQUALS;
                        RexNode condition = relBuilder.getRexBuilder()
                            .makeCall(equalsOperator, operand1, operand2);
                        newPathPattern = matchJoin.copy(matchJoin.getTraitSet(), condition,
                            matchJoin.getLeft(), matchJoin.getRight(), matchJoin.getJoinType());
                    }
                } else if (geaflowTable instanceof EdgeTable) {
                    if (matchNode instanceof SingleMatchNode && GQLRelUtil.getLatestMatchNode(
                        (SingleMatchNode) matchNode) instanceof VertexMatch) {
                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                             : concatToMatchNode(relBuilder, rightInput, rightHead,
                                                 matchNode, rexRightNodeMap);
                        isEdgeReverse = graphJoinType.equals(GraphJoinType.EDGE_JOIN_VERTEX);
                        EdgeDirection edgeDirection =
                            isEdgeReverse ? EdgeDirection.IN : EdgeDirection.OUT;
                        String edgeName = geaflowTable.getName();
                        PathRecordType pathRecordType =
                            concatedLeftMatch.getPathSchema().getFieldNames().contains(edgeName)
                            ? concatedLeftMatch.getPathSchema()
                            : concatedLeftMatch.getPathSchema().addField(edgeName, tableType, false);
                        newPathPattern = EdgeMatch.create(concatedLeftMatch.getCluster(),
                            (SingleMatchNode) concatedLeftMatch, edgeName,
                            Collections.singletonList(edgeName), edgeDirection, tableType,
                            pathRecordType);
                        newPathPattern =
                            isMatchInLeft ? concatToMatchNode(relBuilder, rightInput, rightHead,
                                newPathPattern, rexRightNodeMap)
                                             : concatToMatchNode(relBuilder, leftInput, leftHead,
                                                 newPathPattern, rexLeftNodeMap);

                    } else {
                        String edgeName = geaflowTable.getName();
                        GraphDescriptor graphDescriptor = currentGraph.getDescriptor();
                        Optional<EdgeDescriptor> edgeDesc = graphDescriptor.edges.stream()
                            .filter(e -> e.type.equals(geaflowTable.getName())).findFirst();
                        VertexTable dummyVertex = null;
                        if (edgeDesc.isPresent()) {
                            EdgeDescriptor edgeDescriptor = edgeDesc.get();
                            String dummyNodeType = edgeDescriptor.sourceType;
                            dummyVertex = currentGraph.getVertexTables().stream()
                                .filter(v -> v.getName().equals(dummyNodeType)).findFirst().get();
                        }
                        if (dummyVertex == null) {
                            return null;
                        }
                        String dummyNodeName = dummyVertex.getName();
                        RelDataType dummyVertexRelType = dummyVertex.getRowType(
                            relBuilder.getTypeFactory());
                        PathRecordType pathRecordType = new PathRecordType(
                            new ArrayList<>()).addField(dummyNodeName, dummyVertexRelType, true);
                        VertexMatch dummyVertexMatch = VertexMatch.create(matchNode.getCluster(),
                            null, dummyNodeName, Collections.singletonList(dummyVertex.getName()),
                            dummyVertexRelType, pathRecordType);
                        RelDataType edgeRelType = geaflowTable.getRowType(
                            relBuilder.getTypeFactory());
                        pathRecordType = pathRecordType.addField(edgeName, edgeRelType, true);
                        EdgeMatch edgeMatch = EdgeMatch.create(matchNode.getCluster(),
                            dummyVertexMatch, edgeName,
                            Collections.singletonList(geaflowTable.getName()), EdgeDirection.OUT,
                            edgeRelType, pathRecordType);

                        concatedLeftMatch =
                            isMatchInLeft ? concatToMatchNode(relBuilder, leftInput, leftHead,
                                matchNode, rexLeftNodeMap)
                                             : concatToMatchNode(relBuilder, rightInput, rightHead,
                                                 matchNode, rexRightNodeMap);
                        IMatchNode matchJoinRight =
                            isMatchInLeft ? concatToMatchNode(relBuilder, rightInput, rightHead,
                                edgeMatch, rexRightNodeMap)
                                             : concatToMatchNode(relBuilder, leftInput, leftHead,
                                                 edgeMatch, rexLeftNodeMap);
                        MatchJoin matchJoin = MatchJoin.create(matchNode.getCluster(),
                            matchNode.getTraitSet(), concatedLeftMatch, matchJoinRight,
                            relBuilder.getRexBuilder().makeLiteral(true), JoinRelType.INNER);

                        PathInputRef vertexRef = new PathInputRef(dummyNodeName,
                            matchJoin.getRowType().getField(dummyNodeName, true, false).getIndex(),
                            matchJoin.getRowType().getField(dummyNodeName, true, false).getType());
                        RexNode operand1 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        RelDataTypeField field = matchJoin.getRowType().getFieldList().get(
                            matchJoin.getRowType().getFieldCount() - edgeMatch.getRowType()
                                .getFieldCount());
                        vertexRef = new PathInputRef(field.getName(), field.getIndex(),
                            field.getType());
                        RexNode operand2 = relBuilder.getRexBuilder()
                            .makeFieldAccess(vertexRef, VertexType.ID_FIELD_POSITION);
                        SqlOperator equalsOperator = SqlStdOperatorTable.EQUALS;
                        RexNode condition = relBuilder.getRexBuilder()
                            .makeCall(equalsOperator, operand1, operand2);
                        newPathPattern = matchJoin.copy(matchJoin.getTraitSet(), condition,
                            matchJoin.getLeft(), matchJoin.getRight(), matchJoin.getJoinType());
                    }
                } else {
                    return null;
                }
                break;
            default:
                return null;
        }
        if (newPathPattern == null) {
            return null;
        }
        GraphMatch newGraphMatch = graphMatch.copy(newPathPattern);
        // Add the original Projects from the GraphMatch branch, filtering out fields that
        // no longer exist after rebuilding GraphMatch inputs.
        List<RexNode> oldProjects = project.getProjects().stream().filter(prj -> {
            if (prj instanceof RexFieldAccess
                && ((RexFieldAccess) prj).getReferenceExpr() instanceof PathInputRef) {
                PathInputRef pathInputRef =
                    (PathInputRef) ((RexFieldAccess) prj).getReferenceExpr();
                String label = pathInputRef.getLabel();
                RelDataTypeField pathField = newGraphMatch.getRowType().getField(label, true, false);
                if (pathField != null) {
                    RexFieldAccess fieldAccess = (RexFieldAccess) prj;
                    int index = fieldAccess.getField().getIndex();
                    return index < pathField.getType().getFieldList().size()
                        && fieldAccess.getField().equals(pathField.getType().getFieldList().get(index));
                }
            }
            return false;
        }).collect(Collectors.toList());
        List<RexNode> newProjects = new ArrayList<>();
        if (newPathPattern instanceof MatchJoin) {
            newProjects.addAll(
                isMatchInLeft ? adjustLeftRexNodes(oldProjects, newGraphMatch,
                    relBuilder) : adjustRightRexNodes(oldProjects, newGraphMatch,
                    relBuilder, (IMatchNode) newPathPattern.getInput(0),
                    (IMatchNode) newPathPattern.getInput(1)));
        } else {
            newProjects.addAll(adjustLeftRexNodes(oldProjects, newGraphMatch, relBuilder));
        }

        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        // Add fields of the Table into the projects.
        String tableName = geaflowTable.getName();
        RelDataTypeField pathTableField = newPathPattern.getPathSchema()
            .getField(tableName, true, false);
        List<RexNode> tableProjects = new ArrayList<>();
        if (pathTableField != null) {
            int baseOffset = newProjects.size();
            PathInputRef pathTableRef = new PathInputRef(tableName, pathTableField.getIndex(),
                pathTableField.getType());
            tableProjects = tableType.getFieldList().stream()
                .map(f -> rexBuilder.makeFieldAccess(pathTableRef, f.getIndex()))
                .collect(Collectors.toList());
            newProjects.addAll(tableProjects);

            //In the case of reverse matching in the IN direction, the positions of the source
            // vertex and the destination vertex are swapped.
            if (isEdgeReverse) {
                int edgeSrcIdIndex = tableType.getFieldList().stream().filter(
                    f -> f.getType() instanceof MetaFieldType
                            && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_SRC_ID))
                    .collect(Collectors.toList()).get(0).getIndex();
                int edgeTargetIdIndex = tableType.getFieldList().stream().filter(
                    f -> f.getType() instanceof MetaFieldType
                            && ((MetaFieldType) f.getType()).getMetaField()
                            .equals(MetaField.EDGE_TARGET_ID)).collect(Collectors.toList()).get(0)
                    .getIndex();
                Collections.swap(newProjects, baseOffset + edgeSrcIdIndex,
                    baseOffset + edgeTargetIdIndex);
            }
        }

        // Add fields newly added in the rebuild of the left branch.
        if (rexLeftNodeMap.size() > 0) {
            List<RexNode> tmpLeftProjects = new ArrayList<>(rexLeftNodeMap);
            newProjects.addAll(adjustLeftRexNodes(tmpLeftProjects, newGraphMatch, relBuilder));
        }

        // Add fields newly added in the rebuild of the right branch.
        if (rexRightNodeMap.size() > 0) {
            List<RexNode> tmpRightProjects = new ArrayList<>(rexRightNodeMap);
            if (newPathPattern instanceof MatchJoin) {
                newProjects.addAll(adjustRightRexNodes(tmpRightProjects, newGraphMatch, relBuilder,
                    (IMatchNode) newPathPattern.getInput(0),
                    (IMatchNode) newPathPattern.getInput(1)));
            } else {
                newProjects.addAll(adjustLeftRexNodes(tmpRightProjects, newGraphMatch, relBuilder));
            }
        }

        // Complete the projection from Path to Row.
        List<RelDataTypeField> matchTypeFields = new ArrayList<>();
        List<String> newFieldNames = getNewFieldNames(newProjects.size(), new HashSet<>());
        for (int i = 0; i < newProjects.size(); i++) {
            matchTypeFields.add(
                new RelDataTypeFieldImpl(newFieldNames.get(i), i, newProjects.get(i).getType()));
        }
        RelNode tail = LogicalProject.create(newGraphMatch, newProjects,
            new RelRecordType(matchTypeFields));

        // Complete the Join projection.
        if (newPathPattern instanceof MatchJoin) {
            rexRightNodeMap = adjustRightRexNodes(rexRightNodeMap, newGraphMatch, relBuilder,
                (IMatchNode) newPathPattern.getInput(0), (IMatchNode) newPathPattern.getInput(1));
        }
        List<RexNode> joinProjects = new ArrayList<>();
        //  If the left branch undergoes rebuilding, take the reconstructed Rex from
        //  the left branch, otherwise take the original Projects.
        final RelNode finalTail = tail;
        int projectFieldCount = oldProjects.size();
        int joinFieldCount = projectFieldCount + tableProjects.size();
        if (rexLeftNodeMap.size() > 0) {
            joinProjects.addAll(
                IntStream.range(joinFieldCount, joinFieldCount + rexLeftNodeMap.size())
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
        } else {
            if (isMatchInLeft) {
                joinProjects.addAll(IntStream.range(0, projectFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            } else {
                joinProjects.addAll(IntStream.range(projectFieldCount, joinFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            }
        }
        // If the right branch undergoes rebuilding, take the reconstructed Rex from the right
        // branch, otherwise take the original Projects.
        if (rexRightNodeMap.size() > 0) {
            joinProjects.addAll(IntStream.range(joinFieldCount + rexLeftNodeMap.size(),
                    joinFieldCount + rexLeftNodeMap.size() + rexRightNodeMap.size())
                .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i)).collect(Collectors.toList()));
        } else {
            if (isMatchInLeft) {
                joinProjects.addAll(IntStream.range(projectFieldCount, joinFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            } else {
                joinProjects.addAll(IntStream.range(0, projectFieldCount)
                    .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i))
                    .collect(Collectors.toList()));
            }
        }
        tail = LogicalProject.create(tail, joinProjects, join.getRowType());
        return tail;
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
