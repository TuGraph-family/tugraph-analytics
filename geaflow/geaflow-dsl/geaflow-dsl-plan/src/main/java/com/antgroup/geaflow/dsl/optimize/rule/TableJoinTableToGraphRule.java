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
import com.antgroup.geaflow.dsl.common.descriptor.EdgeDescriptor;
import com.antgroup.geaflow.dsl.common.descriptor.GraphDescriptor;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphScan;
import com.antgroup.geaflow.dsl.rel.match.EdgeMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.VertexMatch;
import com.antgroup.geaflow.dsl.rex.PathInputRef;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

public class TableJoinTableToGraphRule extends AbstractJoinToGraphRule {

    public static final TableJoinTableToGraphRule INSTANCE = new TableJoinTableToGraphRule();

    private TableJoinTableToGraphRule() {
        super(operand(LogicalJoin.class,
            operand(LogicalTableScan.class, any()),
            operand(LogicalTableScan.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableScan leftTableScan = call.rel(1);
        LogicalTableScan rightTableScan = call.rel(2);

        GeaFlowTable leftTable = leftTableScan.getTable().unwrap(GeaFlowTable.class);
        GeaFlowTable rightTable = rightTableScan.getTable().unwrap(GeaFlowTable.class);

        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph currentGraph = typeFactory.getCurrentGraph();
        if (!currentGraph.containTable(leftTable)) {
            if (leftTable instanceof VertexTable || leftTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    leftTable.getName());
            }
            return;
        }
        if (!currentGraph.containTable(rightTable)) {
            if (rightTable instanceof VertexTable || rightTable instanceof EdgeTable) {
                throw new GeaFlowDSLException("Unknown graph element: {}, use graph please.",
                    rightTable.getName());
            }
            return;
        }

        LogicalJoin join = call.rel(0);
        GraphJoinType graphJoinType = getJoinType(join);
        switch (graphJoinType) {
            case VERTEX_JOIN_EDGE:
                RelNode graphMatch;
                if (leftTable instanceof VertexTable
                    && rightTable instanceof EdgeTable) {
                    VertexTable vertexTable = (VertexTable) leftTable;
                    EdgeTable edgeTable = (EdgeTable) rightTable;
                    graphMatch = vertexJoinEdgeSrc(vertexTable, edgeTable, call, true);
                } else if (leftTable instanceof EdgeTable
                    && rightTable instanceof VertexTable) {
                    VertexTable vertexTable = (VertexTable) rightTable;
                    EdgeTable edgeTable = (EdgeTable) leftTable;
                    graphMatch = vertexJoinEdgeSrc(vertexTable, edgeTable, call, false);
                } else {
                    graphMatch = null;
                }
                if (graphMatch == null) {
                    return;
                }
                call.transformTo(graphMatch);
                break;
            case EDGE_JOIN_VERTEX:
                if (leftTable instanceof VertexTable
                    && rightTable instanceof EdgeTable) {
                    VertexTable vertexTable = (VertexTable) leftTable;
                    EdgeTable edgeTable = (EdgeTable) rightTable;
                    graphMatch = edgeTargetJoinVertex(vertexTable, edgeTable, call, true);
                } else if (leftTable instanceof EdgeTable
                    && rightTable instanceof VertexTable) {
                    VertexTable vertexTable = (VertexTable) rightTable;
                    EdgeTable edgeTable = (EdgeTable) leftTable;
                    graphMatch = edgeTargetJoinVertex(vertexTable, edgeTable, call, false);
                } else {
                    graphMatch = null;
                }
                if (graphMatch == null) {
                    return;
                }
                call.transformTo(graphMatch);
                break;
            default:
        }
    }

    private RelNode vertexJoinEdgeSrc(VertexTable vertexTable,
                                      EdgeTable edgeTable,
                                      RelOptRuleCall call,
                                      boolean isLeftTableVertex) {
        return vertexJoinEdge(vertexTable, edgeTable, call, EdgeDirection.OUT, isLeftTableVertex);
    }

    private RelNode edgeTargetJoinVertex(VertexTable vertexTable,
                                         EdgeTable edgeTable,
                                         RelOptRuleCall call,
                                         boolean isLeftTableVertex) {
        return vertexJoinEdge(vertexTable, edgeTable, call, EdgeDirection.IN, isLeftTableVertex);
    }

    private RelNode vertexJoinEdge(VertexTable vertexTable,
                                   EdgeTable edgeTable,
                                   RelOptRuleCall call,
                                   EdgeDirection direction,
                                   boolean isLeftTableVertex) {
        LogicalJoin join = call.rel(0);
        RelOptCluster cluster = join.getCluster();
        RelDataType vertexRelType = vertexTable.getRowType(call.builder().getTypeFactory());
        PathRecordType pathRecordType = PathRecordType.EMPTY;
        String nodeName = vertexTable.getName();
        RelDataType edgeRelType = edgeTable.getRowType(call.builder().getTypeFactory());
        String edgeName = edgeTable.getName();

        VertexMatch vertexMatch;
        EdgeMatch edgeMatch;
        IMatchNode matchNode;
        boolean swapSrcTargetId;
        if (isLeftTableVertex) {
            pathRecordType = pathRecordType.addField(nodeName, vertexRelType, false);
            vertexMatch = VertexMatch.create(cluster, null, nodeName,
                Collections.singletonList(vertexTable.getName()), vertexRelType, pathRecordType);
            pathRecordType = pathRecordType.addField(edgeName, edgeRelType, false);
            edgeMatch = EdgeMatch.create(cluster, vertexMatch, edgeName,
                Collections.singletonList(edgeTable.getName()), direction, edgeRelType, pathRecordType);
            swapSrcTargetId = direction.equals(EdgeDirection.IN);
            matchNode = edgeMatch;
        } else {
            GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
            GeaFlowGraph graph = typeFactory.getCurrentGraph();
            GraphDescriptor graphDescriptor = graph.getDescriptor();
            Optional<EdgeDescriptor> edgeDesc = graphDescriptor.edges.stream().filter(
                e -> e.type.equals(edgeTable.getName())).findFirst();
            VertexTable dummyVertex = null;
            if (edgeDesc.isPresent()) {
                EdgeDescriptor edgeDescriptor = edgeDesc.get();
                String dummyNodeType = direction.equals(EdgeDirection.IN)
                                       ? edgeDescriptor.sourceType : edgeDescriptor.targetType;
                dummyVertex = graph.getVertexTables().stream().filter(
                    v -> v.getName().equals(dummyNodeType)).findFirst().get();
            }
            if (dummyVertex == null) {
                return null;
            }
            String dummyNodeName = dummyVertex.getName();
            RelDataType dummyVertexRelType = dummyVertex.getRowType(call.builder().getTypeFactory());
            pathRecordType = pathRecordType.addField(dummyNodeName, dummyVertexRelType, false);
            VertexMatch dummyVertexMatch = VertexMatch.create(cluster, null, dummyNodeName,
                Collections.singletonList(dummyVertex.getName()), dummyVertexRelType, pathRecordType);
            pathRecordType = pathRecordType.addField(edgeName, edgeRelType, false);
            EdgeDirection reverseDirection = EdgeDirection.reverse(direction);
            edgeMatch = EdgeMatch.create(cluster, dummyVertexMatch, edgeName,
                Collections.singletonList(edgeTable.getName()),
                reverseDirection, edgeRelType, pathRecordType);
            swapSrcTargetId = reverseDirection.equals(EdgeDirection.IN);
            pathRecordType = pathRecordType.addField(nodeName, vertexRelType, false);
            vertexMatch = VertexMatch.create(cluster, edgeMatch, nodeName,
                Collections.singletonList(vertexTable.getName()), vertexRelType, pathRecordType);
            matchNode = vertexMatch;
        }

        RelDataType joinOutputType = join.getRowType();
        List<String> fieldNames = new ArrayList<>(joinOutputType.getFieldNames());
        List<RexNode> projects = new ArrayList<>(fieldNames.size());
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        if (isLeftTableVertex) {
            //Add vertex fields
            PathInputRef vertexRef = new PathInputRef(vertexMatch.getLabel(), 0,
                vertexRelType);
            for (int i = 0; i < vertexRelType.getFieldCount(); i++) {
                projects.add(rexBuilder.makeFieldAccess(vertexRef, i));
            }
            //Add edge fields
            PathInputRef edgeRef = new PathInputRef(edgeMatch.getLabel(), 1, edgeRelType);
            for (int i = 0; i < edgeRelType.getFieldCount(); i++) {
                projects.add(rexBuilder.makeFieldAccess(edgeRef, i));
            }
        } else {
            //Add edge fields
            PathInputRef edgeRef = new PathInputRef(edgeMatch.getLabel(), 1, edgeRelType);
            for (int i = 0; i < edgeRelType.getFieldCount(); i++) {
                projects.add(rexBuilder.makeFieldAccess(edgeRef, i));
            }
            //Add vertex fields
            PathInputRef vertexRef = new PathInputRef(vertexMatch.getLabel(), 2,
                vertexRelType);
            for (int i = 0; i < vertexRelType.getFieldCount(); i++) {
                projects.add(rexBuilder.makeFieldAccess(vertexRef, i));
            }
        }
        //In the case of reverse matching in the IN direction, the positions of the source
        // vertex and the destination vertex are swapped.
        if (swapSrcTargetId) {
            int edgeSrcIdIndex = edgeRelType.getFieldList().stream().filter(
                f -> f.getType() instanceof MetaFieldType
                        && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_SRC_ID))
                .collect(Collectors.toList()).get(0).getIndex();
            int edgeTargetIdIndex = edgeRelType.getFieldList().stream().filter(
                f -> f.getType() instanceof MetaFieldType
                        && ((MetaFieldType) f.getType()).getMetaField().equals(MetaField.EDGE_TARGET_ID))
                .collect(Collectors.toList()).get(0).getIndex();
            int baseOffset = isLeftTableVertex ? vertexRelType.getFieldCount() : 0;
            Collections.swap(projects, baseOffset + edgeSrcIdIndex, baseOffset + edgeTargetIdIndex);
            Collections.swap(fieldNames, baseOffset + edgeSrcIdIndex, baseOffset + edgeTargetIdIndex);
        }

        assert projects.size() == fieldNames.size();
        GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) call.builder().getTypeFactory();
        GeaFlowGraph graph = typeFactory.getCurrentGraph();
        LogicalGraphScan graphScan = LogicalGraphScan.create(cluster, graph);
        // add remain filter.
        matchNode = addRemainFilter(join, matchNode);
        LogicalGraphMatch graphMatch = LogicalGraphMatch.create(cluster, graphScan,
            matchNode, matchNode.getPathSchema());
        return LogicalProject.create(graphMatch, projects, joinOutputType);
    }

}
