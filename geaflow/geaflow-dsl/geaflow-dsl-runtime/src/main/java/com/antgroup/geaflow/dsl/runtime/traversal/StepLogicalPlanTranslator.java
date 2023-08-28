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

package com.antgroup.geaflow.dsl.runtime.traversal;

import static com.antgroup.geaflow.common.utils.ArrayUtil.toIntArray;
import static org.apache.calcite.sql.SqlKind.DESCENDING;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.calcite.EdgeRecordType;
import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.calcite.VertexRecordType;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.BinaryUtil;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.dsl.rel.AbstractMatchNodeVisitor;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.PathModify.PathModifyExpression;
import com.antgroup.geaflow.dsl.rel.PathSort;
import com.antgroup.geaflow.dsl.rel.match.EdgeMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.LoopUntilMatch;
import com.antgroup.geaflow.dsl.rel.match.MatchDistinct;
import com.antgroup.geaflow.dsl.rel.match.MatchFilter;
import com.antgroup.geaflow.dsl.rel.match.MatchJoin;
import com.antgroup.geaflow.dsl.rel.match.MatchPathModify;
import com.antgroup.geaflow.dsl.rel.match.MatchPathSort;
import com.antgroup.geaflow.dsl.rel.match.MatchUnion;
import com.antgroup.geaflow.dsl.rel.match.SubQueryStart;
import com.antgroup.geaflow.dsl.rel.match.VertexMatch;
import com.antgroup.geaflow.dsl.rel.match.VirtualEdgeMatch;
import com.antgroup.geaflow.dsl.rex.RexObjectConstruct.VariableInfo;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.ExpressionTranslator;
import com.antgroup.geaflow.dsl.runtime.expression.field.FieldExpression;
import com.antgroup.geaflow.dsl.runtime.expression.field.ParameterFieldExpression;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVirtualEdgeFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVirtualEdgeFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepBoolFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepJoinFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepJoinFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepKeyFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepNodeTypeFilterFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepPathModifyFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepSortFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepSortFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.table.order.OrderByField;
import com.antgroup.geaflow.dsl.runtime.function.table.order.OrderByField.ORDER;
import com.antgroup.geaflow.dsl.runtime.function.table.order.SortInfo;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ConstantStartId;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ParameterStartId;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.StartId;
import com.antgroup.geaflow.dsl.runtime.util.FilterPushDownUtil;
import com.antgroup.geaflow.dsl.util.GQLRexUtil;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class StepLogicalPlanTranslator {

    /**
     * Translate path pattern to {@link StepLogicalPlan}.
     *
     * @param graphMatch The path pattern to translate.
     * @return The last node of the {@link StepLogicalPlan}.
     */
    public StepLogicalPlan translate(GraphMatch graphMatch,
                                     StepLogicalPlanSet logicalPlanSet) {
        // do the plan translate.
        LogicalPlanTranslatorVisitor translator =
            new LogicalPlanTranslatorVisitor(logicalPlanSet);
        return translator.translate(graphMatch.getPathPattern());
    }

    /**
     * Translate the {@link RelNode} in graph match to {@link StepLogicalPlan}.
     **/
    private static class LogicalPlanTranslatorVisitor extends AbstractMatchNodeVisitor<StepLogicalPlan> {

        private final GraphSchema graphSchema;

        private final StepLogicalPlanSet logicalPlanSet;

        private final GraphSchema modifyGraphSchema;

        // label -> plan
        private Map<String, StepLogicalPlan> planCache = new HashMap<>();

        private StepLogicalPlan logicalPlanHead = null;

        private final Map<RelNode, RexNode> nodePushDownFilters;

        public LogicalPlanTranslatorVisitor(StepLogicalPlanSet logicalPlanSet) {
            this(logicalPlanSet, new HashMap<>());
        }

        private LogicalPlanTranslatorVisitor(StepLogicalPlanSet logicalPlanSet,
                                             Map<RelNode, RexNode> nodePushDownFilters) {
            this.graphSchema = logicalPlanSet.getGraphSchema();
            this.logicalPlanSet = Objects.requireNonNull(logicalPlanSet);
            this.modifyGraphSchema = graphSchema;
            this.nodePushDownFilters = Objects.requireNonNull(nodePushDownFilters);
        }

        public StepLogicalPlan translate(RelNode pathPattern) {
            return this.visit(pathPattern);
        }

        @Override
        public StepLogicalPlan visitVertexMatch(VertexMatch vertexMatch) {
            String label = vertexMatch.getLabel();
            RexNode filter = nodePushDownFilters.get(vertexMatch);
            // TODO use optimizer rule to push the filter to the vertex-match.
            if (vertexMatch.getPushDownFilter() != null) {
                filter = vertexMatch.getPushDownFilter();
            }
            Set<StartId> startIds = new HashSet<>();
            if (vertexMatch.getInput() == null && filter != null) {
                Set<RexNode> ids = GQLRexUtil.findVertexIds(filter, (VertexRecordType) vertexMatch.getNodeType());
                startIds = toStartIds(ids);
            }
            Set<BinaryString> nodeTypes = vertexMatch.getTypes().stream()
                .map(s -> (BinaryString) BinaryUtil.toBinaryForString(s))
                .collect(Collectors.toSet());

            // If this head label node has generated in other branch, just reuse it and push down the startIds.
            if (vertexMatch.getInput() == null && planCache.containsKey(label)) {
                StepLogicalPlan plan = planCache.get(label);
                // push start ids to StepSourceOperator
                assert plan.getInputs().size() == 1;
                if (plan.getInputs().get(0).getOperator() instanceof StepSourceOperator) {
                    StepSourceOperator sourceOp = (StepSourceOperator) plan.getInputs().get(0).getOperator();
                    sourceOp.joinStartId(startIds);
                }
                if (vertexMatch.getTypes().size() > 0) {
                    return plan.filterNode(new StepNodeTypeFilterFunction(nodeTypes));
                }
                return plan;
            }
            IType<?> nodeType = SqlTypeUtil.convertType(vertexMatch.getNodeType());
            // generate input plan.
            StepLogicalPlan input;
            if (vertexMatch.getInput() != null) {
                input = this.visit(vertexMatch.getInput());
            } else {
                if (logicalPlanHead == null) { // create start plan for the first time
                    input = StepLogicalPlan.start(startIds)
                        .withGraphSchema(graphSchema)
                        .withModifyGraphSchema(modifyGraphSchema)
                        .withInputPathSchema(PathType.EMPTY)
                        .withOutputPathSchema(PathType.EMPTY)
                        .withOutputType(nodeType);
                    logicalPlanHead = input;
                } else { // start from the exists start plan.
                    StepLogicalPlan startPlan = logicalPlanHead;
                    assert startPlan.getOperator() instanceof StepSourceOperator :
                        "Start plan should be StepSourceOperator";
                    // push startIds of this branch to the StepSourceOperator.
                    ((StepSourceOperator) startPlan.getOperator()).unionStartId(startIds);
                    input = startPlan;
                }
            }
            PathType outputPath = (PathType) SqlTypeUtil.convertType(vertexMatch.getPathSchema());
            MatchVertexFunction mvf = new MatchVertexFunctionImpl(nodeTypes, label);
            StepLogicalPlan plan = input.vertexMatch(mvf)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(nodeType);
            planCache.put(label, plan);
            return plan;
        }

        @Override
        public StepLogicalPlan visitEdgeMatch(EdgeMatch edgeMatch) {
            String label = edgeMatch.getLabel();
            if (planCache.containsKey(label)) {
                return planCache.get(label);
            }
            if (edgeMatch.getInput() == null) {
                throw new GeaFlowDSLException("Graph match should start from a vertex");
            }
            StepLogicalPlan input = this.visit(edgeMatch.getInput());

            IType<?> nodeType = SqlTypeUtil.convertType(edgeMatch.getNodeType());
            PathType outputPath = (PathType) SqlTypeUtil.convertType(edgeMatch.getPathSchema());

            IFilter<?>[] pushDownFilter = null;
            RexNode filter = nodePushDownFilters.get(edgeMatch);
            if (filter != null) {
                // push down edge timestamp condition
                IFilter<?> tsRangeFilter = null;
                List<TimeRange> tsRanges = FilterPushDownUtil.findTsRange(filter,
                    (EdgeRecordType) edgeMatch.getNodeType()).stream().collect(Collectors.toList());
                if (!tsRanges.isEmpty()) {
                    for (TimeRange timeRange : tsRanges) {
                        if (tsRangeFilter != null) {
                            tsRangeFilter = tsRangeFilter.or(new EdgeTsFilter(timeRange));
                        } else {
                            tsRangeFilter = new EdgeTsFilter(timeRange);
                        }
                    }
                }
                if (tsRangeFilter != null) {
                    pushDownFilter = new IFilter[]{tsRangeFilter};
                }
            }
            Set<BinaryString> edgeTypes = edgeMatch.getTypes().stream()
                .map(s -> (BinaryString) BinaryUtil.toBinaryForString(s))
                .collect(Collectors.toSet());
            MatchEdgeFunction mef =
                pushDownFilter == null ? new MatchEdgeFunctionImpl(edgeMatch.getDirection(),
                    edgeTypes, label) :
                new MatchEdgeFunctionImpl(edgeMatch.getDirection(), edgeTypes,
                    label, pushDownFilter);

            StepLogicalPlan plan = input.edgeMatch(mef)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(nodeType);
            planCache.put(label, plan);
            return plan;
        }

        @Override
        public StepLogicalPlan visitVirtualEdgeMatch(VirtualEdgeMatch virtualEdgeMatch) {
            StepLogicalPlan input = this.visit(virtualEdgeMatch.getInput());
            PathRecordType inputPath = ((IMatchNode) virtualEdgeMatch.getInput()).getPathSchema();
            Expression targetId = ExpressionTranslator.of(inputPath, logicalPlanSet)
                .translate(virtualEdgeMatch.getTargetId());
            PathType outputPath = (PathType) SqlTypeUtil.convertType(virtualEdgeMatch.getPathSchema());
            MatchVirtualEdgeFunction virtualEdgeFunction = new MatchVirtualEdgeFunctionImpl(targetId);
            return input.virtualEdgeMatch(virtualEdgeFunction)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(SqlTypeUtil.convertType(virtualEdgeMatch.getNodeType()));
        }

        @Override
        public StepLogicalPlan visitFilter(MatchFilter filter) {
            // push down filter condition
            nodePushDownFilters.put(filter.getInput(), filter.getCondition());
            StepLogicalPlan input = this.visit(filter.getInput());
            PathType outputPath = (PathType) SqlTypeUtil.convertType(filter.getPathSchema());
            PathRecordType inputPath = ((IMatchNode) filter.getInput()).getPathSchema();

            Expression condition =
                ExpressionTranslator.of(inputPath, logicalPlanSet).translate(filter.getCondition());
            StepBoolFunction fn = new StepBoolFunctionImpl(condition);
            return input.filter(fn).withModifyGraphSchema(input.getModifyGraphSchema())
                .withOutputPathSchema(outputPath);
        }

        @Override
        public StepLogicalPlan visitJoin(MatchJoin join) {
            JoinInfo joinInfo = join.analyzeCondition();
            PathRecordType leftPathType = ((IMatchNode) join.getLeft()).getPathSchema();
            PathRecordType rightPathType = ((IMatchNode) join.getRight()).getPathSchema();

            IType<?>[] leftKeyTypes = joinInfo.leftKeys.stream()
                .map(index ->
                    SqlTypeUtil.convertType(leftPathType
                        .getFieldList().get(index).getType()))
                .collect(Collectors.toList())
                .toArray(new IType[]{});
            IType<?>[] rightKeyTypes = joinInfo.rightKeys.stream()
                .map(index ->
                    SqlTypeUtil.convertType(rightPathType
                        .getFieldList().get(index).getType()))
                .collect(Collectors.toList())
                .toArray(new IType[]{});

            StepKeyFunction leftKeyFn = new StepKeyFunctionImpl(toIntArray(joinInfo.leftKeys), leftKeyTypes);
            StepKeyFunction rightKeyFn = new StepKeyFunctionImpl(toIntArray(joinInfo.rightKeys), rightKeyTypes);

            StepLogicalPlan leftPlan = visit(join.getLeft());
            StepLogicalPlan rightPlan = visit(join.getRight());
            IType<?>[] leftPathTypes = leftPlan.getOutputPathSchema().getTypes();
            IType<?>[] rightPathTypes = rightPlan.getOutputPathSchema().getTypes();

            StepJoinFunction joinFunction = new StepJoinFunctionImpl(join.getJoinType(),
                leftPathTypes, rightPathTypes);

            PathType inputJoinPath = (PathType) SqlTypeUtil.convertType(leftPathType.join(rightPathType,
                join.getCluster().getTypeFactory()));
            PathType joinOutputPath = (PathType) SqlTypeUtil.convertType(join.getPathSchema());
            StepLogicalPlan joinPlan = leftPlan.join(rightPlan, leftKeyFn, rightKeyFn, joinFunction, inputJoinPath)
                .withOutputPathSchema(joinOutputPath);

            if (!joinInfo.isEqui()) {
                RexNode nonEqFilter = joinInfo.getRemaining(join.getCluster().getRexBuilder());
                Expression nonEqFilterExp = ExpressionTranslator.of(join.getPathSchema()).translate(nonEqFilter);

                StepBoolFunction filterFn = new StepBoolFunctionImpl(nonEqFilterExp);
                return joinPlan.filter(filterFn).withOutputPathSchema(joinOutputPath);
            }
            return joinPlan;
        }

        @Override
        public StepLogicalPlan visitDistinct(MatchDistinct distinct) {
            RelNode input = distinct.getInput(0);
            IType<?>[] types = ((IMatchNode) input).getPathSchema().getFieldList().stream()
                .map(field -> SqlTypeUtil.convertType(field.getType()))
                .collect(Collectors.toList()).toArray(new IType[]{});
            int[] keyIndices = new int[types.length];
            for (int i = 0, size = types.length; i < size; i++) {
                keyIndices[i] = i;
            }
            StepKeyFunction keyFunction = new StepKeyFunctionImpl(keyIndices, types);
            PathType distinctPathType = (PathType) SqlTypeUtil.convertType(distinct.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(distinct.getNodeType());
            return visit(input).distinct(keyFunction)
                .withOutputPathSchema(distinctPathType)
                .withOutputType(nodeType);
        }

        @Override
        public StepLogicalPlan visitUnion(MatchUnion union) {
            List<StepLogicalPlan> inputPlans = new ArrayList<>();

            for (int i = 0, size = union.getInputs().size(); i < size; i++) {
                // The input of union should not referer the plan cache generated by each other.
                // So we create a new plan cache for each input.
                Map<String, StepLogicalPlan> prePlanCache = planCache;
                planCache = new HashMap<>(planCache);
                inputPlans.add(visit(union.getInput(i)));
                // recover pre-plan cache.
                planCache = prePlanCache;
            }

            StepLogicalPlan firstPlan = inputPlans.get(0);
            PathType unionPathType = (PathType) SqlTypeUtil.convertType(union.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(union.getNodeType());

            StepLogicalPlan unionPlan = firstPlan.union(inputPlans.subList(1, inputPlans.size()))
                .withModifyGraphSchema(firstPlan.getModifyGraphSchema())
                .withOutputPathSchema(unionPathType)
                .withOutputType(nodeType);
            if (union.all) {
                return unionPlan;
            } else {
                IType<?>[] types = unionPlan.getOutputPathSchema().getFields().stream()
                    .map(TableField::getType)
                    .collect(Collectors.toList()).toArray(new IType[]{});
                int[] keyIndices = new int[types.length];
                for (int i = 0, size = types.length; i < size; i++) {
                    keyIndices[i] = i;
                }
                StepKeyFunction keyFunction = new StepKeyFunctionImpl(keyIndices, types);
                return unionPlan.distinct(keyFunction)
                    .withModifyGraphSchema(unionPlan.getModifyGraphSchema())
                    .withOutputPathSchema(unionPlan.getOutputPathSchema())
                    .withOutputType(unionPlan.getOutputType());
            }
        }

        @Override
        public StepLogicalPlan visitLoopMatch(LoopUntilMatch loopMatch) {
            StepLogicalPlan loopStart = visit(loopMatch.getInput());
            StepLogicalPlan loopBody = visit(loopMatch.getLoopBody());
            for (StepLogicalPlan plan : loopBody.getFinalPlans()) {
                plan.withModifyGraphSchema(loopStart.getModifyGraphSchema());
            }
            ExpressionTranslator translator = ExpressionTranslator.of(loopMatch.getLoopBody().getPathSchema());
            Expression utilCondition = translator.translate(loopMatch.getUtilCondition());

            PathType outputPath = (PathType) SqlTypeUtil.convertType(loopMatch.getPathSchema());
            IType<?> nodeType = SqlTypeUtil.convertType(loopMatch.getNodeType());
            int loopStartPathFieldCount = loopStart.getOutputPathSchema().size();
            int loopBodyPathFieldCount = loopBody.getOutputPathSchema().size() - loopStartPathFieldCount;
            return loopStart.loopUtil(loopBody, new StepBoolFunctionImpl(utilCondition),
                    loopMatch.getMinLoopCount(), loopMatch.getMaxLoopCount(),
                    loopStartPathFieldCount, loopBodyPathFieldCount)
                .withModifyGraphSchema(loopStart.getModifyGraphSchema())
                .withOutputPathSchema(outputPath)
                .withOutputType(nodeType)
                ;
        }

        @Override
        public StepLogicalPlan visitSubQueryStart(SubQueryStart subQueryStart) {
            PathType pathType = (PathType) SqlTypeUtil.convertType(subQueryStart.getPathSchema());

            return StepLogicalPlan.subQueryStart(subQueryStart.getQueryName())
                .withGraphSchema(graphSchema)
                .withInputPathSchema(pathType)
                .withOutputPathSchema(pathType)
                .withOutputType(SqlTypeUtil.convertType(subQueryStart.getNodeType()));
        }

        @Override
        public StepLogicalPlan visitPathModify(MatchPathModify pathModify) {
            StepLogicalPlan input = visit(pathModify.getInput());
            List<PathModifyExpression> modifyExpressions = pathModify.getExpressions();
            int[] updatePathIndices = new int[modifyExpressions.size()];
            Expression[] updateExpressions = new Expression[modifyExpressions.size()];

            ExpressionTranslator translator = ExpressionTranslator.of(pathModify.getInput().getRowType(),
                logicalPlanSet);
            for (int i = 0; i < modifyExpressions.size(); i++) {
                PathModifyExpression modifyExpression = modifyExpressions.get(i);
                updatePathIndices[i] = modifyExpression.getIndex();
                updateExpressions[i] = translator.translate(modifyExpression.getObjectConstruct());
            }
            IType<?>[] inputFieldTypes = input.getOutputPathSchema().getFields()
                .stream()
                .map(TableField::getType)
                .collect(Collectors.toList())
                .toArray(new IType[]{});
            GraphSchema modifyGraphSchema = (GraphSchema) SqlTypeUtil.convertType(pathModify.getModifyGraphType());
            StepPathModifyFunction modifyFunction = new StepPathModifyFunction(updatePathIndices,
                updateExpressions, inputFieldTypes);
            boolean isGlobal = pathModify.getExpressions().stream().anyMatch(exp -> {
                return exp.getObjectConstruct().getVariableInfo().stream().anyMatch(VariableInfo::isGlobal);
            });
            return input.map(modifyFunction, isGlobal)
                .withGraphSchema(graphSchema)
                .withModifyGraphSchema(modifyGraphSchema)
                .withInputPathSchema(input.getOutputPathSchema())
                .withOutputPathSchema((PathType) SqlTypeUtil.convertType(pathModify.getRowType()))
                .withOutputType(input.getOutputType());
        }

        @Override
        public StepLogicalPlan visitSort(MatchPathSort pathSort) {
            StepLogicalPlan input = visit(pathSort.getInput());
            SortInfo sortInfo = buildSortInfo(pathSort);
            StepSortFunction orderByFunction = new StepSortFunctionImpl(sortInfo);
            PathType inputPath = input.getOutputPathSchema();
            return input.sort(orderByFunction)
                .withModifyGraphSchema(input.getModifyGraphSchema())
                .withInputPathSchema(inputPath)
                .withOutputPathSchema(inputPath).withOutputType(inputPath);
        }

        private SortInfo buildSortInfo(PathSort sort) {
            SortInfo sortInfo = new SortInfo();
            ExpressionTranslator translator = ExpressionTranslator.of(sort.getRowType());
            for (RexNode fd : sort.getOrderByExpressions()) {
                OrderByField orderByField = new OrderByField();
                if (fd.getKind() == DESCENDING) {
                    orderByField.order = ORDER.DESC;
                } else {
                    orderByField.order = ORDER.ASC;
                }
                orderByField.expression = translator.translate(fd);
                sortInfo.orderByFields.add(orderByField);
            }
            sortInfo.fetch = sort.getLimit() == null ? -1 :
                             (int) TypeCastUtil.cast(
                                 translator.translate(sort.getLimit()).evaluate(null),
                                 Integer.class);
            return sortInfo;
        }
    }

    private static Set<StartId> toStartIds(Set<RexNode> ids) {
        return ids.stream()
            .map(StepLogicalPlanTranslator::toStartId)
            .collect(Collectors.toSet());
    }

    private static StartId toStartId(RexNode id) {
        List<RexNode> nonLiteralLeafNodes = GQLRexUtil.collect(id,
            child -> !(child instanceof RexCall) && !(child instanceof RexLiteral));

        Expression expression = ExpressionTranslator.of(null).translate(id);
        if (nonLiteralLeafNodes.isEmpty()) { // all the leaf node is constant.
            Object constantValue = expression.evaluate(null);
            return new ConstantStartId(constantValue);
        } else {
            Expression idExpression = expression.replace(exp -> {
                if (exp instanceof ParameterFieldExpression) {
                    ParameterFieldExpression field = (ParameterFieldExpression) exp;
                    return new FieldExpression(field.getFieldIndex(), field.getOutputType());
                }
                return exp;
            });
            return new ParameterStartId(idExpression);
        }
    }
}
