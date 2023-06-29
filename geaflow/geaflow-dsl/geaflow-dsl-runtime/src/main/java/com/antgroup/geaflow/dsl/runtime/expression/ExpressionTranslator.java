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

package com.antgroup.geaflow.dsl.runtime.expression;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.utils.ArrayUtil;
import com.antgroup.geaflow.common.utils.ClassUtil;
import com.antgroup.geaflow.dsl.calcite.GraphRecordType;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.function.UDAF;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.GraphScan;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphScan;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.SingleMatchNode;
import com.antgroup.geaflow.dsl.rex.PathInputRef;
import com.antgroup.geaflow.dsl.rex.RexLambdaCall;
import com.antgroup.geaflow.dsl.rex.RexObjectConstruct;
import com.antgroup.geaflow.dsl.rex.RexObjectConstruct.VariableInfo;
import com.antgroup.geaflow.dsl.rex.RexParameterRef;
import com.antgroup.geaflow.dsl.rex.RexSystemVariable;
import com.antgroup.geaflow.dsl.rex.RexSystemVariable.SystemVariable;
import com.antgroup.geaflow.dsl.runtime.expression.field.SystemVariableExpression;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallQueryExpression;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepAggFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepSingleValueMapFunction;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicAggregateRelNode;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlan;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlanSet;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlanTranslator;
import com.antgroup.geaflow.dsl.runtime.traversal.data.GlobalVariable;
import com.antgroup.geaflow.dsl.runtime.traversal.data.SingleValue;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSubQueryStartOperator;
import com.antgroup.geaflow.dsl.schema.function.GeaFlowUserDefinedScalarFunction;
import com.antgroup.geaflow.dsl.schema.function.GeaFlowUserDefinedTableFunction;
import com.antgroup.geaflow.dsl.udf.table.string.Like;
import com.antgroup.geaflow.dsl.util.GQLRexUtil;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;

public class ExpressionTranslator implements RexVisitor<Expression> {

    private final ExpressionBuilder builder;

    private final RelDataType inputType;

    private final StepLogicalPlanSet logicalPlanSet;

    private ExpressionTranslator(RelDataType inputType) {
        this(inputType, null);
    }

    private ExpressionTranslator(RelDataType inputType, StepLogicalPlanSet logicalPlanSet) {
        this.inputType = inputType;
        this.logicalPlanSet = logicalPlanSet;
        this.builder = new DefaultExpressionBuilder();
    }

    public static ExpressionTranslator of(RelDataType inputType) {
        return new ExpressionTranslator(inputType);
    }

    public static ExpressionTranslator of(RelDataType inputType, StepLogicalPlanSet logicalPlanSet) {
        return new ExpressionTranslator(inputType, logicalPlanSet);
    }

    public Expression translate(RexNode exp) {
        return exp.accept(this);
    }

    public List<Expression> translate(List<RexNode> rexNodes) {
        return rexNodes.stream()
            .map(exp -> exp.accept(this))
            .collect(Collectors.toList());
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        IType<?> type = SqlTypeUtil.convertType(inputRef.getType());
        if (inputRef instanceof PathInputRef) {
            PathInputRef pathInputRef = (PathInputRef) inputRef;
            return builder.pathField(pathInputRef.getLabel(), index, type);
        }
        return builder.field(null, index, type);
    }

    @Override
    public Expression visitLocalRef(RexLocalRef localRef) {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        Object value = GQLRexUtil.getLiteralValue(literal);
        IType<?> type = SqlTypeUtil.convertType(literal.getType());
        return builder.literal(value, type);
    }

    @Override
    public Expression visitOver(RexOver over) {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public Expression visitCall(RexCall call) {
        if (isPathPatternSubQuery(call)) {
            return processPathPatternSubQuery(call);
        }
        List<RexNode> operands = call.getOperands();
        List<Expression> inputs = Lists.newArrayList();
        for (RexNode operand : operands) {
            Expression input = operand.accept(this);
            inputs.add(input);
        }

        SqlSyntax syntax = call.getOperator().getSyntax();
        SqlKind callKind = call.getKind();
        IType<?> outputType = SqlTypeUtil.convertType(call.getType());
        switch (syntax) {
            case BINARY:
                switch (callKind) {
                    case PLUS:
                        assert inputs.size() == 2;
                        return builder.plus(inputs.get(0), inputs.get(1), outputType);
                    case MINUS:
                        assert inputs.size() == 2;
                        return builder.minus(inputs.get(0), inputs.get(1), outputType);
                    case TIMES:
                        assert inputs.size() == 2;
                        return builder.multiply(inputs.get(0), inputs.get(1), outputType);
                    case DIVIDE:
                        assert inputs.size() == 2;
                        return builder.divide(inputs.get(0), inputs.get(1), outputType);
                    case AND:
                        return builder.and(inputs);
                    case OR:
                        return builder.or(inputs);
                    case LESS_THAN:
                        assert inputs.size() == 2;
                        return builder.lessThan(inputs.get(0), inputs.get(1));
                    case LESS_THAN_OR_EQUAL:
                        assert inputs.size() == 2;
                        return builder.lessEqThan(inputs.get(0), inputs.get(1));
                    case EQUALS:
                        assert inputs.size() == 2;
                        return builder.equal(inputs.get(0), inputs.get(1));
                    case NOT_EQUALS:
                        assert inputs.size() == 2;
                        return builder.not(builder.equal(inputs.get(0), inputs.get(1)));
                    case GREATER_THAN:
                        assert inputs.size() == 2;
                        return builder.greaterThan(inputs.get(0), inputs.get(1));
                    case GREATER_THAN_OR_EQUAL:
                        assert inputs.size() == 2;
                        return builder.greaterEqThen(inputs.get(0), inputs.get(1));
                    case IS_NULL:
                        assert inputs.size() == 1;
                        return builder.isNull(inputs.get(0));
                    case OTHER:
                        return processOtherTrans(inputs, call);
                    default:
                        break;
                }
                break;
            case FUNCTION:
            case FUNCTION_ID:
                switch (callKind) {
                    case FLOOR:
                        if (call.getOperands().size() == 1) {
                            return builder.buildIn(inputs, outputType, BuildInExpression.FLOOR);
                        } else if (call.getOperands().size() == 2) {
                            return builder.buildIn(inputs, outputType, BuildInExpression.TIMESTAMP_FLOOR);
                        }
                        break;
                    case CEIL:
                        if (call.getOperands().size() == 1) {
                            return builder.buildIn(inputs, outputType, BuildInExpression.CEIL);
                        } else if (call.getOperands().size() == 2) {
                            return builder.buildIn(inputs, outputType, BuildInExpression.TIMESTAMP_CEIL);
                        }
                        break;
                    case TRIM:
                        return builder.buildIn(inputs, outputType, BuildInExpression.TRIM);
                    case OTHER_FUNCTION:
                        return processOtherTrans(inputs, call);
                    default:
                        break;
                }
                break;
            case SPECIAL:
                switch (callKind) {
                    case CAST:
                    case REINTERPRET:
                        assert inputs.size() == 1;
                        return builder.cast(inputs.get(0), outputType);
                    case CASE:
                        return builder.caseWhen(inputs, outputType);
                    case LIKE:
                        return builder.udf(inputs, outputType, Like.class);
                    case SIMILAR:
                        return builder.buildIn(inputs, outputType, BuildInExpression.SIMILAR);
                    case VERTEX_VALUE_CONSTRUCTOR:
                        RexObjectConstruct objConstruct = (RexObjectConstruct) call;
                        List<VariableInfo> variableInfoList = objConstruct.getVariableInfo();
                        List<GlobalVariable> globalVariables = new ArrayList<>();

                        for (int i = 0; i < objConstruct.getOperands().size(); i++) {
                            VariableInfo variableInfo = variableInfoList.get(i);
                            if (variableInfo.isGlobal()) {
                                RexNode operand = objConstruct.getOperands().get(i);
                                IType<?> type = SqlTypeUtil.convertType(operand.getType());
                                globalVariables.add(new GlobalVariable(variableInfo.getName(), i, type));
                            }
                        }
                        return builder.vertexConstruct(inputs, globalVariables, (VertexType) outputType);
                    case EDGE_VALUE_CONSTRUCTOR:
                        return builder.edgeConstruct(inputs, (EdgeType) outputType);
                    default:
                        break;
                }
                break;
            case POSTFIX:
                switch (callKind) {
                    case IS_NULL:
                        assert inputs.size() == 1;
                        return builder.isNull(inputs.get(0));
                    case IS_NOT_NULL:
                        assert inputs.size() == 1;
                        return builder.isNotNull(inputs.get(0));
                    case IS_FALSE:
                        assert inputs.size() == 1;
                        return builder.isFalse(inputs.get(0));
                    case IS_NOT_FALSE:
                        assert inputs.size() == 1;
                        return builder.isNotFalse(inputs.get(0));
                    case IS_TRUE:
                        assert inputs.size() == 1;
                        return builder.isTrue(inputs.get(0));
                    case IS_NOT_TRUE:
                        assert inputs.size() == 1;
                        return builder.isNotTrue(inputs.get(0));
                    case DESCENDING:
                        return call.operands.get(0).accept(this);
                    default:
                        break;
                }
                break;
            case PREFIX:
                switch (callKind) {
                    case NOT:
                        assert inputs.size() == 1;
                        return builder.not(inputs.get(0));
                    case MINUS_PREFIX:
                        assert inputs.size() == 1;
                        return builder.minusPrefix(inputs.get(0), outputType);
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return processOtherTrans(inputs, call);
    }

    private Expression processOtherTrans(List<Expression> inputs, RexCall call) {
        SqlOperator sqlOperator = call.getOperator();
        // Upper operator name.
        String operatorName = sqlOperator.getName().toUpperCase();
        String functionName = null;
        IType<?> outputType = SqlTypeUtil.convertType(call.getType());

        switch (operatorName) {
            case "NULL":
                return builder.literal(null, outputType);
            case "IF":
                assert inputs.size() == 3;
                return builder.ifExp(inputs.get(0), inputs.get(1), inputs.get(2), outputType);
            case "ITEM":
                assert inputs.size() == 2;
                return builder.item(inputs.get(0), inputs.get(1));
            case "%":
            case "MOD":
                return builder.mod(inputs.get(0), inputs.get(1), outputType);
            case "PI":
                return builder.pi();
            case "||":
                functionName = BuildInExpression.CONCAT;
                break;
            case "CHAR_LENGTH":
            case "CHARACTER_LENGTH":
                functionName = BuildInExpression.LENGTH;
                break;
            case "UPPER":
                functionName = BuildInExpression.UPPER;
                break;
            case "LOWER":
                functionName = BuildInExpression.LOWER;
                break;
            case "POSITION":
                functionName = BuildInExpression.POSITION;
                break;
            case "OVERLAY":
                functionName = BuildInExpression.OVERLAY;
                break;
            case "SUBSTRING":
                functionName = BuildInExpression.SUBSTRING;
                break;
            case "INITCAP":
                functionName = BuildInExpression.INITCAP;
                break;
            case "POWER":
                functionName = BuildInExpression.POWER;
                break;
            case "ABS":
                functionName = BuildInExpression.ABS;
                break;
            case "LN":
                functionName = BuildInExpression.LN;
                break;
            case "LOG10":
                functionName = BuildInExpression.LOG10;
                break;
            case "EXP":
                functionName = BuildInExpression.EXP;
                break;
            case "SIN":
                functionName = BuildInExpression.SIN;
                break;
            case "COS":
                functionName = BuildInExpression.COS;
                break;
            case "TAN":
                functionName = BuildInExpression.TAN;
                break;
            case "COT":
                functionName = BuildInExpression.COT;
                break;
            case "ASIN":
                functionName = BuildInExpression.ASIN;
                break;
            case "ACOS":
                functionName = BuildInExpression.ACOS;
                break;
            case "ATAN":
                functionName = BuildInExpression.ATAN;
                break;
            case "DEGREES":
                functionName = BuildInExpression.DEGREES;
                break;
            case "RADIANS":
                functionName = BuildInExpression.RADIANS;
                break;
            case "SIGN":
                functionName = BuildInExpression.SIGN;
                break;
            case "RAND":
                functionName = BuildInExpression.RAND;
                break;
            case "RAND_INTEGER":
                functionName = BuildInExpression.RAND_INTEGER;
                break;
            case "LOCALTIMESTAMP":
            case "CURRENT_TIMESTAMP":
                functionName = BuildInExpression.CURRENT_TIMESTAMP;
                break;
            default:
        }
        if (functionName != null) {
            return builder.buildIn(inputs, outputType, functionName);
        }

        if (call.getOperator() instanceof GeaFlowUserDefinedTableFunction) {
            GeaFlowUserDefinedTableFunction operator = (GeaFlowUserDefinedTableFunction) call.getOperator();
            return builder.udtf(inputs, outputType, operator.getImplementClass());
        } else if (call.getOperator() instanceof GeaFlowUserDefinedScalarFunction) {
            GeaFlowUserDefinedScalarFunction operator = (GeaFlowUserDefinedScalarFunction) call.getOperator();
            return builder.udf(inputs, outputType, operator.getImplementClass());
        }
        throw new GeaFlowDSLException("Not support expression: " + call);
    }

    @SuppressWarnings("unchecked")
    private Expression processPathPatternSubQuery(RexCall call) {
        RexLambdaCall lambdaCall = (RexLambdaCall) call.operands.get(0);
        SingleMatchNode matchNode = (SingleMatchNode) (lambdaCall.getInput()).rel;

        // generate sub query logical plan.
        assert logicalPlanSet != null;
        StepLogicalPlanTranslator planTranslator = new StepLogicalPlanTranslator();
        GraphSchema graphSchema = logicalPlanSet.getGraphSchema();
        GraphRecordType graphRecordType = (GraphRecordType) SqlTypeUtil.convertToRelType(graphSchema, false,
            matchNode.getCluster().getTypeFactory());
        GraphScan emptyScan = LogicalGraphScan.emptyScan(matchNode.getCluster(), graphRecordType);
        GraphMatch graphMatch = LogicalGraphMatch.create(matchNode.getCluster(), emptyScan, matchNode,
            matchNode.getPathSchema());
        StepLogicalPlan matchPlan = planTranslator.translate(graphMatch, logicalPlanSet);

        SqlAggFunction aggFunction = (SqlAggFunction) call.getOperator();
        IType<?> aggInputType = SqlTypeUtil.convertType(lambdaCall.type);
        Class<? extends UDAF<?, ?, ?>> udafClass = PhysicAggregateRelNode.findUDAF(
            aggFunction, new IType[]{aggInputType});
        UDAF<Object, Object, Object> udaf = (UDAF<Object, Object, Object>) ClassUtil.newInstance(udafClass);
        StepAggregateFunction stepAggFunction = new StepAggFunctionImpl(udaf, aggInputType);

        Expression valueExpression = lambdaCall.getValue().accept(this);
        IType<?> valueType = valueExpression.getOutputType();
        StructType singleValueType = StructType.singleValue(valueType, lambdaCall.getValue().getType().isNullable());
        StepLogicalPlan valuePlan = matchPlan.mapRow(new StepSingleValueMapFunction(valueExpression))
            .withOutputPathSchema(PathType.EMPTY)
            .withOutputType(singleValueType);

        IType<?> aggOutputType = SqlTypeUtil.convertType(call.getType());
        StepLogicalPlan aggPlan = valuePlan.aggregate(stepAggFunction).withOutputType(aggOutputType);
        StepLogicalPlan returnPlan = aggPlan.ret();
        // add sub query plan to plan set.
        logicalPlanSet.addSubLogicalPlan(returnPlan);

        // create call sub query expression.
        RelDataTypeField startField = inputType.getFieldList().get(inputType.getFieldCount() - 1);
        int startVertexIndex = startField.getIndex();
        VertexType startVertexType = (VertexType) SqlTypeUtil.convertType(startField.getType());
        assert matchPlan.getHeadPlan().getOperator() instanceof StepSubQueryStartOperator;
        StepSubQueryStartOperator startOperator = (StepSubQueryStartOperator) matchPlan.getHeadPlan().getOperator();

        List<String> subQueryRefPathFields = startOperator.getOutputPathSchema().getFieldNames();
        List<String> inputPathFields = inputType.getFieldNames();
        List<Integer> refParentPathIndices = new ArrayList<>();
        for (int i = 0; i < inputPathFields.size(); i++) {
            if (subQueryRefPathFields.contains(inputPathFields.get(i))) {
                refParentPathIndices.add(i);
            }
        }
        Object accumulator = stepAggFunction.createAccumulator();
        SingleValue defaultAggValue = stepAggFunction.getValue(accumulator);
        return new CallQueryExpression(startOperator.getQueryName(),
            startOperator.getId(),
            startVertexIndex,
            startVertexType,
            aggOutputType,
            ArrayUtil.toIntArray(refParentPathIndices),
            defaultAggValue.getValue(aggOutputType));
    }

    private boolean isPathPatternSubQuery(RexCall call) {
        return call.getOperator().isAggregator()
            && call.operands.size() == 1
            && call.operands.get(0) instanceof RexLambdaCall
            && (((RexLambdaCall) call.operands.get(0)).getInput()).rel instanceof IMatchNode
            ;
    }

    @Override
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
    }

    @Override
    public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new GeaFlowDSLException("Not support expression: " + dynamicParam);
    }

    @Override
    public Expression visitRangeRef(RexRangeRef rangeRef) {
        throw new GeaFlowDSLException("Not support expression: " + rangeRef);
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
        Expression input = fieldAccess.getReferenceExpr().accept(this);
        int index = fieldAccess.getField().getIndex();
        IType type = SqlTypeUtil.convertType(fieldAccess.getField().getType());

        return builder.field(input, index, type);
    }

    @Override
    public Expression visitSubQuery(RexSubQuery subQuery) {
        throw new GeaFlowDSLException("Not support expression: " + subQuery);
    }

    @Override
    public Expression visitTableInputRef(RexTableInputRef fieldRef) {
        throw new GeaFlowDSLException("Not support expression: " + fieldRef);
    }

    @Override
    public Expression visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
        throw new GeaFlowDSLException("Not support expression: " + rexPatternFieldRef);
    }

    @Override
    public Expression visitOther(RexNode other) {
        if (other instanceof RexParameterRef) {
            RexParameterRef rexParameterRef = (RexParameterRef) other;
            IType<?> outputType = SqlTypeUtil.convertType(rexParameterRef.getType());
            return builder.parameterField(rexParameterRef.getIndex(), outputType);
        } else if (other instanceof RexSystemVariable) {
            RexSystemVariable systemVariable = (RexSystemVariable) other;
            return new SystemVariableExpression(SystemVariable.of(systemVariable.getName()));
        }
        throw new GeaFlowDSLException("Not support expression: " + other);
    }
}
