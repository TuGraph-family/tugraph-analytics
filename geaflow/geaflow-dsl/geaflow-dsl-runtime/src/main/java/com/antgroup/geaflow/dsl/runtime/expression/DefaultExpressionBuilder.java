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
import com.antgroup.geaflow.dsl.common.function.UDF;
import com.antgroup.geaflow.dsl.common.function.UDTF;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.expression.binary.DivideExpression;
import com.antgroup.geaflow.dsl.runtime.expression.binary.MinusExpression;
import com.antgroup.geaflow.dsl.runtime.expression.binary.MinusPrefixExpression;
import com.antgroup.geaflow.dsl.runtime.expression.binary.ModExpression;
import com.antgroup.geaflow.dsl.runtime.expression.binary.MultiplyExpression;
import com.antgroup.geaflow.dsl.runtime.expression.binary.PlusExpression;
import com.antgroup.geaflow.dsl.runtime.expression.cast.CastExpression;
import com.antgroup.geaflow.dsl.runtime.expression.condition.CaseExpression;
import com.antgroup.geaflow.dsl.runtime.expression.condition.IfExpression;
import com.antgroup.geaflow.dsl.runtime.expression.construct.EdgeConstructExpression;
import com.antgroup.geaflow.dsl.runtime.expression.construct.VertexConstructExpression;
import com.antgroup.geaflow.dsl.runtime.expression.field.FieldExpression;
import com.antgroup.geaflow.dsl.runtime.expression.field.ParameterFieldExpression;
import com.antgroup.geaflow.dsl.runtime.expression.field.PathFieldExpression;
import com.antgroup.geaflow.dsl.runtime.expression.item.ItemExpression;
import com.antgroup.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import com.antgroup.geaflow.dsl.runtime.expression.literal.PIExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.AndExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.EqualExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.GTEExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.GTExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.IsFalseExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.IsNotFalseExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.IsNotNullExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.IsNotTrueExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.IsNullExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.IsTrueExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.LTEExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.LTExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.NotExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.OrExpression;
import com.antgroup.geaflow.dsl.runtime.traversal.data.GlobalVariable;
import com.antgroup.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions;
import java.util.List;

public class DefaultExpressionBuilder implements ExpressionBuilder {

    public DefaultExpressionBuilder() {

    }

    @Override
    public Expression plus(Expression left, Expression right, IType<?> outputType) {
        return new PlusExpression(left, right, outputType);
    }

    @Override
    public Expression minus(Expression left, Expression right, IType<?> outputType) {
        return new MinusExpression(left, right, outputType);
    }

    @Override
    public Expression multiply(Expression left, Expression right, IType<?> outputType) {
        return new MultiplyExpression(left, right, outputType);
    }

    @Override
    public Expression divide(Expression left, Expression right, IType<?> outputType) {
        return new DivideExpression(left, right, outputType);
    }

    @Override
    public Expression mod(Expression left, Expression right, IType<?> outputType) {
        return new ModExpression(left, right, outputType);
    }

    @Override
    public Expression minusPrefix(Expression input, IType<?> outputType) {
        return new MinusPrefixExpression(input, outputType);
    }

    @Override
    public Expression cast(Expression input, IType<?> outputType) {
        return new CastExpression(input, outputType);
    }

    @Override
    public Expression isNull(Expression input) {
        return new IsNullExpression(input);
    }

    @Override
    public Expression isNotNull(Expression input) {
        return new IsNotNullExpression(input);
    }

    @Override
    public Expression caseWhen(List<Expression> inputs, IType<?> outputType) {
        return new CaseExpression(inputs, outputType);
    }

    @Override
    public Expression ifExp(Expression condition, Expression trueValue, Expression falseValue, IType<?> outputType) {
        return new IfExpression(condition, trueValue, falseValue, outputType);
    }

    @Override
    public Expression field(Expression input, int fieldIndex, IType<?> outputType) {
        return new FieldExpression(input, fieldIndex, outputType);
    }

    @Override
    public Expression pathField(String label, int fieldIndex, IType<?> outputType) {
        return new PathFieldExpression(label, fieldIndex, outputType);
    }

    @Override
    public Expression parameterField(int fieldIndex, IType<?> outputType) {
        return new ParameterFieldExpression(fieldIndex, outputType);
    }

    @Override
    public Expression item(Expression input, Expression index) {
        return new ItemExpression(input, index);
    }

    @Override
    public Expression literal(Object value, IType<?> outputType) {
        return new LiteralExpression(value, outputType);
    }

    @Override
    public Expression pi() {
        return new PIExpression();
    }

    @Override
    public Expression and(List<Expression> inputs) {
        return new AndExpression(inputs);
    }

    @Override
    public Expression or(List<Expression> inputs) {
        return new OrExpression(inputs);
    }

    @Override
    public Expression isFalse(Expression input) {
        return new IsFalseExpression(input);
    }

    @Override
    public Expression isNotFalse(Expression input) {
        return new IsNotFalseExpression(input);
    }

    @Override
    public Expression isTrue(Expression input) {
        return new IsTrueExpression(input);
    }

    @Override
    public Expression isNotTrue(Expression input) {
        return new IsNotTrueExpression(input);
    }

    @Override
    public Expression greaterThan(Expression left, Expression right) {
        return new GTExpression(left, right);
    }

    @Override
    public Expression greaterEqThen(Expression left, Expression right) {
        return new GTEExpression(left, right);
    }

    @Override
    public Expression lessThan(Expression left, Expression right) {
        return new LTExpression(left, right);
    }

    @Override
    public Expression lessEqThan(Expression left, Expression right) {
        return new LTEExpression(left, right);
    }

    @Override
    public Expression equal(Expression left, Expression right) {
        return new EqualExpression(left, right);
    }

    @Override
    public Expression not(Expression input) {
        return new NotExpression(input);
    }

    @Override
    public Expression vertexConstruct(List<Expression> inputs, List<GlobalVariable> globalVariables,
                                      VertexType vertexType) {
        return new VertexConstructExpression(inputs, globalVariables, vertexType);
    }

    @Override
    public Expression edgeConstruct(List<Expression> inputs, EdgeType edgeType) {
        return new EdgeConstructExpression(inputs, edgeType);
    }

    @Override
    public Expression udf(List<Expression> inputs, IType<?> outputType, Class<? extends UDF> implementClass) {
        return new UDFExpression(inputs, outputType, implementClass);
    }

    @Override
    public Expression udtf(List<Expression> inputs, IType<?> outputType, Class<? extends UDTF> implementClass) {
        return new UDTFExpression(inputs, outputType, implementClass);
    }

    @Override
    public Expression buildIn(List<Expression> inputs, IType<?> outputType, String methodName) {
        return new BuildInExpression(inputs, outputType, GeaFlowBuiltinFunctions.class, methodName);
    }
}
