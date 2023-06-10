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
import com.antgroup.geaflow.dsl.runtime.traversal.data.GlobalVariable;
import java.util.List;

public interface ExpressionBuilder {

    Expression plus(Expression left, Expression right, IType<?> outputType);

    Expression minus(Expression left, Expression right, IType<?> outputType);

    Expression multiply(Expression left, Expression right, IType<?> outputType);

    Expression divide(Expression left, Expression right, IType<?> outputType);

    Expression mod(Expression left, Expression right, IType<?> outputType);

    Expression minusPrefix(Expression input, IType<?> outputType);

    Expression cast(Expression input, IType<?> outputType);

    Expression isNull(Expression input);

    Expression isNotNull(Expression input);

    Expression caseWhen(List<Expression> inputs, IType<?> outputType);

    Expression ifExp(Expression condition, Expression trueValue, Expression falseValue, IType<?> outputType);

    Expression field(Expression input, int fieldIndex, IType<?> outputType);

    Expression pathField(String label, int fieldIndex, IType<?> outputType);

    Expression parameterField(int fieldIndex, IType<?> outputType);

    Expression item(Expression input, Expression index);

    Expression literal(Object value, IType<?> outputType);

    Expression pi();

    Expression and(List<Expression> inputs);

    Expression or(List<Expression> inputs);

    Expression isFalse(Expression input);

    Expression isNotFalse(Expression input);

    Expression isTrue(Expression input);

    Expression isNotTrue(Expression input);

    Expression greaterThan(Expression left, Expression right);

    Expression greaterEqThen(Expression left, Expression right);

    Expression lessThan(Expression left, Expression right);

    Expression lessEqThan(Expression left, Expression right);

    Expression equal(Expression left, Expression right);

    Expression not(Expression input);

    Expression vertexConstruct(List<Expression> inputs, List<GlobalVariable> globalVariables,
                               VertexType vertexType);

    Expression edgeConstruct(List<Expression> inputs, EdgeType edgeType);

    Expression udf(List<Expression> inputs, IType<?> outputType, Class<? extends UDF> implementClass);

    Expression udtf(List<Expression> inputs, IType<?> outputType, Class<? extends UDTF> implementClass);

    Expression buildIn(List<Expression> inputs, IType<?> outputType, String methodName);
}
