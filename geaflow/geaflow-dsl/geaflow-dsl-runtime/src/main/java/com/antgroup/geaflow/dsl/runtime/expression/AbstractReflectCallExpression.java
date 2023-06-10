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
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.util.FunctionCallUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractReflectCallExpression extends AbstractNonLeafExpression {

    protected final Class<?> implementClass;

    protected final String methodName;

    private transient Method method = null;

    protected transient Object implementInstance = null;

    public AbstractReflectCallExpression(List<Expression> inputs, IType outputType,
                                         Class<?> implementClass, String methodName) {
        super(inputs, outputType);
        this.implementClass = Objects.requireNonNull(implementClass);
        this.methodName = Objects.requireNonNull(methodName);
    }

    @Override
    public Object evaluate(Row row) {
        Object[] paramValues = new Object[inputs.size()];
        for (int i = 0; i < inputs.size(); i++) {
            paramValues[i] = inputs.get(i).evaluate(row);
        }
        try {
            initMethod();
            return FunctionCallUtils.callMethod(method, implementInstance, paramValues);
        } catch (Exception e) {
            String msg = "Error in call " + methodName + ","
                + " params is (" + StringUtils.join(paramValues, ", ") + ")";
            throw new RuntimeException(msg, e);
        }
    }

    private void initMethod() {
        try {
            if (method == null) {
                method = FunctionCallUtils.findMatchMethod(implementClass,
                    methodName, inputTypes);
                if (!Modifier.isStatic(method.getModifiers())) {
                    implementInstance = implementClass.newInstance();
                }
            }
        } catch (Exception e) {
            throw new GeaFlowDSLException(e);
        }
    }
}
