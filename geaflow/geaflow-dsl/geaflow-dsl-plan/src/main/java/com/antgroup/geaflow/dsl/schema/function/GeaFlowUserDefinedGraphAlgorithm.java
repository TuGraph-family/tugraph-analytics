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

package com.antgroup.geaflow.dsl.schema.function;

import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.util.FunctionUtil;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

public class GeaFlowUserDefinedGraphAlgorithm extends SqlUserDefinedFunction {

    private final Class<? extends AlgorithmUserFunction> implementClass;

    private GeaFlowUserDefinedGraphAlgorithm(String name, Class<? extends AlgorithmUserFunction> clazz,
                                             GQLJavaTypeFactory typeFactory) {
        super(new SqlIdentifier(name, SqlParserPos.ZERO),
            getReturnTypeInference(clazz),
            FunctionUtil.getSqlOperandTypeInference(clazz, typeFactory),
            FunctionUtil.getSqlOperandTypeChecker(name, clazz, typeFactory),
            null, null);
        this.implementClass = clazz;
    }

    public static GeaFlowUserDefinedGraphAlgorithm create(String name, Class<? extends AlgorithmUserFunction> clazz,
                                                          GQLJavaTypeFactory typeFactory) {
        return new GeaFlowUserDefinedGraphAlgorithm(name, clazz, typeFactory);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_ALGORITHM;
    }

    private static SqlReturnTypeInference getReturnTypeInference(final Class<?> clazz) {
        AlgorithmUserFunction algorithm;
        try {
            algorithm = (AlgorithmUserFunction)clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new GeaFlowDSLException("Cannot new instance for class: " + clazz.getName(), e);
        }
        final StructType outputType = algorithm.getOutputType();
        return opBinding -> {
            final JavaTypeFactoryImpl typeFactory = (JavaTypeFactoryImpl) opBinding.getTypeFactory();
            return SqlTypeUtil.convertToRelType(outputType, true, typeFactory);
        };
    }

    public Class<? extends AlgorithmUserFunction> getImplementClass() {
        return implementClass;
    }

}
