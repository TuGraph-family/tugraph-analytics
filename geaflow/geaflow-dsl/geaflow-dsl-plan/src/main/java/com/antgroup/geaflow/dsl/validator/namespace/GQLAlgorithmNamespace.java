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

package com.antgroup.geaflow.dsl.validator.namespace;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.sqlnode.SqlGraphAlgorithmCall;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.util.Pair;

public class GQLAlgorithmNamespace extends GQLBaseNamespace {

    private final SqlGraphAlgorithmCall graphAlgorithmCall;

    public GQLAlgorithmNamespace(SqlValidatorImpl validator, SqlGraphAlgorithmCall graphAlgorithmCall) {
        super(validator, graphAlgorithmCall);
        this.graphAlgorithmCall = graphAlgorithmCall;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        Map<String, Boolean> columnNameMap = new HashMap<>();
        SqlNodeList yields = graphAlgorithmCall.getYields();
        if (yields != null) {
            for (SqlNode yield : yields) {
                String yieldName = ((SqlIdentifier) yield).getSimple();
                if (columnNameMap.get(yieldName) == null) {
                    columnNameMap.put(yieldName, true);
                } else {
                    throw new GeaFlowDSLException(yield.getParserPosition(), "duplicate yield "
                        + "name: {}", yieldName);
                }
            }
        }
        List<SqlOperator> overloads = new ArrayList<>();
        getValidator().getOperatorTable().lookupOperatorOverloads(graphAlgorithmCall.getAlgorithm(),
            SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR, SqlSyntax.FUNCTION, overloads);
        if (overloads.isEmpty()) {
            throw new GeaFlowDSLException(graphAlgorithmCall.getParserPosition(),
                "Cannot load graph algorithm implementation of {}",
                graphAlgorithmCall.getAlgorithm().getSimple());
        } else {
            //When multiple implementation classes of an algorithm with the same name are found,
            // use the last registered class.
            graphAlgorithmCall.setOperator(overloads.get(overloads.size() - 1));
        }
        SqlOperator function = graphAlgorithmCall.getOperator();
        RelDataType inferType = function.inferReturnType(getValidator().getTypeFactory(),
            Collections.emptyList());
        if (yields == null) {
            return inferType;
        } else {
            if (yields.size() != inferType.getFieldCount()) {
                throw new GeaFlowDSLException(graphAlgorithmCall.getParserPosition().toString(),
                    "The number of fields returned after calling the graph algorithm: {} "
                        + "should be consistent with the definition in the graph algorithm implementation class: {}.",
                    yields.size(), inferType.getFieldCount());
            }
            final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();
            for (int i = 0, size = yields.size(); i < size; i++) {
                fieldList.add(Pair.of(((SqlIdentifier)yields.get(i)).getSimple(),
                    inferType.getFieldList().get(i).getType()));
            }
            return validator.getTypeFactory().createStructType(fieldList);
        }
    }

    @Override
    public SqlNode getNode() {
        return graphAlgorithmCall;
    }
}
