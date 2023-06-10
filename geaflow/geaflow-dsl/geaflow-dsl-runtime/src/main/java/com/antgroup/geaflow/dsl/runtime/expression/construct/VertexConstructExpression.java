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

package com.antgroup.geaflow.dsl.runtime.expression.construct;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.traversal.data.GlobalVariable;
import java.util.List;

public class VertexConstructExpression extends AbstractNonLeafExpression {

    private final Expression idExpression;

    private final Expression labelExpression;

    private final List<GlobalVariable> globalVariables;

    private final VertexType vertexType;

    public VertexConstructExpression(List<Expression> inputs, List<GlobalVariable> globalVariables,
                                     IType<?> outputType) {
        super(inputs, outputType);
        this.idExpression = inputs.get(VertexType.ID_FIELD_POSITION);
        this.labelExpression = inputs.get(VertexType.LABEL_FIELD_POSITION);
        this.globalVariables = globalVariables;
        this.vertexType = (VertexType) outputType;
    }

    @Override
    public Object evaluate(Row row) {
        Object id = idExpression.evaluate(row);
        Object label = labelExpression.evaluate(row);
        Object[] values = new Object[vertexType.getValueSize()];
        for (int i = vertexType.getValueOffset(); i < vertexType.size(); i++) {
            values[i - vertexType.getValueOffset()] = inputs.get(i).evaluate(row);
        }
        RowVertex vertex = VertexEdgeFactory.createVertex((VertexType) outputType);
        vertex.setId(id);
        vertex.setBinaryLabel((BinaryString) label);
        vertex.setValue(ObjectRow.create(values));
        return vertex;
    }

    @Override
    public String showExpression() {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < vertexType.size(); i++) {
            if (i > 0) {
                str.append(",");
            }
            str.append(vertexType.getField(i).getName()).append(":").append(inputs.get(i).showExpression());
        }
        return "Vertex{" + str + "}";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new VertexConstructExpression(inputs, globalVariables, getOutputType());
    }

    public List<GlobalVariable> getGlobalVariables() {
        return globalVariables;
    }
}
