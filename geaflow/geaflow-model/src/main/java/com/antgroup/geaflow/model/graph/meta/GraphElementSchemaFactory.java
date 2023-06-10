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

package com.antgroup.geaflow.model.graph.meta;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.schema.Field;
import com.antgroup.geaflow.common.schema.ISchema;
import com.antgroup.geaflow.common.schema.SchemaImpl;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.model.graph.IGraphElementWithLabelField;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import java.util.ArrayList;
import java.util.List;

public class GraphElementSchemaFactory {

    public static final Field FIELD_LABEL = Field.newStringField(GraphFiledName.LABEL.name());
    public static final Field FIELD_TIME = Field.newLongField(GraphFiledName.TIME.name());
    public static final Field FIELD_DIRECTION = Field.newByteField(GraphFiledName.DIRECTION.name());

    public static ISchema newSchema(IType keyType, Class<?> elementClass) {
        if (IVertex.class.isAssignableFrom(elementClass)) {
            return newVertexSchema(keyType, elementClass);
        }
        if (IEdge.class.isAssignableFrom(elementClass)) {
            return newEdgeSchema(keyType, elementClass);
        }
        String msg = "unrecognized graph element class: " + elementClass.getCanonicalName();
        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
    }

    private static ISchema newVertexSchema(IType keyType, Class<?> elementClass) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(GraphFiledName.ID.name(), keyType));
        if (IGraphElementWithLabelField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_LABEL);
        }
        if (IGraphElementWithTimeField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_TIME);
        }
        return new SchemaImpl(elementClass.getName(), fields);
    }

    private static ISchema newEdgeSchema(IType keyType, Class<?> elementClass) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(GraphFiledName.SRC_ID.name(), keyType));
        fields.add(new Field(GraphFiledName.DST_ID.name(), keyType));
        fields.add(FIELD_DIRECTION);
        if (IGraphElementWithLabelField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_LABEL);
        }
        if (IGraphElementWithTimeField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_TIME);
        }
        return new SchemaImpl(elementClass.getName(), fields);
    }

}
