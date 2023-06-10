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

package com.antgroup.geaflow.dsl.common.binary;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.binary.encoder.DefaultEdgeEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.DefaultRowEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.DefaultVertexEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.EdgeEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.IBinaryEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.RowEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.VertexEncoder;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import java.util.Locale;

public class EncoderFactory {

    public static IBinaryEncoder createEncoder(IType<?> type) {
        String typeName = type.getName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case Types.TYPE_NAME_VERTEX:
                return new DefaultVertexEncoder((VertexType) type);
            case Types.TYPE_NAME_EDGE:
                return new DefaultEdgeEncoder((EdgeType) type);
            case Types.TYPE_NAME_STRUCT:
                return new DefaultRowEncoder((StructType) type);
            default:
                throw new GeaFlowDSLException("encoder type " + type.getName() + " is not support");
        }
    }

    public static VertexEncoder createVertexEncoder(VertexType vertexType) {
        return new DefaultVertexEncoder(vertexType);
    }

    public static EdgeEncoder createEdgeEncoder(EdgeType edgeType) {
        return new DefaultEdgeEncoder(edgeType);
    }

    public static RowEncoder createRowEncoder(StructType rowType) {
        return new DefaultRowEncoder(rowType);
    }
}
