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

package com.antgroup.geaflow.dsl.util;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public class GraphSchemaUtil {

    public static RelDataType getCurrentGraphVertexIdType(GQLJavaTypeFactory typeFactory) {
        if (typeFactory.getCurrentGraph() == null) {
            throw new GeaFlowDSLException("Cannot get vertex id type without setting current graph");
        } else if (typeFactory.getCurrentGraph().getVertexTables().isEmpty()) {
            throw new GeaFlowDSLException("No vertex table found in current graph {}", typeFactory.getCurrentGraph());
        } else {
            return typeFactory.getCurrentGraph().getVertexTables().get(0).getRowType(typeFactory).getIdField().getType();
        }
    }

    public static RelDataType getCurrentGraphEdgeSrcIdType(GQLJavaTypeFactory typeFactory) {
        if (typeFactory.getCurrentGraph() == null) {
            throw new GeaFlowDSLException("Cannot get edge src id type without setting current graph");
        } else if (typeFactory.getCurrentGraph().getEdgeTables().isEmpty()) {
            throw new GeaFlowDSLException("No edge table found in current graph {}", typeFactory.getCurrentGraph());
        } else {
            return typeFactory.getCurrentGraph().getEdgeTables().get(0).getRowType(typeFactory).getSrcIdField().getType();
        }
    }

    public static RelDataType getCurrentGraphEdgeTargetIdType(GQLJavaTypeFactory typeFactory) {
        if (typeFactory.getCurrentGraph() == null) {
            throw new GeaFlowDSLException("Cannot get edge target id type without setting current graph");
        } else if (typeFactory.getCurrentGraph().getEdgeTables().isEmpty()) {
            throw new GeaFlowDSLException("No edge table found in current graph {}", typeFactory.getCurrentGraph());
        } else {
            return typeFactory.getCurrentGraph().getEdgeTables().get(0).getRowType(typeFactory).getTargetIdField().getType();
        }
    }

    public static RelDataType getCurrentGraphLabelType(GQLJavaTypeFactory typeFactory) {
        if (typeFactory.getCurrentGraph() == null) {
            throw new GeaFlowDSLException("Cannot get label type without setting current graph");
        } else if (!typeFactory.getCurrentGraph().getVertexTables().isEmpty()) {
            return typeFactory.getCurrentGraph().getVertexTables().get(0).getRowType(typeFactory)
                .getLabelField().getType();
        } else if (!typeFactory.getCurrentGraph().getEdgeTables().isEmpty()) {
            return typeFactory.getCurrentGraph().getEdgeTables().get(0).getRowType(typeFactory)
                .getLabelField().getType();
        } else {
            throw new GeaFlowDSLException("No vertex or edge table found in current graph {}", typeFactory.getCurrentGraph());
        }
    }

    public static Optional<RelDataType> getCurrentGraphEdgeTimestampType(GQLJavaTypeFactory typeFactory) {
        if (typeFactory.getCurrentGraph() == null) {
            throw new GeaFlowDSLException("Cannot get edge ts type without setting current graph");
        } else if (typeFactory.getCurrentGraph().getEdgeTables().isEmpty()) {
            throw new GeaFlowDSLException("No edge table found in current graph {}", typeFactory.getCurrentGraph());
        } else if (typeFactory.getCurrentGraph().getEdgeTables().get(0).getRowType(typeFactory).getTimestampField().isPresent()) {
            return Optional.of(typeFactory.getCurrentGraph().getEdgeTables().get(0).getRowType(typeFactory)
                .getTimestampField().get().getType());
        } else {
            return Optional.empty();
        }
    }

}
