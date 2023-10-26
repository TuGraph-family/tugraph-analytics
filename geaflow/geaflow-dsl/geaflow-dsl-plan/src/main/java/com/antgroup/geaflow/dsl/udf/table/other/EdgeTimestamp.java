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

package com.antgroup.geaflow.dsl.udf.table.other;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.util.GraphSchemaUtil;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.calcite.rel.type.RelDataType;

@Description(name = "ts", description = "Returns ts for edge with timestamp")
public class EdgeTimestamp extends UDF implements GraphMetaFieldAccessFunction {

    public Long eval(IGraphElementWithTimeField edge) {
        return edge.getTime();
    }

    @Override
    public RelDataType getReturnRelDataType(GQLJavaTypeFactory typeFactory) {
        if (GraphSchemaUtil.getCurrentGraphEdgeTimestampType(typeFactory).isPresent()) {
            return GraphSchemaUtil.getCurrentGraphEdgeTimestampType(typeFactory).get();
        } else {
            throw new GeaFlowDSLException("Cannot find timestamp type");
        }
    }
}
