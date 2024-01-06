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

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.util.GraphSchemaUtil;
import org.apache.calcite.rel.type.RelDataType;

@Description(name = "label", description = "Returns label for edge or vertex")
public class Label extends UDF implements GraphMetaFieldAccessFunction {

    public BinaryString eval(RowEdge edge) {
        return edge.getBinaryLabel();
    }

    public BinaryString eval(RowVertex vertex) {
        return vertex.getBinaryLabel();
    }

    @Override
    public RelDataType getReturnRelDataType(GQLJavaTypeFactory typeFactory) {
        return GraphSchemaUtil.getCurrentGraphLabelType(typeFactory);
    }
}
