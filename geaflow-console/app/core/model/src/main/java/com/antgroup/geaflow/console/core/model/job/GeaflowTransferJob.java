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

package com.antgroup.geaflow.console.core.model.job;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowJobType;
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.data.GeaflowTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
public abstract class GeaflowTransferJob extends GeaflowCodeJob {

    protected List<StructMapping> structMappings = new ArrayList<>();

    public GeaflowTransferJob(GeaflowJobType type) {
        super(type);
    }


    public abstract GeaflowCode generateCode();

    @Override
    public List<GeaflowStruct> getStructs() {
        return new ArrayList<>(structs.values());
    }

    @Override
    public List<GeaflowFunction> getFunctions() {
        return new ArrayList<>();
    }

    public List<StructMapping> getStructMappings() {
        return structMappings;
    }

    public void fromTableToTable(GeaflowTable input, GeaflowTable output, List<FieldMappingItem> fieldMapping) {
        addStructMapping(input, output, fieldMapping);
    }

    protected void addStructMapping(GeaflowStruct input, GeaflowStruct output, List<FieldMappingItem> fieldMapping) {
        String inputName = input.getName();
        String outputName = output.getName();

        //inputStructs.put(inputName, input);
        //outputStructs.put(outputName, output);
        structMappings.add(new StructMapping(inputName, outputName, fieldMapping));
    }

    protected GeaflowStruct importGraphStruct(GeaflowGraph graph, GeaflowStructType type, String name) {
        String graphName = graph.getName();
        graphs.put(graphName, graph);
        Preconditions.checkArgument(graphs.size() == 1, "Only one graph supported");

        GeaflowStruct struct;
        switch (type) {
            case VERTEX:
                struct = graph.getVertices().get(name);
                break;
            case EDGE:
                struct = graph.getEdges().get(name);
                break;
            default:
                throw new GeaflowException("Struct type {} not allowed in graph", type);
        }
        return struct;
    }

    @Setter
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(of = {"tableName", "structName"})
    public static class StructMapping {

        private String tableName;
        private String structName;
        private List<FieldMappingItem> fieldMappings = new ArrayList<>();
    }

    @Setter
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(of = {"tableFieldName", "structFieldName"})
    public static class FieldMappingItem {

        private String tableFieldName;
        private String structFieldName;
    }

}
