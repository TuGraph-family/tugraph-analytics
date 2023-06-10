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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class GeaflowTransferJob extends GeaflowCodeJob {

    protected Map<String, Map<String, Map<String, String>>> structMappings = new LinkedHashMap<>();

    public GeaflowTransferJob(GeaflowJobType type) {
        super(type);
    }

    @Override
    public GeaflowCode generateCode() {
        //TODO generateCode
        return null;
    }

    @Override
    public List<GeaflowStruct> getStructs() {
        return new ArrayList<>(structs.values());
    }

    @Override
    public List<GeaflowFunction> getFunctions() {
        return new ArrayList<>();
    }

    public Map<String, Map<String, Map<String, String>>> getStructMappings() {
        return structMappings;
    }

    @Override
    public GeaflowCode getUserCode() {
        return null;
    }

    public void fromTableToTable(GeaflowTable input, GeaflowTable output, Map<String, String> fieldMapping) {
        addStructMapping(input, output, fieldMapping);
    }

    protected void addStructMapping(GeaflowStruct input, GeaflowStruct output, Map<String, String> fieldMapping) {
        String inputName = input.getName();
        String outputName = output.getName();

        //inputStructs.put(inputName, input);
        //outputStructs.put(outputName, output);

        structMappings.putIfAbsent(inputName, new LinkedHashMap<>());
        structMappings.get(inputName).putIfAbsent(outputName, fieldMapping);
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
}
