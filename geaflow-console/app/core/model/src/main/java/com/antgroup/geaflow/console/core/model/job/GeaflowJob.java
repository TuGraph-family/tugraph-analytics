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

import com.antgroup.geaflow.console.common.util.type.GeaflowJobType;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class GeaflowJob extends GeaflowName {

    protected final Map<String, GeaflowStruct> structs = new LinkedHashMap<>();

    protected final Map<String, GeaflowGraph> graphs = new LinkedHashMap<>();

    protected List<GeaflowFunction> functions = new ArrayList<>();

    @Setter
    protected GeaflowJobType type;

    @Setter
    protected GeaflowJobSla sla;

    @Setter
    protected String instanceId;

    public GeaflowJob(GeaflowJobType type) {
        this.type = type;
    }


    public abstract boolean isApiJob();

    public abstract GeaflowRemoteFile getJarPackage();

    public abstract String getEntryClass();

    public abstract List<GeaflowFunction> getFunctions();

    public abstract Map<String, Map<String, Map<String, String>>> getStructMappings();

    public abstract GeaflowCode getUserCode();

    public abstract GeaflowCode generateCode();

    public List<GeaflowGraph> getGraphs() {
        return new ArrayList<>(graphs.values());
    }

    public List<GeaflowStruct> getStructs() {
        return new ArrayList<>(structs.values());
    }

    public void setStructs(List<GeaflowStruct> structs) {
        for (GeaflowStruct struct : structs) {
            this.structs.put(struct.getName(), struct);
        }
    }


    public void setGraph(List<GeaflowGraph> graphs) {
        for (GeaflowGraph graph : graphs) {
            this.graphs.put(graph.getName(), graph);
        }
    }

    public void setFunctions(List<GeaflowFunction> functions) {
        this.functions = functions;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(type, "job type is null");
        Preconditions.checkNotNull(instanceId, "instanceId is null");
    }

}
