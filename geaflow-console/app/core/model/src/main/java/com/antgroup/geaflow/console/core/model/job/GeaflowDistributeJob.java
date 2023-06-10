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
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.data.GeaflowTable;
import java.util.Map;

public class GeaflowDistributeJob extends GeaflowTransferJob {


    public GeaflowDistributeJob() {
        super(GeaflowJobType.DISTRIBUTE);
    }

    public void fromGraphToTable(GeaflowGraph graph, GeaflowStructType type, String name, GeaflowTable table,
                                 Map<String, String> fieldMapping) {
        GeaflowStruct struct = super.importGraphStruct(graph, type, name);
        super.addStructMapping(struct, table, fieldMapping);
    }
}
