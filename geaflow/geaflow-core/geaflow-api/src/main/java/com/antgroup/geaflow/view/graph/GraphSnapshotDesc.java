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

package com.antgroup.geaflow.view.graph;

import com.antgroup.geaflow.api.partition.graph.vertex.GraphPartitioner;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import java.util.Map;

public class GraphSnapshotDesc extends GraphViewDesc {

    public GraphSnapshotDesc(String viewName, int shardNum, BackendType backend,
                             GraphPartitioner partitioner,
                             GraphMetaType graphMetaType,
                             Map props, long latestVersion) {
        super(viewName, shardNum, backend, partitioner, graphMetaType, props, latestVersion);
    }

    @Override
    public boolean isStatic() {
        return false;
    }
}
