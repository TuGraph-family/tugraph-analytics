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

package com.antgroup.geaflow.view;

import com.antgroup.geaflow.api.partition.graph.vertex.GraphPartitioner;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.utils.math.MathUtil;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.google.common.base.Preconditions;
import java.util.Map;

public class GraphViewBuilder {

    public static final String DEFAULT_GRAPH = "default_graph";

    private final String viewName;

    private int shardNum;
    private BackendType backend;
    private GraphPartitioner partitioner;
    private GraphMetaType graphMetaType;
    private Map props;
    private long latestVersion = -1L;

    private GraphViewBuilder(String name) {
        this.viewName = name;
    }

    public static GraphViewBuilder createGraphView(String name) {
        return new GraphViewBuilder(name);
    }

    public GraphViewBuilder withShardNum(int shardNum) {
        this.shardNum = shardNum;
        return this;
    }

    public GraphViewBuilder withBackend(BackendType backend) {
        this.backend = backend;
        return this;
    }

    public GraphViewBuilder withSchema(GraphMetaType graphMetaType) {
        this.graphMetaType = graphMetaType;
        return this;
    }

    public GraphViewBuilder withProps(Map props) {
        this.props = props;
        return this;
    }

    public GraphViewBuilder withLatestVersion(long latestVersion) {
        this.latestVersion = latestVersion;
        return this;
    }

    public GraphViewDesc build() {
        Preconditions.checkArgument(this.viewName != null, "this name is empty");
        Preconditions.checkArgument(MathUtil.isPowerOf2(this.shardNum), "this shardNum must be power of 2");
        Preconditions.checkArgument(this.backend != null, "this backend is null");

        return new GraphViewDesc(viewName, shardNum, backend, partitioner, graphMetaType, props, latestVersion);
    }

}
