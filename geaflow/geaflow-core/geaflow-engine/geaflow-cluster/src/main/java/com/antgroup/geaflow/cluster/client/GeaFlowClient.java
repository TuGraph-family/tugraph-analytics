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

package com.antgroup.geaflow.cluster.client;

import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import java.io.Serializable;

public class GeaFlowClient implements Serializable {

    private IClusterClient clusterClient;
    private IPipelineClient pipelineClient;

    public void init(IEnvironmentContext environmentContext, IClusterClient clusterClient) {
        this.clusterClient = clusterClient;
        this.clusterClient.init(environmentContext);
    }

    public void startCluster() {
        this.pipelineClient = this.clusterClient.startCluster();
    }

    public IPipelineResult submit(Pipeline pipeline) {
        return this.pipelineClient.submit(pipeline);
    }

    public void shutdown() {
        this.pipelineClient.close();
        this.clusterClient.shutdown();
    }

}

