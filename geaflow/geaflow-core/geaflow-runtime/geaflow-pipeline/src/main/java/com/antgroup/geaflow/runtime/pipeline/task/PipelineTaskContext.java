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

package com.antgroup.geaflow.runtime.pipeline.task;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.pdata.graph.view.IncGraphView;
import com.antgroup.geaflow.pdata.graph.window.WindowStreamGraph;
import com.antgroup.geaflow.pdata.stream.window.WindowStreamSource;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.runtime.pipeline.PipelineContext;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;

public class PipelineTaskContext implements IPipelineTaskContext {

    private long pipelineTaskId;
    private PipelineContext pipelineContext;

    public PipelineTaskContext(long pipelineTaskId,
                               PipelineContext pipelineContext) {
        this.pipelineTaskId = pipelineTaskId;
        this.pipelineContext = pipelineContext;
    }

    @Override
    public long getPipelineTaskId() {
        return this.pipelineTaskId;
    }

    @Override
    public Configuration getConfig() {
        return pipelineContext.getConfig();
    }

    @Override
    public <T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window) {
        return new WindowStreamSource<>(pipelineContext, sourceFunction, window);
    }

    @Override
    public <K, VV, EV> PGraphView<K, VV, EV> getGraphView(String viewName) {
        IViewDesc viewDesc = pipelineContext.getViewDesc(viewName);
        return new IncGraphView<>(pipelineContext, viewDesc);
    }

    @Override
    public <K, VV, EV> PGraphView<K, VV, EV> createGraphView(IViewDesc viewDesc) {
        return new IncGraphView<>(pipelineContext, viewDesc);
    }

    @Override
    public <K, VV, EV> PGraphWindow<K, VV, EV> buildWindowStreamGraph(PWindowStream<IVertex<K, VV>> vertexWindowSteam,
                                                                      PWindowStream<IEdge<K, EV>> edgeWindowStream,
                                                                      GraphViewDesc graphViewDesc) {
        return new WindowStreamGraph(graphViewDesc, pipelineContext, vertexWindowSteam, edgeWindowStream);
    }

}
