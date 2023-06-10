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

package com.antgroup.geaflow.pipeline.task;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import java.io.Serializable;

public interface IPipelineTaskContext extends Serializable {

    /**
     * Returns pipeline task id.
     */
    long getPipelineTaskId();

    /**
     * Returns pipeline config.
     */
    Configuration getConfig();

    /**
     * Build window source with source function and window.
     */
    <T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window);

    /**
     * Returns graph view with view name.
     */
    <K, VV, EV> PGraphView<K, VV, EV> getGraphView(String viewName);

    /**
     * Create graph view with view desc.
     */
    <K, VV, EV> PGraphView<K, VV, EV> createGraphView(IViewDesc viewDesc);

    /**
     * Build window stream graph.
     */
    <K, VV, EV> PGraphWindow<K, VV, EV> buildWindowStreamGraph(PWindowStream<IVertex<K, VV>> vertexWindowSteam,
                                                               PWindowStream<IEdge<K, EV>> edgeWindowStream,
                                                               GraphViewDesc graphViewDesc);

}
