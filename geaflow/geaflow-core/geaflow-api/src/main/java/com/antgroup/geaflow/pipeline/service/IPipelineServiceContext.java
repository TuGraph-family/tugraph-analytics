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

package com.antgroup.geaflow.pipeline.service;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.view.graph.PGraphView;
import java.io.Serializable;

public interface IPipelineServiceContext extends Serializable {

    /**
     * Returns session id.
     */
    long sessionId();

    /**
     * Get the request.
     */
    Object getRequest();

    /**
     * Collect response.
     */
    void response(Object response);

    /**
     * Returns configuration of pipeline service context.
     */
    Configuration getConfig();

    /**
     * Build window source with corresponding source function and window.
     */
    <T> PWindowSource<T> buildSource(SourceFunction<T> sourceFunction, IWindow<T> window);

    /**
     * Build graph view with view name.
     */
    <K, VV, EV> PGraphView<K, VV, EV> buildGraphView(String viewName);

}
