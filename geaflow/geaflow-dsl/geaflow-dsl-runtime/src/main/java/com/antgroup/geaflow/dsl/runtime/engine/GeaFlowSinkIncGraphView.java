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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.SinkDataView;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import java.util.List;

public class GeaFlowSinkIncGraphView implements SinkDataView {

    private final IPipelineTaskContext context;

    public GeaFlowSinkIncGraphView(IPipelineTaskContext context) {
        this.context = context;
    }

    @Override
    public <T> T getPlan() {
        throw new GeaFlowDSLException("Should not call getPlan() on GeaFlowSinkIncGraphView");
    }

    @Override
    public List<? extends Row> take() {
        throw new GeaFlowDSLException("Should not call take() on GeaFlowIncGraphView");
    }
}
