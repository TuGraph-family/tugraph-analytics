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

import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.SinkDataView;
import com.antgroup.geaflow.pipeline.job.IPipelineJobContext;
import java.util.List;

public class GeaFlowSinkDataView implements SinkDataView {

    private final IPipelineJobContext context;

    private final PStreamSink<Row> sink;

    public GeaFlowSinkDataView(IPipelineJobContext context, PStreamSink<Row> sink) {
        this.context = context;
        this.sink = sink;
    }

    @Override
    public <T> T getPlan() {
        return (T) sink;
    }

    @Override
    public List<? extends Row> take(IType<?> type) {
        throw new GeaFlowDSLException("Should not call take() on SinkDataView");
    }
}
