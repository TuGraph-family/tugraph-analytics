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

package com.antgroup.geaflow.dsl.runtime;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import java.util.Collection;
import java.util.Map;

/**
 * Interface for a query engine.
 */
public interface QueryEngine {

    Map<String, String> getConfig();

    IPipelineTaskContext getContext();

    RuntimeTable createRuntimeTable(QueryContext context, GeaFlowTable table, Expression pushFilter);

    RuntimeTable createRuntimeTable(QueryContext context, Collection<Row> rows);

    <T> PWindowSource<T> createRuntimeTable(QueryContext context, SourceFunction<T> sourceFunction);

    RuntimeGraph createRuntimeGraph(QueryContext context, GeaFlowGraph graph);
}
