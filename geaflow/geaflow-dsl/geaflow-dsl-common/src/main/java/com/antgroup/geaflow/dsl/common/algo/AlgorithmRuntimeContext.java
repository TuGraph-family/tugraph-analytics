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

package com.antgroup.geaflow.dsl.common.algo;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.List;

public interface AlgorithmRuntimeContext<K, M> {

    List<RowEdge> loadEdges(EdgeDirection direction);

    void sendMessage(K vertexId, M message);

    void updateVertexValue(Row value);

    void take(Row value);

    long getCurrentIterationId();

    GraphSchema getGraphSchema();
}
