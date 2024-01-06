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

import com.antgroup.geaflow.api.function.iterator.RichIteratorFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggTraversalFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import java.util.Iterator;
import java.util.Objects;

public class GeaFlowStaticVCAggTraversalFunction implements
    VertexCentricAggTraversalFunction<Object, Row, Row, MessageBox, ITreePath, ITraversalAgg,
        ITraversalAgg>, RichIteratorFunction {

    private final GeaFlowCommonTraversalFunction commonFunction;
    private GeaFlowStaticTraversalRuntimeContext traversalRuntimeContext;

    public GeaFlowStaticVCAggTraversalFunction(ExecuteDagGroup executeDagGroup, boolean isTraversalAllWithRequest) {
        this.commonFunction = new GeaFlowCommonTraversalFunction(executeDagGroup, isTraversalAllWithRequest);
    }

    @Override
    public void open(
        VertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> vertexCentricFuncContext) {
        commonFunction.open(this.traversalRuntimeContext =
            new GeaFlowStaticTraversalRuntimeContext(vertexCentricFuncContext));
    }

    @Override
    public void initIteration(long windowId) {

    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        commonFunction.init(traversalRequest);
    }

    @Override
    public void compute(Object vertexId, Iterator<MessageBox> messageIterator) {
        commonFunction.compute(vertexId, messageIterator);
    }

    @Override
    public void finishIteration(long windowId) {
        commonFunction.finish(windowId);
    }

    @Override
    public void finish() {

    }

    @Override
    public void close() {
        commonFunction.close();
    }

    @Override
    public void initContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.traversalRuntimeContext.setAggContext(Objects.requireNonNull(aggContext));
    }
}
