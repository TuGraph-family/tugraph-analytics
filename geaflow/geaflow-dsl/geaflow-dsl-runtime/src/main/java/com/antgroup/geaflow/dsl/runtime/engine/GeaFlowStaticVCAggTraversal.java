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

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggTraversalFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import java.util.Objects;

public class GeaFlowStaticVCAggTraversal extends VertexCentricAggTraversal<Object, Row, Row,
    MessageBox, ITreePath,
    ITraversalAgg, ITraversalAgg, ITraversalAgg, ITraversalAgg, ITraversalAgg> {
    private final ExecuteDagGroup executeDagGroup;

    private final boolean isTraversalAllWithRequest;

    private final int parallelism;

    public GeaFlowStaticVCAggTraversal(ExecuteDagGroup executeDagGroup,
                                       int maxTraversal,
                                       boolean isTraversalAllWithRequest,
                                       int parallelism) {
        super(maxTraversal);
        this.executeDagGroup = Objects.requireNonNull(executeDagGroup);
        this.isTraversalAllWithRequest = isTraversalAllWithRequest;
        assert parallelism > 0;
        this.parallelism = parallelism;
    }

    @Override
    public VertexCentricCombineFunction<MessageBox> getCombineFunction() {
        return new MessageBoxCombineFunction();
    }

    @Override
    public IEncoder<MessageBox> getMessageEncoder() {
        return null;
    }

    @Override
    public VertexCentricAggTraversalFunction<Object, Row, Row, MessageBox, ITreePath, ITraversalAgg,
        ITraversalAgg> getTraversalFunction() {
        return new GeaFlowStaticVCAggTraversalFunction(executeDagGroup, isTraversalAllWithRequest);
    }

    @Override
    public VertexCentricAggregateFunction<ITraversalAgg, ITraversalAgg, ITraversalAgg, ITraversalAgg, ITraversalAgg> getAggregateFunction() {
        return (VertexCentricAggregateFunction) new GeaFlowKVTraversalAggregateFunction(parallelism);
    }

    private static class MessageBoxCombineFunction implements VertexCentricCombineFunction<MessageBox> {

        @Override
        public MessageBox combine(MessageBox oldMessage, MessageBox newMessage) {
            return newMessage.combine(oldMessage);
        }
    }
}
