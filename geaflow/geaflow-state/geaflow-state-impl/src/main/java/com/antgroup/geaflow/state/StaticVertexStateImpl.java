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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.query.QueryType;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;
import java.util.Arrays;
import java.util.Collection;

public class StaticVertexStateImpl<K, VV, EV> extends BaseQueryState<K, VV, EV, IVertex<K, VV>> implements
    StaticVertexState<K, VV, EV> {

    public StaticVertexStateImpl(IGraphManager<K, VV, EV> graphManager) {
        super(new QueryType<>(DataType.V), graphManager);
    }

    @Override
    public void add(IVertex<K, VV> vertex) {
        this.graphManager.getStaticGraphTrait().addVertex(vertex);
    }

    @Override
    public void update(IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(K... ids) {
        delete(Arrays.asList(ids));
    }

    @Override
    public void delete(Collection<K> id) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
