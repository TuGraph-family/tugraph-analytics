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
import java.util.Collection;
import java.util.List;

public class DynamicVertexStateImpl<K, VV, EV>  extends
    BaseDynamicQueryState<K, VV, EV, IVertex<K, VV>> implements
    DynamicVertexState<K, VV, EV> {

    public DynamicVertexStateImpl(IGraphManager<K, VV, EV> graphManager) {
        super(new QueryType<>(DataType.V), graphManager);
    }

    @Override
    public List<Long> getAllVersions(K id) {
        return this.graphManager.getDynamicGraphTrait().getAllVersions(id, DataType.V);
    }

    @Override
    public long getLatestVersion(K id) {
        return this.graphManager.getDynamicGraphTrait().getLatestVersion(id, DataType.V);
    }

    @Override
    public void add(long version, IVertex<K, VV> vertex) {
        this.graphManager.getDynamicGraphTrait().addVertex(version, vertex);
    }

    @Override
    public void update(long version, IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(long version, IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(long version, K... ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(long version, Collection<K> ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
