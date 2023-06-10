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
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.query.QueryType;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class StaticEdgeStateImpl<K, VV, EV> extends BaseQueryState<K, VV, EV, IEdge<K, EV>> implements
    StaticEdgeState<K, VV, EV> {

    public StaticEdgeStateImpl(IGraphManager<K, VV, EV> manager) {
        super(new QueryType<>(DataType.E), manager);
    }

    @Override
    public void add(IEdge<K, EV> edge) {
        this.graphManager.getStaticGraphTrait().addEdge(edge);
    }

    @Override
    public void update(IEdge<K, EV> edge) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(IEdge<K, EV> edge) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(K... ids) {
        delete(Arrays.asList(ids));
    }

    @Override
    public void delete(Collection<K> ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public Iterator<K> idIterator() {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
