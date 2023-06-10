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

package com.antgroup.geaflow.state.query;

import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.pushdown.project.IProjector;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The base query interface for graph.
 */
public interface QueryableGraphState<K, VV, EV, R> {

    /**
     * query by some projector.
     */
    <U> QueryableGraphState<K, VV, EV, U> select(IProjector<R, U> projector);

    /**
     * query by edge limit.
     */
    QueryableGraphState<K, VV, EV, R> limit(long out, long in);

    /**
     * query by some order.
     */
    QueryableGraphState<K, VV, EV, R> orderBy(EdgeAtom atom);

    /**
     * query a aggregate result.
     */
    Map<K, Long> aggregate();

    /**
     * query result is a list.
     */
    List<R> asList();

    /**
     * query result is a iterator.
     */
    Iterator<R> iterator();

    /**
     * get a simple result like a vertex.
     */
    R get();
}
