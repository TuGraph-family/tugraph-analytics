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

import com.antgroup.geaflow.state.query.QueryableAllGraphState;
import com.antgroup.geaflow.state.query.QueryableKeysGraphState;
import java.util.Iterator;
import java.util.List;

/**
 * The query interface for static graph.
 */
public interface StaticQueryableState<K, VV, EV, R> {

    /**
     * Returns the all graph handler.
     */
    QueryableAllGraphState<K, VV, EV, R> query();

    /**
     * Returns the point query graph handler.
     */
    QueryableKeysGraphState<K, VV, EV, R> query(K id);

    /**
     * Returns the point query graph handler.
     */
    QueryableKeysGraphState<K, VV, EV, R> query(K... ids);

    /**
     * Returns the point query graph handler.
     */
    QueryableKeysGraphState<K, VV, EV, R> query(List<K> ids);

    /**
     * Returns the graph id iterator.
     */
    Iterator<K> idIterator();

    /**
     * Returns the graph query result iterator.
     */
    Iterator<R> iterator();

    /**
     * Returns the graph query result list.
     */
    List<R> asList();

}
