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

package com.antgroup.geaflow.state.graph;

import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This interface describes the dynamic graph traits.
 */
public interface DynamicGraphTrait<K, VV, EV> {

    /**
     * Add the edge and its corresponding version.
     */
    void addEdge(long version, IEdge<K, EV> edge);

    /**
     * Add the vertex and its corresponding version.
     */
    void addVertex(long version, IVertex<K, VV> vertex);

    /**
     * Fetch the vertex according to the version, id and pushdown condition.
     */
    IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown);

    /**
     * Fetch the edge list according to the version, id and pushdown condition.
     */
    List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown);

    /**
     * Fetch the one degree graph according to the version, id and pushdown condition.
     */
    OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid, IStatePushDown pushdown);

    /**
     * Fetch the iterator of the ids of all graph vertices.
     */
    Iterator<K> vertexIDIterator();

    /**
     * Fetch the iterator of the graph vertices according to the version and pushdown condition.
     */
    Iterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of some vertices according to the version, ids and pushdown condition.
     */
    Iterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of graph edges according to the version and pushdown condition.
     */
    Iterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of some edges according to the version, ids and pushdown condition.
     */
    Iterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the version and pushdown condition.
     */
    Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the version, ids and pushdown condition.
     */
    Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the versions of some id.
     */
    List<Long> getAllVersions(K id, DataType dataType);

    /**
     * Fetch the latest version of some id.
     */
    long getLatestVersion(K id, DataType dataType);

    /**
     * Fetch all versioned data by id and pushdown condition.
     */
    Map<Long, IVertex<K,VV>> getAllVersionData(K id, IStatePushDown pushdown, DataType dataType);

    /**
     * Fetch some specific versioned data by id and pushdown condition.
     */
    Map<Long, IVertex<K,VV>> getVersionData(K id, Collection<Long> versions,
                                            IStatePushDown pushdown, DataType dataType);
}
