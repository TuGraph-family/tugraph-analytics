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

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This interface describes the static graph traits.
 */
public interface StaticGraphTrait<K, VV, EV> {

    /**
     * Add the edge to the static graph.
     */
    void addEdge(IEdge<K, EV> edge);

    /**
     * Add the vertex to the static graph.
     */
    void addVertex(IVertex<K, VV> vertex);

    /**
     * Fetch the vertex according to the id and pushdown condition.
     */
    IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown);

    /**
     * Fetch the edges according to the id and pushdown condition.
     */
    List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown);

    /**
     * Fetch the one degree graph according to the id and pushdown condition.
     */
    OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown);

    /**
     * Fetch the iterator of the ids of all graph vertices.
     */
    Iterator<K> vertexIDIterator();

    /**
     * Fetch the iterator of the graph vertices according to the pushdown condition.
     */
    Iterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown);

    /**
     * Fetch the iterator of some vertices according to the ids and pushdown condition.
     */
    Iterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of graph edges according to the pushdown condition.
     */
    Iterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown);

    /**
     * Fetch the iterator of the graph edges according to the ids and pushdown condition.
     */
    Iterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the pushdown condition.
     */
    Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the ids and pushdown condition.
     */
    Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown);


    /**
     * Fetch the project result of edges according to the pushdown condition.
     */
    <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown);

    /**
     * Fetch the project result of edges according to the ids and pushdown condition.
     */
    <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys, IStatePushDown<K, IEdge<K, EV>, R> pushdown);

    /**
     * Fetch the aggregated results according to the pushdown condition.
     */
    Map<K, Long> getAggResult(IStatePushDown pushdown);

    /**
     * Fetch the aggregated results according to the ids and pushdown condition.
     */
    Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown);
}
