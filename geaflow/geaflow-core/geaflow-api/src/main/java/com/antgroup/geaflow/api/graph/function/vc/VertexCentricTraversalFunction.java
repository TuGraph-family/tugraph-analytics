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

package com.antgroup.geaflow.api.graph.function.vc;

import com.antgroup.geaflow.api.graph.function.vc.base.VertexCentricFunction;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import java.util.Iterator;

/**
 * Interface for vertex centric traversal function.
 * @param <K> The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M> The message type during iterations.
 * @param <R> The request type for traversal.
 */
public interface VertexCentricTraversalFunction<K, VV, EV, M, R> extends VertexCentricFunction<K, VV, EV, M> {

    /**
     * Open traversal function based on context.
     */
    void open(VertexCentricTraversalFuncContext<K, VV, EV, M, R> vertexCentricFuncContext);

    /**
     * Initialize the traversal by request.
     */
    void init(ITraversalRequest<K> traversalRequest);

    /**
     * Perform traversing based on message iterator during iterations.
     */
    void compute(K vertexId, Iterator<M> messageIterator);

    /**
     * Finish iteration traversal.
     */
    void finish();

    /**
     * Close resources in iteration traversal.
     */
    void close();

    interface VertexCentricTraversalFuncContext<K, VV, EV, M, R> extends VertexCentricFuncContext<K,
        VV, EV, M> {

        /**
         * Receive the response.
         */
        void takeResponse(ITraversalResponse<R> response);

        /**
         * Returns the TraversalVertexQuery.
         */
        TraversalVertexQuery<K, VV> vertex();

        /**
         * Returns the TraversalEdgeQuery.
         */
        TraversalEdgeQuery<K, EV> edges();

        /**
         * Broadcast message.
         */
        void broadcast(IGraphMessage<K, M> message);
    }

    interface TraversalVertexQuery<K, VV> extends VertexQuery<K, VV> {

        /**
         * Load vertex id iterator.
         */
        Iterator<K> loadIdIterator();
    }

    interface TraversalEdgeQuery<K, EV> extends EdgeQuery<K, EV> {

        /**
         * Set vertex id.
         * @param vertexId
         * @return
         */
        TraversalEdgeQuery<K, EV> withId(K vertexId);
    }
}
