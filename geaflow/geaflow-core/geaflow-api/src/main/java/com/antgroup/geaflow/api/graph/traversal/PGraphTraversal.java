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

package com.antgroup.geaflow.api.graph.traversal;


import com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import java.util.List;

public interface PGraphTraversal<K, R> {

    /**
     * Start traversal all computing.
     */
    PWindowStream<ITraversalResponse<R>> start();

    /**
     * Start traversal computing with vid.
     */
    PWindowStream<ITraversalResponse<R>> start(K vId);

    /**
     * Start traversal computing with vid list.
     */
    PWindowStream<ITraversalResponse<R>> start(List<K> vId);

    /**
     * Start traversal computing with requests.
     */
    PWindowStream<ITraversalResponse<R>> start(PWindowStream<? extends ITraversalRequest<K>> requests);

    /**
     * Set the traversal parallelism.
     */
    PGraphTraversal<K, R> withParallelism(int parallelism);

    /**
     * Returns graph traversal type.
     */
    GraphExecAlgo getGraphTraversalType();

}
