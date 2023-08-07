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

package com.antgroup.geaflow.dsl.common.algo;


import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.StructType;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Interface for the User Defined Graph Algorithm.
 * @param <K> The id type for vertex.
 * @param <M> The message type for message send between vertices.
 */
public interface AlgorithmUserFunction<K, M> extends Serializable {

    /**
     * Init method for the function.
     * @param context The runtime context.
     * @param params  The parameters for the function.
     */
    void init(AlgorithmRuntimeContext<K, M> context, Object[] params);

    /**
     * Processing method for each vertex and the messages it received.
     */
    void process(RowVertex vertex, Iterator<M> messages);

    /**
     * Finish method called by each vertex upon algorithm convergence.
     */
    void finish(RowVertex vertex);

    /**
     * Returns the output type for the function.
     */
    StructType getOutputType();
}
