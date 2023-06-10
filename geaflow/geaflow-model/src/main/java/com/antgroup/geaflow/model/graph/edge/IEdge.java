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

package com.antgroup.geaflow.model.graph.edge;

import java.io.Serializable;

public interface IEdge<K, EV> extends Serializable {

    /**
     * Get the source id of edge.
     *
     * @return
     */
    K getSrcId();

    /**
     * Set the source id for the edge.
     *
     * @param srcId
     */
    void setSrcId(K srcId);

    /**
     * Get the target id of edge.
     *
     * @return
     */
    K getTargetId();

    /**
     * Set the target id for the edge.
     *
     * @param targetId
     */
    void setTargetId(K targetId);

    /**
     * Get the direction of edge.
     *
     * @return
     */
    EdgeDirection getDirect();

    /**
     * Set the direction for the edge.
     *
     * @param direction
     */
    void setDirect(EdgeDirection direction);

    /**
     * Get the value of edge.
     *
     * @return
     */
    EV getValue();

    /**
     * Reset the value for the edge.
     *
     * @param value
     */
    IEdge<K, EV> withValue(EV value);

    /**
     * Reverse the source id and target id, and return new edge.
     *
     * @return
     */
    IEdge<K, EV> reverse();

}
