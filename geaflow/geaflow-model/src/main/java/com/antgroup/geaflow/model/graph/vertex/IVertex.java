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

package com.antgroup.geaflow.model.graph.vertex;

import java.io.Serializable;

public interface IVertex<K, VV> extends Serializable, Comparable {

    /**
     * Get id of vertex.
     *
     * @return id
     */
    K getId();

    /**
     * Set id for the vertex.
     *
     * @param id id
     */
    void setId(K id);

    /**
     * Get value of vertex.
     *
     * @return value
     */
    VV getValue();

    /**
     * Reset value for the vertex.
     *
     * @param value value
     * @return vertex
     */
    IVertex<K, VV> withValue(VV value);

    /**
     * Reset label value for the vertex.
     *
     * @param label label
     * @return vertex
     */
    IVertex<K, VV> withLabel(String label);

    /**
     * Reset time value for the vertex.
     *
     * @param time time
     * @return vertex
     */
    IVertex<K, VV> withTime(long time);

}
