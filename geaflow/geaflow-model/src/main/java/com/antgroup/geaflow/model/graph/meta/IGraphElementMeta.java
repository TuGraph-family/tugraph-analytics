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

package com.antgroup.geaflow.model.graph.meta;

import com.antgroup.geaflow.common.schema.ISchema;
import java.io.Serializable;
import java.util.function.Supplier;

public interface IGraphElementMeta<ELEMENT> extends Serializable {

    /**
     * Get graph element id.
     *
     * @return graph element id
     */
    byte getGraphElementId();

    /**
     * Get graph element class.
     *
     * @return vertex or edge class
     */
    Class<ELEMENT> getGraphElementClass();

    /**
     * Get meta of graph element, like id, label of vertex or src, dst, ts of edge.
     *
     * @return vertex or edge primitive schema
     */
    ISchema getGraphMeta();

    /**
     * Get graph field serializer.
     *
     * @return graph field serializer
     */
    IGraphFieldSerializer<ELEMENT> getGraphFieldSerializer();

    /**
     * Get property class of a vertex or edge.
     *
     * @return property class
     */
    Class<?> getPropertyClass();

    Supplier<ELEMENT> getGraphElementConstruct();
}
