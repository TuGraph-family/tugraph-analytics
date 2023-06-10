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

import java.io.Serializable;

public interface IGraphFieldSerializer<ELEMENT> extends Serializable {

    /**
     * Extract value of a key from a vertex or edge.
     *
     * @param element vertex or edge
     * @param field graph field name
     * @return value
     */
    Object getValue(ELEMENT element, GraphFiledName field);

    /**
     * Inject value of a key to a vertex or edge.
     *
     * @param element vertex or edge
     * @param field graph field name
     * @param value value
     */
    void setValue(ELEMENT element, GraphFiledName field, Object value);

}
