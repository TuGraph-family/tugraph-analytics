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

package com.antgroup.geaflow.common.schema;

import java.io.Serializable;
import java.util.List;

public interface ISchema extends Serializable {

    /**
     * Get schema id.
     *
     * @return schema id
     */
    int getSchemaId();

    /**
     * Get schema name.
     *
     * @return schema name
     */
    String getSchemaName();

    /**
     * Get a field of an index.
     *
     * @param index field index
     * @return field
     */
    Field getField(int index);

    /**
     * Get a field of a specified field name.
     *
     * @param fieldName field name
     * @return field
     */
    Field getField(String fieldName);

    /**
     * Get all fields.
     *
     * @return field array
     */
    List<Field> getFields();

}
