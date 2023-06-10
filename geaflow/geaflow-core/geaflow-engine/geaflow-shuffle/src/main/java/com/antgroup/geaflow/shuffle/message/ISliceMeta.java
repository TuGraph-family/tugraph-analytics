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

package com.antgroup.geaflow.shuffle.message;

import java.io.Serializable;

public interface ISliceMeta extends Serializable {

    /**
     * Get source index of the slice.
     *
     * @return source index.
     */
    int getSourceIndex();

    /**
     * Get target index of the slice.
     *
     * @return target index.
     */
    int getTargetIndex();

    /**
     * Get record number of the slice.
     *
     * @return record number.
     */
    long getRecordNum();

    /**
     * Get encode size (bytes) of the slice.
     *
     * @return encode size.
     */
    long getEncodedSize();

}
