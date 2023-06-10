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

package com.antgroup.geaflow.state.manage;

/**
 * The state operator class.
 */
public interface StateOperator {

    /**
     * set checkpoint id to state.
     */
    void setCheckpointId(long checkpointId);

    /**
     * flush state to disk.
     */
    void finish();

    /**
     * compact state data.
     */
    void compact();

    /**
     * persist data.
     */
    void archive();

    /**
     * recover data from persistent storage.
     */
    void recover();

    /**
     * close state and release used resource.
     */
    void close();

    /**
     * drop disk data and close.
     */
    void drop();
}
