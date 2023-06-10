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

package com.antgroup.geaflow.store;

import com.antgroup.geaflow.store.context.StoreContext;

/**
 * Basic store interface for all types of store.
 */
public interface IBaseStore {

    /**
     * init the store context, including all setup properties.
     */
    void init(StoreContext storeContext);

    /**
     * archive current store data for persistence.
     */
    void archive(long checkpointId);

    /**
     * recovery the store data from persistent storage.
     */
    void recovery(long checkpointId);

    /**
     * recovery the latest store data.
     */
    long recoveryLatest();

    /**
     * compact the store data.
     */
    void compact();

    /**
     * flush the store data to disk or remote storage.
     */
    void flush();

    /**
     * close the store handler and ll other used resources.
     */
    void close();

    /**
     * delete the disk data.
     */
    void drop();
}
