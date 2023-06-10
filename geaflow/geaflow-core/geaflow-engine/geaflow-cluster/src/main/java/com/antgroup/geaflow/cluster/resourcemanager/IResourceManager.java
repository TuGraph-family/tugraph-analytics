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

package com.antgroup.geaflow.cluster.resourcemanager;

/**
 * IResourceManager interface.
 */
public interface IResourceManager {

    /**
     * Initialization.
     * @param context resource manager context
     */
    void init(ResourceManagerContext context);

    /**
     * Require resource with specified allocate strategy.
     * @param requireRequest require request
     * @return require response
     */
    RequireResponse requireResource(RequireResourceRequest requireRequest);

    /**
     * Release resource.
     * @param releaseRequest release request
     * @return release response
     */
    ReleaseResponse releaseResource(ReleaseResourceRequest releaseRequest);

}
