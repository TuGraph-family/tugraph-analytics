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

package com.antgroup.geaflow.service.discovery;

public interface ServiceListener {

    /**
     * Called when a new node has been created.
     */
    void nodeCreated(String path);

    /**
     * Called when a node has been deleted.
     */
    void nodeDeleted(String path);

    /**
     * Called when an existing node has changed data.
     */
    void nodeDataChanged(String path);

    /**
     * Called when an existing node has a child node added or removed.
     */
    void nodeChildrenChanged(String path);
}
