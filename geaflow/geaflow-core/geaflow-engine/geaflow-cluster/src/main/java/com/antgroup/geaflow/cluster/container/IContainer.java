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

package com.antgroup.geaflow.cluster.container;

import com.antgroup.geaflow.cluster.common.IEventProcessor;
import com.antgroup.geaflow.cluster.protocol.OpenContainerEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerResponseEvent;

public interface IContainer<I, O> extends IEventProcessor<I, O> {

    /**
     * Initialize container.
     */
    void init(ContainerContext containerContext);

    /**
     * Open container to run workers.
     */
    OpenContainerResponseEvent open(OpenContainerEvent event);

    /**
     * Close container.
     */
    void close();

}
