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

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.collector.ICollector;
import java.util.List;

public class InitCollectEmitterRequest extends InitEmitterRequest {

    private int opId;

    public InitCollectEmitterRequest(int opId) {
        super(null);
        this.opId = opId;
    }

    /**
     * Init output collectors.
     */
    public void initEmitter(List<ICollector> collectors) {
        collectors.clear();
        collectors.add(new CollectCollector(opId));
        this.collectors = collectors;
    }
}
