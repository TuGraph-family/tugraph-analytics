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
import com.antgroup.geaflow.shuffle.IOutputDesc;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitEmitterRequest implements IEmitterRequest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitEmitterRequest.class);

    private OutputDescriptor outputDescriptor;
    protected List<ICollector> collectors;

    public InitEmitterRequest(OutputDescriptor outputDescriptor) {
        this.outputDescriptor = outputDescriptor;
    }

    /**
     * Init output collectors.
     */
    public void initEmitter(List<ICollector> collectors) {
        if (outputDescriptor != null && outputDescriptor.getOutputDescList() != null) {
            collectors.clear();
            for (IOutputDesc outputDesc : outputDescriptor.getOutputDescList()) {
                ICollector collector = CollectorFactory.create(outputDesc);
                collectors.add(collector);
            }
        }
        this.collectors = collectors;
    }


    public List<ICollector> getCollectors() {
        while (collectors == null) {
            LOGGER.debug("wait init request done");
        }
        return collectors;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.INIT;
    }
}
