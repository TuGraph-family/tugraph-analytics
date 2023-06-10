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

import com.antgroup.geaflow.cluster.task.runner.AbstractTaskRunner;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmitterRunner extends AbstractTaskRunner<IEmitterRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmitterRunner.class);

    private final List<ICollector> collectors;

    public EmitterRunner() {
        this.collectors = new ArrayList<>();
    }

    @Override
    protected void process(IEmitterRequest request) {
        switch (request.getRequestType()) {
            case INIT:
                InitEmitterRequest initEmitRequest = (InitEmitterRequest) request;
                initEmitRequest.initEmitter(this.collectors);
                break;
            case CLOSE:
                CloseEmitterRequest closeEmitterRequest = (CloseEmitterRequest) request;
                closeEmitterRequest.closeEmitter(this.collectors);
                break;
            default:
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.requestTypeNotSupportError(request.getRequestType().name()));
        }
    }
}
