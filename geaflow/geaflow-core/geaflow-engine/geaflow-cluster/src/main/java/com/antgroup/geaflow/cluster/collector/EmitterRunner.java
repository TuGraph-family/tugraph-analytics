/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.cluster.task.runner.AbstractTaskRunner;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

public class EmitterRunner extends AbstractTaskRunner<IEmitterRequest> {

    private final PipelineOutputEmitter outputEmitter;

    public EmitterRunner(Configuration configuration, int index) {
        this.outputEmitter = new PipelineOutputEmitter(configuration, index);
    }

    @Override
    protected void process(IEmitterRequest request) {
        switch (request.getRequestType()) {
            case INIT:
                this.outputEmitter.init((InitEmitterRequest) request);
                break;
            case POP:
                this.outputEmitter.update((UpdateEmitterRequest) request);
                break;
            case CLOSE:
                this.outputEmitter.close((CloseEmitterRequest) request);
                break;
            case STASH:
                this.outputEmitter.stash((StashEmitterRequest) request);
                break;
            case CLEAR:
                this.outputEmitter.clear();
                break;
            default:
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.requestTypeNotSupportError(request.getRequestType().name()));
        }
    }

}
