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

package com.antgroup.geaflow.cluster.client;

import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.rpc.proto.Driver.PipelineRes;
import java.util.concurrent.CompletableFuture;

public class PipelineResult<R> implements IPipelineResult<R> {

    private final CompletableFuture<PipelineRes> resultFuture;
    private Boolean success;
    private R result;

    public PipelineResult(CompletableFuture<PipelineRes> resultFuture) {
        this.resultFuture = resultFuture;
        this.success = null;
    }

    @Override
    public boolean isSuccess() {
        if (success == null) {
            try {
                PipelineRes pipelineRes = resultFuture.get();
                result = RpcMessageEncoder.decode(pipelineRes.getPayload());
            } catch (Exception e) {
                throw new GeaflowRuntimeException("get pipeline result error", e);
            }
            success = true;
            return true;
        } else {
            return success;
        }
    }

    @Override
    public R get() {
        if (isSuccess()) {
            return result;
        } else {
            throw new GeaflowRuntimeException("failed to get result");
        }
    }
}
