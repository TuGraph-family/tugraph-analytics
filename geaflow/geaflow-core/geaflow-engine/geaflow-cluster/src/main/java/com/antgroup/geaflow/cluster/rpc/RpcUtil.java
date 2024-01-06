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

package com.antgroup.geaflow.cluster.rpc;

import com.baidu.brpc.client.RpcCallback;
import io.grpc.Context;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public class RpcUtil implements Serializable {

    public static void asyncExecute(Runnable runnable) {
        Context.current().fork().run(runnable);
    }

    /**
     * Build brpc callback.
     */
    public static <T> RpcCallback<T> buildRpcCallback(RpcEndpointRef.RpcCallback<T> listener,
                                                      CompletableFuture<T> result) {
        return new RpcCallback<T>() {
                @Override
                public void success(T response) {
                    if (listener != null) {
                        listener.onSuccess(response);
                    }
                    result.complete(response);
                }

                @Override
                public void fail(Throwable t) {
                    if (listener != null) {
                        listener.onFailure(t);
                    }
                    result.completeExceptionally(t);
                }
            };
    }


}
