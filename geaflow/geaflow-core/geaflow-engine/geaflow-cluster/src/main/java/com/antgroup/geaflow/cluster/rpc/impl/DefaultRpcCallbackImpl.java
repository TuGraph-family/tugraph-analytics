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

package com.antgroup.geaflow.cluster.rpc.impl;

import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import com.antgroup.geaflow.ha.service.IHAService;

public class DefaultRpcCallbackImpl<T> implements RpcCallback<T> {

    private RpcCallback<T> rpcCallback;

    private String resourceId;

    private IHAService haService;

    public DefaultRpcCallbackImpl(RpcCallback<T> rpcCallback, String resourceId, IHAService haService) {
        this.rpcCallback = rpcCallback;
        this.resourceId = resourceId;
        this.haService = haService;
    }

    public DefaultRpcCallbackImpl() {

    }

    @Override
    public void onSuccess(T value) {
        if (rpcCallback != null) {
            rpcCallback.onSuccess(value);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        if (rpcCallback != null) {
            rpcCallback.onFailure(t);
        }

        if (resourceId != null) {
            haService.invalidateResource(resourceId);
        }
    }

}
