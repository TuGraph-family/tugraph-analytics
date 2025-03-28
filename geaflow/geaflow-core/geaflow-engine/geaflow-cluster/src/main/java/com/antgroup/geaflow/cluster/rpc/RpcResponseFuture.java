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

package com.antgroup.geaflow.cluster.rpc;

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.rpc.proto.Container;
import com.google.protobuf.ByteString;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

public class RpcResponseFuture implements Future<IEvent> {

    private final Future<IEvent> delegate;

    public RpcResponseFuture(Future<IEvent> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public IEvent get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    @Override
    public IEvent get(long timeout, @NotNull TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }

    private IEvent getEvent(Container.Response response) {
        ByteString payload = response.getPayload();
        if (payload == ByteString.EMPTY) {
            return null;
        } else {
            return RpcMessageEncoder.decode(payload);
        }
    }
}
