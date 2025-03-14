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

package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.graph.IDynamicGraphStore;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReadOnlyDynamicGraphAccessor<K, VV, EV> extends RWDynamicGraphAccessor<K, VV, EV> {

    private final ReadOnlyGraph<K, VV, EV> readOnlyGraph = new ReadOnlyGraph<>();
    private Lock lock = new ReentrantLock();

    @Override
    public void init(StateContext context, IStoreBuilder storeBuilder) {
        this.readOnlyGraph.init(context, storeBuilder);
    }

    @Override
    protected List<ActionType> allowActionTypes() {
        return this.readOnlyGraph.allowActionTypes();
    }

    @Override
    public void doStoreAction(int shard, ActionType actionType, ActionRequest request) {
        request.setShard(shard);
        lock.lock();
        this.readOnlyGraph.doStoreAction(shard, actionType, request);
        lock.unlock();
    }

    @Override
    public IDynamicGraphStore<K, VV, EV> getStore() {
        return (IDynamicGraphStore<K, VV, EV>) this.readOnlyGraph.getStore();
    }
}
