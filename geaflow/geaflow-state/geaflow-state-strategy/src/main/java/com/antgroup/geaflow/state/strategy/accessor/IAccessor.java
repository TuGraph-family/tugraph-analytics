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

package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.action.IAction;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.IStoreBuilder;

public interface IAccessor {

    void init(StateContext stateContext, IStoreBuilder storeBuilder);

    IBaseStore getStore();

    // action
    void registerAction(IAction action);

    void doStoreAction(int shardId, ActionType actionType, ActionRequest request);
}
