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

package com.antgroup.geaflow.state.manage;

import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.strategy.manager.IStateManager;

public class StateOperatorImpl implements StateOperator {

    private final IStateManager stateManager;
    private long checkpointId;

    public StateOperatorImpl(IStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public void finish() {
        this.stateManager.doStoreAction(ActionType.FINISH, null);
    }

    @Override
    public void compact() {
        this.stateManager.doStoreAction(ActionType.COMPACT, null);
    }

    @Override
    public void archive() {
        this.stateManager.doStoreAction(ActionType.ARCHIVE, new ActionRequest<>(checkpointId));
    }

    @Override
    public void recover() {
        this.stateManager.doStoreAction(ActionType.RECOVER, new ActionRequest<>(checkpointId));
    }

    @Override
    public void close() {
        this.stateManager.doStoreAction(ActionType.CLOSE, null);
    }

    @Override
    public void drop() {
        this.stateManager.doStoreAction(ActionType.DROP, null);
    }
}
