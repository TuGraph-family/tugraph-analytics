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
import com.antgroup.geaflow.state.action.EmptyAction;
import com.antgroup.geaflow.state.action.IAction;
import com.antgroup.geaflow.state.action.StateActionContext;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.store.IBaseStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseActionAccess {

    protected Map<ActionType, IAction> registeredAction = new HashMap<>();

    protected abstract List<ActionType> allowActionTypes();

    protected void initAction(IBaseStore baseStore, StateContext stateContext) {
        List<ActionType> allowActionTypes = allowActionTypes();
        for (ActionType actionType : ActionType.values()) {
            if (allowActionTypes.contains(actionType)) {
                IAction action = ActionBuilder.build(actionType);
                action.init(new StateActionContext(baseStore, stateContext.getConfig()));
                this.registerAction(action);
            } else {
                this.registerAction(new EmptyAction(actionType));
            }
        }
    }

    public void registerAction(IAction action) {
        if (action != null) {
            this.registeredAction.put(action.getActionType(), action);
        }
    }

    public void doStoreAction(ActionType actionType, ActionRequest request) {
        this.registeredAction.get(actionType).apply(request);
    }
}
