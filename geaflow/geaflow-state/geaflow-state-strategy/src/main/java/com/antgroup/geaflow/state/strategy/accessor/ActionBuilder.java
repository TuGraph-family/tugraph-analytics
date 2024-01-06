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

import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.action.EmptyAction;
import com.antgroup.geaflow.state.action.IAction;
import com.antgroup.geaflow.state.action.archive.ArchiveAction;
import com.antgroup.geaflow.state.action.close.CloseAction;
import com.antgroup.geaflow.state.action.compact.CompactAction;
import com.antgroup.geaflow.state.action.drop.DropAction;
import com.antgroup.geaflow.state.action.finish.FinishAction;
import com.antgroup.geaflow.state.action.load.LoadAction;
import com.antgroup.geaflow.state.action.recovery.RecoveryAction;

public class ActionBuilder {

    public static IAction build(ActionType actionType) {
        switch (actionType) {
            case ARCHIVE:
                return new ArchiveAction();
            case RECOVER:
                return new RecoveryAction();
            case FINISH:
                return new FinishAction();
            case CLOSE:
                return new CloseAction();
            case DROP:
                return new DropAction();
            case COMPACT:
                return new CompactAction();
            case LOAD:
                return new LoadAction();
            default:
                return new EmptyAction(actionType);
        }
    }
}
