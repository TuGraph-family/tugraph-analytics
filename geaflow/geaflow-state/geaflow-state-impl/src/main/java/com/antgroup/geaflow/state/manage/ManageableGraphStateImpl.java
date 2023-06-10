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

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;

public class ManageableGraphStateImpl extends ManageableStateImpl implements ManageableGraphState {

    public ManageableGraphStateImpl(IGraphManager graphManager) {
        super(graphManager);
    }

    @Override
    public GraphStateSummary summary() {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public StateMetric metric() {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
