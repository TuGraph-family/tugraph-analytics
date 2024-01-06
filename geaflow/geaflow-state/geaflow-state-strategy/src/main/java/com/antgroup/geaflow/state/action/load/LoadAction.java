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

package com.antgroup.geaflow.state.action.load;

import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.action.BaseAction;
import com.antgroup.geaflow.state.manage.LoadOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadAction extends BaseAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadAction.class);

    @Override
    public void apply(ActionRequest request) {
        LoadOption option = (LoadOption) request.getRequest();
        if (option.getKeyGroup() == null || option.getKeyGroup().contains(request.getShard())) {
            LOGGER.info("base store recover version {}", option.getCheckPointId());
            context.getBaseStore().recovery(option.getCheckPointId());
        } else {
            LOGGER.warn("key group is null or key group {} not contain shard {}, ignore",
                option.getKeyGroup(), request.getShard());
        }
    }

    @Override
    public ActionType getActionType() {
        return ActionType.LOAD;
    }
}
