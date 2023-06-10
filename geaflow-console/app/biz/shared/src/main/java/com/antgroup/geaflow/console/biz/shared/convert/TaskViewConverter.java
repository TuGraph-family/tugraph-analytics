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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.TaskView;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TaskViewConverter extends IdViewConverter<GeaflowTask, TaskView> {

    @Autowired
    protected PluginConfigViewConverter pluginConfigViewConverter;

    @Autowired
    protected ReleaseViewConverter releaseViewConverter;

    @Override
    protected TaskView modelToView(GeaflowTask model) {
        TaskView view = super.modelToView(model);
        view.setRelease(releaseViewConverter.convert(model.getRelease()));
        view.setType(model.getType());
        view.setStatus(model.getStatus());
        view.setStartTime(model.getStartTime());
        view.setEndTime(model.getEndTime());
        view.setRuntimeMetaPluginConfig(pluginConfigViewConverter.modelToView(model.getRuntimeMetaPluginConfig()));
        view.setHaMetaPluginConfig(pluginConfigViewConverter.modelToView(model.getHaMetaPluginConfig()));
        view.setMetricPluginConfig(pluginConfigViewConverter.modelToView(model.getMetricPluginConfig()));
        view.setDataPluginConfig(pluginConfigViewConverter.modelToView(model.getDataPluginConfig()));
        return view;
    }

    public GeaflowTask convert(TaskView view) {
        return viewToModel(view);
    }

}
