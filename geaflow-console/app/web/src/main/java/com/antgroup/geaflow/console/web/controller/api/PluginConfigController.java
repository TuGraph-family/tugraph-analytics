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

package com.antgroup.geaflow.console.web.controller.api;

import com.antgroup.geaflow.console.biz.shared.PluginConfigManager;
import com.antgroup.geaflow.console.biz.shared.view.PluginConfigView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.PluginConfigSearch;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/plugin-configs")
public class PluginConfigController {

    @Autowired
    private PluginConfigManager pluginConfigManager;

    @GetMapping
    public GeaflowApiResponse<PageList<PluginConfigView>> searchPluginConfigs(PluginConfigSearch searchView) {
        return GeaflowApiResponse.success(pluginConfigManager.search(searchView));
    }

    @GetMapping("/{id}")
    public GeaflowApiResponse<PluginConfigView> getPluginConfig(@PathVariable String id) {
        return GeaflowApiResponse.success(pluginConfigManager.get(id));
    }

    @PostMapping
    public GeaflowApiResponse<String> createPluginConfig(@RequestBody PluginConfigView view) {
        return GeaflowApiResponse.success(pluginConfigManager.create(view));
    }

    @PutMapping("/{id}")
    public GeaflowApiResponse<Boolean> updatePluginConfig(@PathVariable String id, @RequestBody PluginConfigView view) {
        return GeaflowApiResponse.success(pluginConfigManager.updateById(id, view));
    }

    @DeleteMapping("/{id}")
    public GeaflowApiResponse<Boolean> deletePluginConfig(@PathVariable String id) {
        return GeaflowApiResponse.success(pluginConfigManager.drop(id));
    }

}
