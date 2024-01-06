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

import com.antgroup.geaflow.console.biz.shared.ConfigManager;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.config.ConfigDescItem;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/config")
public class ConfigController {

    @Autowired
    private ConfigManager configManager;

    @GetMapping("/cluster")
    public GeaflowApiResponse<List<ConfigDescItem>> getClusterConfig() {
        return GeaflowApiResponse.success(configManager.getClusterConfig());
    }

    @GetMapping("/job")
    public GeaflowApiResponse<List<ConfigDescItem>> getJobConfig() {
        return GeaflowApiResponse.success(configManager.getJobConfig());
    }

    @GetMapping("/plugin/categories")
    public GeaflowApiResponse<List<GeaflowPluginCategory>> getPluginCategories() {
        return GeaflowApiResponse.success(configManager.getPluginCategories());
    }

    @GetMapping("/plugin/categories/{category}/types")
    public GeaflowApiResponse<List<String>> getPluginCategoryTypes(
        @PathVariable GeaflowPluginCategory category) {
        return GeaflowApiResponse.success(configManager.getPluginCategoryTypes(category));
    }

    @GetMapping("/plugin/categories/{category}/types/{type}")
    public GeaflowApiResponse<List<ConfigDescItem>> getPluginConfig(@PathVariable GeaflowPluginCategory category,
                                                                    @PathVariable String type) {
        return GeaflowApiResponse.success(configManager.getPluginConfig(category, type));
    }

}
