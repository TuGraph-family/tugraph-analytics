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

import com.antgroup.geaflow.console.biz.shared.InstanceManager;
import com.antgroup.geaflow.console.biz.shared.view.InstanceView;
import com.antgroup.geaflow.console.common.dal.model.InstanceSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
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
@RequestMapping("/instances")
public class InstanceController {

    @Autowired
    private InstanceManager instanceManager;

    @GetMapping
    public GeaflowApiResponse<PageList<InstanceView>> searchInstances(InstanceSearch instanceSearch) {
        return GeaflowApiResponse.success(instanceManager.search(instanceSearch));
    }

    @GetMapping("/{instanceName}")
    public GeaflowApiResponse<InstanceView> queryInstance(@PathVariable String instanceName) {
        return GeaflowApiResponse.success(instanceManager.getByName(instanceName));
    }

    @PostMapping
    public GeaflowApiResponse<String> createInstance(@RequestBody InstanceView instanceView) {
        return GeaflowApiResponse.success(instanceManager.create(instanceView));
    }

    @PutMapping("/{instanceName}")
    public GeaflowApiResponse<Boolean> updateInstance(@PathVariable String instanceName,
                                                      @RequestBody InstanceView instanceView) {
        return GeaflowApiResponse.success(instanceManager.updateByName(instanceName, instanceView));
    }

    @DeleteMapping("/{instanceName}")
    public GeaflowApiResponse<Boolean> deleteInstance(@PathVariable String instanceName) {
        throw new GeaflowException("Delete instance {} not allowed", instanceName);
    }

}
