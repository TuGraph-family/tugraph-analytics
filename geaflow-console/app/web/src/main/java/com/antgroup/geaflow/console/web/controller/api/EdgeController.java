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

import com.antgroup.geaflow.console.biz.shared.EdgeManager;
import com.antgroup.geaflow.console.biz.shared.view.EdgeView;
import com.antgroup.geaflow.console.common.dal.model.EdgeSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
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
@RequestMapping
public class EdgeController {

    @Autowired
    private EdgeManager edgeManager;

    @GetMapping("/edges")
    public GeaflowApiResponse<PageList<EdgeView>> searchEdges(EdgeSearch search) {
        return GeaflowApiResponse.success(edgeManager.search(search));
    }


    @GetMapping("/instances/{instanceName}/edges")
    public GeaflowApiResponse<PageList<EdgeView>> searchEdges(@PathVariable("instanceName") String instanceName,
                                                              EdgeSearch search) {
        return GeaflowApiResponse.success(edgeManager.searchByInstanceName(instanceName, search));
    }

    @GetMapping("/instances/{instanceName}/edges/{edgeName}")
    public GeaflowApiResponse<EdgeView> getEdge(@PathVariable("instanceName") String instanceName,
                                                @PathVariable("edgeName") String edgeName) {
        return GeaflowApiResponse.success(edgeManager.getByName(instanceName, edgeName));
    }

    @PostMapping("/instances/{instanceName}/edges")
    public GeaflowApiResponse<String> create(@PathVariable("instanceName") String instanceName,
                                             @RequestBody EdgeView edgeView) {
        return GeaflowApiResponse.success(edgeManager.create(instanceName, edgeView));
    }

    @PutMapping("/instances/{instanceName}/edges/{edgeName}")
    public GeaflowApiResponse<Boolean> update(@PathVariable("instanceName") String instanceName,
                                              @PathVariable("edgeName") String edgeName,
                                              @RequestBody EdgeView edgeView) {
        return GeaflowApiResponse.success(edgeManager.updateByName(instanceName, edgeName, edgeView));
    }


    @DeleteMapping("/instances/{instanceName}/edges/{edgeName}")
    public GeaflowApiResponse<Boolean> drop(@PathVariable("instanceName") String instanceName,
                                            @PathVariable("edgeName") String edgeName) {
        return GeaflowApiResponse.success(edgeManager.dropByName(instanceName, edgeName));
    }

}
