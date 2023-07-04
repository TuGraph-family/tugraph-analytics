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

import com.antgroup.geaflow.console.biz.shared.FunctionManager;
import com.antgroup.geaflow.console.biz.shared.view.FunctionView;
import com.antgroup.geaflow.console.common.dal.model.FunctionSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class FunctionController {

    @Autowired
    private FunctionManager functionManager;

    @GetMapping("/instances/{instanceName}/functions")
    public GeaflowApiResponse<PageList<FunctionView>> instanceSearch(@PathVariable("instanceName") String instanceName,
                                                                     FunctionSearch search) {
        return GeaflowApiResponse.success(functionManager.searchByInstanceName(instanceName, search));
    }

    @GetMapping("/instances/{instanceName}/functions/{functionName}")
    public GeaflowApiResponse<FunctionView> getFunction(@PathVariable String instanceName,
                                                        @PathVariable String functionName) {
        return GeaflowApiResponse.success(functionManager.getByName(instanceName, functionName));
    }

    @PostMapping("/instances/{instanceName}/functions")
    public GeaflowApiResponse<String> createFunction(@PathVariable String instanceName, FunctionView view,
                                                     @RequestParam(required = false) MultipartFile functionFile,
                                                     @RequestParam(required = false) String fileId) {
        return GeaflowApiResponse.success(functionManager.createFunction(instanceName, view, functionFile, fileId));
    }

    @PutMapping("/instances/{instanceName}/functions/{functionName}")
    public GeaflowApiResponse<Boolean> updateFunction(@PathVariable String instanceName, @PathVariable String functionName,
                                                      FunctionView view, @RequestParam(required = false) MultipartFile functionFile) {
        return GeaflowApiResponse.success(functionManager.updateFunction(instanceName, functionName, view, functionFile));
    }

    @DeleteMapping("/instances/{instanceName}/functions/{functionName}")
    public GeaflowApiResponse<Boolean> deleteFunction(@PathVariable String instanceName,
                                                      @PathVariable String functionName) {
        return GeaflowApiResponse.success(functionManager.deleteFunction(instanceName, functionName));
    }
}
