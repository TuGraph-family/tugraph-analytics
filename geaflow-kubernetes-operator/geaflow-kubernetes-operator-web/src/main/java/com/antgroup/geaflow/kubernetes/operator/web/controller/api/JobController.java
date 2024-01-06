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

package com.antgroup.geaflow.kubernetes.operator.web.controller.api;

import com.antgroup.geaflow.kubernetes.operator.core.job.GeaflowJobApiService;
import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import com.antgroup.geaflow.kubernetes.operator.web.api.ApiResponse;
import com.antgroup.geaflow.kubernetes.operator.web.converter.GeaflowJobConverter;
import com.antgroup.geaflow.kubernetes.operator.web.view.GeaflowJobView;
import java.util.Collection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private GeaflowJobApiService geaflowJobApiService;

    @GetMapping
    public ApiResponse<Collection<GeaflowJob>> queryGeaflowJobs() {
        return ApiResponse.success(geaflowJobApiService.queryJobs());
    }

    @PostMapping
    public ApiResponse<Void> createGeaflowJob(@RequestBody GeaflowJobView geaflowJobView) {
        GeaflowJob geaflowJob = GeaflowJobConverter.convert2GeaflowJob(geaflowJobView);
        geaflowJobApiService.createJob(geaflowJob);
        return ApiResponse.success(null);
    }

    @DeleteMapping(path = "/{name}")
    public ApiResponse<Void> deleteGeaflowJob(@PathVariable("name") String name) {
        geaflowJobApiService.deleteJob(name);
        return ApiResponse.success(null);
    }


}
