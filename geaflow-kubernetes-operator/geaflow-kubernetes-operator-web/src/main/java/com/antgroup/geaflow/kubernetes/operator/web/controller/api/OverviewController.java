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
import com.antgroup.geaflow.kubernetes.operator.core.model.job.JobState;
import com.antgroup.geaflow.kubernetes.operator.core.model.view.ClusterOverview;
import com.antgroup.geaflow.kubernetes.operator.core.util.CommonUtil;
import com.antgroup.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import com.antgroup.geaflow.kubernetes.operator.web.api.ApiResponse;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/overview")
@RestController
public class OverviewController {

    @Autowired
    private GeaflowJobApiService geaflowJobApiService;

    @GetMapping
    public ApiResponse<ClusterOverview> getOverview() {
        ClusterOverview overview = new ClusterOverview();
        KubernetesClient client = KubernetesUtil.getKubernetesClient();
        overview.setHost(CommonUtil.getHostName());
        overview.setNamespace(client.getNamespace());
        overview.setMasterUrl(client.getConfiguration().getMasterUrl());

        Collection<GeaflowJob> jobList = geaflowJobApiService.queryJobs();
        overview.setTotalJobNum(jobList.size());
        Map<JobState, Long> jobStateNumMap =
            jobList.stream().filter(job -> job.getStatus().getState() != null).collect(Collectors.groupingBy(job -> job.getStatus().getState(),
            Collectors.counting()));
        overview.setJobStateNumMap(jobStateNumMap);
        return ApiResponse.success(overview);
    }

}
