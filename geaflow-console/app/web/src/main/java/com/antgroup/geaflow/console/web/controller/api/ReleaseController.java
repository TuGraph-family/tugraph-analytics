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

import com.antgroup.geaflow.console.biz.shared.AuthorizationManager;
import com.antgroup.geaflow.console.biz.shared.ReleaseManager;
import com.antgroup.geaflow.console.biz.shared.view.ReleaseUpdateView;
import com.antgroup.geaflow.console.biz.shared.view.ReleaseView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.ReleaseSearch;
import com.antgroup.geaflow.console.core.model.security.GeaflowAuthority;
import com.antgroup.geaflow.console.core.service.security.Resources;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReleaseController {

    @Autowired
    private ReleaseManager releaseManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping("jobs/{jobId}/releases")
    public GeaflowApiResponse<PageList<ReleaseView>> searchReleases(@PathVariable("jobId") String jobId, ReleaseSearch search) {
        search.setJobId(jobId);
        return GeaflowApiResponse.success(releaseManager.search(search));
    }

    @PostMapping("/jobs/{jobId}/releases")
    public GeaflowApiResponse<String> publish(@PathVariable("jobId") String jobId) {
        authorizationManager.hasAuthority(GeaflowAuthority.EXECUTE, Resources.job(jobId));
        return GeaflowApiResponse.success(releaseManager.publish(jobId));
    }

    @PutMapping("jobs/{jobId}/releases")
    public GeaflowApiResponse<Boolean> updateRelease(@PathVariable("jobId") String jobId,
                                                     @RequestBody ReleaseUpdateView updateView) {
        authorizationManager.hasAuthority(GeaflowAuthority.UPDATE, Resources.job(jobId));
        return GeaflowApiResponse.success(releaseManager.updateRelease(jobId, updateView));
    }


}
