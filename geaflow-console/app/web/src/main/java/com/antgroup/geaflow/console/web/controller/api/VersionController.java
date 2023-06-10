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
import com.antgroup.geaflow.console.biz.shared.VersionManager;
import com.antgroup.geaflow.console.biz.shared.view.VersionView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.VersionSearch;
import com.antgroup.geaflow.console.core.model.security.GeaflowRole;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/versions")
public class VersionController {

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<VersionView>> searchVersions(VersionSearch search) {
        return GeaflowApiResponse.success(versionManager.searchVersions(search));
    }

    @GetMapping("/{versionName}")
    public GeaflowApiResponse<VersionView> getVersion(@PathVariable String versionName) {
        return GeaflowApiResponse.success(versionManager.getVersion(versionName));
    }

    @PostMapping
    public GeaflowApiResponse<String> createVersion(VersionView view,
                                                    @RequestParam(required = false) MultipartFile engineJarFile,
                                                    @RequestParam(required = false) MultipartFile langJarFile) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(versionManager.createVersion(view, engineJarFile, langJarFile));
    }

    @PutMapping("/{versionName}")
    public GeaflowApiResponse<Boolean> updateVersion(@PathVariable String versionName, VersionView view,
                                                     @RequestParam(required = false) MultipartFile engineJarFile,
                                                     @RequestParam(required = false) MultipartFile langJarFile) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(versionManager.updateVersion(versionName, view, engineJarFile, langJarFile));
    }

    @DeleteMapping("/{versionName}")
    public GeaflowApiResponse<Boolean> deleteVersion(@PathVariable String versionName) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(versionManager.deleteVersion(versionName));
    }

}
