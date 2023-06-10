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

import com.antgroup.geaflow.console.biz.shared.AuditManager;
import com.antgroup.geaflow.console.biz.shared.view.AuditView;
import com.antgroup.geaflow.console.common.dal.model.AuditSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/audits")
public class AuditController {

    @Autowired
    private AuditManager auditManager;

    @GetMapping
    public GeaflowApiResponse<PageList<AuditView>> searchAudits(AuditSearch search) {
        return GeaflowApiResponse.success(auditManager.search(search));
    }

    @GetMapping("/{id}")
    public GeaflowApiResponse<AuditView> queryAudit(@PathVariable String id) {
        return GeaflowApiResponse.success(auditManager.get(id));
    }

}
