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
import com.antgroup.geaflow.console.biz.shared.view.AuthorizationView;
import com.antgroup.geaflow.console.common.dal.model.AuthorizationSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/authorizations")
public class AuthorizationController {

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<AuthorizationView>> searchAuthorizations(AuthorizationSearch search) {
        return GeaflowApiResponse.success(authorizationManager.search(search));
    }

    @GetMapping( "/{id}")
    public GeaflowApiResponse<AuthorizationView> queryAuthorization(@PathVariable String id) {
        return  GeaflowApiResponse.success(authorizationManager.get(id));
    }

    @PostMapping
    public GeaflowApiResponse<String> applyAuthorization(@RequestBody AuthorizationView view) {
        return GeaflowApiResponse.success(authorizationManager.create(view));
    }

    @DeleteMapping("/{id}")
    public GeaflowApiResponse<Boolean> deleteAuthorization(@PathVariable String id) {
        return GeaflowApiResponse.success(authorizationManager.drop(id));
    }
}
