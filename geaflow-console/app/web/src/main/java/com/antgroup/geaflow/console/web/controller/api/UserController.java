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
import com.antgroup.geaflow.console.biz.shared.UserManager;
import com.antgroup.geaflow.console.biz.shared.view.UserView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.UserSearch;
import com.antgroup.geaflow.console.core.model.security.GeaflowRole;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    private UserManager userManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<UserView>> searchUsers(UserSearch search) {
        return GeaflowApiResponse.success(userManager.search(search));
    }

    @GetMapping("/{userId}")
    public GeaflowApiResponse<UserView> getUser(@PathVariable String userId) {
        return GeaflowApiResponse.success(userManager.getUser(userId));
    }

    @PostMapping
    public GeaflowApiResponse<String> addUser(UserView userView) {
        authorizationManager.hasRole(GeaflowRole.TENANT_ADMIN);
        return GeaflowApiResponse.success(userManager.addUser(userView));
    }

    @PutMapping("/{userId}")
    public GeaflowApiResponse<Boolean> updateUser(@PathVariable String userId, UserView userView) {
        return GeaflowApiResponse.success(userManager.updateUser(userId, userView));
    }

    @DeleteMapping("/{userId}")
    public GeaflowApiResponse<Boolean> deleteUser(@PathVariable String userId) {
        authorizationManager.hasRole(GeaflowRole.TENANT_ADMIN);
        return GeaflowApiResponse.success(userManager.deleteUser(userId));
    }
}
