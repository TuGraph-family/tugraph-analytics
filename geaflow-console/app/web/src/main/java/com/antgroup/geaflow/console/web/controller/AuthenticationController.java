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

package com.antgroup.geaflow.console.web.controller;

import com.antgroup.geaflow.console.biz.shared.AuthenticationManager;
import com.antgroup.geaflow.console.biz.shared.UserManager;
import com.antgroup.geaflow.console.biz.shared.view.AuthenticationView;
import com.antgroup.geaflow.console.biz.shared.view.LoginView;
import com.antgroup.geaflow.console.biz.shared.view.UserView;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/auth")
public class AuthenticationController {

    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private UserManager userManager;

    @PostMapping("/register")
    @ResponseBody
    public GeaflowApiResponse<String> register(@RequestBody UserView userView) {
        String userId = userManager.register(userView);
        return GeaflowApiResponse.success(userId);
    }

    @PostMapping("/login")
    @ResponseBody
    public GeaflowApiResponse<AuthenticationView> login(@RequestBody LoginView loginView) {
        String loginName = loginView.getLoginName();
        String password = loginView.getPassword();
        boolean systemLogin = loginView.isSystemLogin();
        AuthenticationView authentication = authenticationManager.login(loginName, password, systemLogin);
        return GeaflowApiResponse.success(authentication);
    }

}
