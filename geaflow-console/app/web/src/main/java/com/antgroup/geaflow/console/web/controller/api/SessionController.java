/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.console.web.controller.api;

import com.antgroup.geaflow.console.biz.shared.AuthenticationManager;
import com.antgroup.geaflow.console.biz.shared.view.SessionView;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/session")
public class SessionController {

    @Autowired
    private AuthenticationManager authenticationManager;

    @GetMapping
    public GeaflowApiResponse<SessionView> currentSession() {
        return GeaflowApiResponse.success(authenticationManager.currentSession());
    }

    @PostMapping("/switch")
    @ResponseBody
    public GeaflowApiResponse<Boolean> switchSession() {
        return GeaflowApiResponse.success(authenticationManager.switchSession());
    }

    @PostMapping("/logout")
    @ResponseBody
    public GeaflowApiResponse<Boolean> logout() {
        return GeaflowApiResponse.success(authenticationManager.logout());
    }

}
