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

package com.antgroup.geaflow.console.common.util.context;

import com.antgroup.geaflow.console.common.util.type.GeaflowRoleType;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowContext {

    public static final String API_PREFIX = "/api";

    private HttpServletRequest request;

    private String taskId;

    private String userId;

    private boolean systemSession;

    private String tenantId;

    private String sessionToken;

    private Set<GeaflowRoleType> roleTypes = new LinkedHashSet<>();

}
