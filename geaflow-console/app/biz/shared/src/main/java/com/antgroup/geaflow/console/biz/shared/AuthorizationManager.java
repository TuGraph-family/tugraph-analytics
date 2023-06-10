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

package com.antgroup.geaflow.console.biz.shared;

import com.antgroup.geaflow.console.biz.shared.view.AuthorizationView;
import com.antgroup.geaflow.console.common.dal.model.AuthorizationSearch;
import com.antgroup.geaflow.console.common.util.exception.GeaflowSecurityException;
import com.antgroup.geaflow.console.common.util.type.GeaflowRoleType;
import com.antgroup.geaflow.console.core.model.security.GeaflowAuthority;
import com.antgroup.geaflow.console.core.model.security.GeaflowRole;
import com.antgroup.geaflow.console.core.model.security.resource.GeaflowResource;
import java.util.List;

public interface AuthorizationManager  extends IdManager<AuthorizationView, AuthorizationSearch> {

    List<GeaflowRoleType> getUserRoleTypes(String userId);

    void hasRole(GeaflowRole... roles) throws GeaflowSecurityException;

    void hasAuthority(GeaflowAuthority authority, GeaflowResource resource) throws GeaflowSecurityException;
}
