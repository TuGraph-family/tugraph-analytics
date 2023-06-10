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

package com.antgroup.geaflow.console.core.model.security;

import com.antgroup.geaflow.console.common.util.DateTimeUtil;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowAuthentication {

    private String userId;

    private String sessionToken;

    private boolean systemSession;

    private Date accessTime;

    public boolean isExpired(int liveSeconds) {
        if (sessionToken == null) {
            return true;
        }

        return DateTimeUtil.isExpired(accessTime, liveSeconds);
    }
}
