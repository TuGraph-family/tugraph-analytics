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

package com.antgroup.geaflow.console.core.model.job;

import com.antgroup.geaflow.console.common.util.type.GeaflowJobType;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import java.util.Optional;

public abstract class GeaflowCodeJob extends GeaflowJob {

    protected GeaflowCode userCode;

    public GeaflowCodeJob(GeaflowJobType type) {
        super(type);
    }

    @Override
    public GeaflowRemoteFile getJarPackage() {
        return null;
    }

    @Override
    public String getEntryClass() {
        return null;
    }

    @Override
    public boolean isApiJob() {
        return false;
    }

    @Override
    public GeaflowCode getUserCode() {
        return userCode;
    }

    public void setUserCode(String code) {
        this.userCode = Optional.ofNullable(code).map(e -> new GeaflowCode(code)).orElse(null);
    }
}
