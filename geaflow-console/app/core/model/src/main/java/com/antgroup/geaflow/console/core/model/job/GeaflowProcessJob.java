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
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowProcessJob extends GeaflowCodeJob {


    public GeaflowProcessJob() {
        super(GeaflowJobType.PROCESS);
    }

    @Override
    public List<GeaflowFunction> getFunctions() {
        return functions;
    }

    @Override
    public List<StructMapping> getStructMappings() {
        return new ArrayList<>();
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(userCode, "user code is null");
    }
}
