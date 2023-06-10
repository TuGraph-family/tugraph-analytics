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

package com.antgroup.geaflow.console.core.model.data;

import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.common.util.type.GeaflowViewCategory;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowView extends GeaflowStruct {

    private GeaflowViewCategory category = GeaflowViewCategory.LOGICAL;

    private GeaflowCode code;

    public GeaflowView() {
        super(GeaflowStructType.VIEW);
    }
}
