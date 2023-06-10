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

import com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowFieldType;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode(of = {"type", "category"}, callSuper = true)
public class GeaflowField extends GeaflowName {

    private GeaflowFieldType type;

    private GeaflowFieldCategory category = GeaflowFieldCategory.PROPERTY;

    public GeaflowField(String name, String comment, GeaflowFieldType type, GeaflowFieldCategory category) {
        super.name = name;
        super.comment = comment;
        this.type = type;
        this.category = category;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(type, "Invalid type");
        Preconditions.checkNotNull(category, "Invalid constraint");
    }
}
