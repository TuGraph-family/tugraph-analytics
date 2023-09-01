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

import static com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory.VERTEX_ID;
import static com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory.VERTEX_LABEL;

import com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowVertex extends GeaflowStruct {

    public GeaflowVertex() {
        super(GeaflowStructType.VERTEX);
    }

    public GeaflowVertex(String name, String comment) {
        this();
        super.name = name;
        super.comment = comment;
    }

    @Override
    public void validate() {
        super.validate();

        int idCount = 0;
        int labelCount = 0;
        for (GeaflowField value : fields.values()) {
            GeaflowFieldCategory category = value.getCategory();
            if (category == VERTEX_ID) {
                idCount++;
            }

            if (category == VERTEX_LABEL) {
                labelCount++;
            }
        }

        VERTEX_ID.validate(idCount);
        VERTEX_LABEL.validate(labelCount);

    }
}
