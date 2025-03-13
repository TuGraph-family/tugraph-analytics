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

package com.antgroup.geaflow.console.biz.shared.view;

import com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowFieldType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class FieldView extends NameView {

    private GeaflowFieldType type;

    private GeaflowFieldCategory category;

    public FieldView(String name, String comment, GeaflowFieldType type, GeaflowFieldCategory category) {
        super(name, comment);
        this.type = type;
        this.category = category;
    }
}
