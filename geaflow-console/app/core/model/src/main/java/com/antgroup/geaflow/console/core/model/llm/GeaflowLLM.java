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

package com.antgroup.geaflow.console.core.model.llm;

import com.antgroup.geaflow.console.common.util.type.GeaflowLLMType;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowLLM extends GeaflowName {

    private String url;

    private GeaflowConfig args;

    private GeaflowLLMType type;

    public GeaflowLLM(String name, String comment) {
        super.name = name;
        super.comment = comment;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(url, "Invalid url");
        Preconditions.checkNotNull(type, "Invalid type");
        switch (type) {
            case OPEN_AI:
                args.parse(OpenAIConfigArgsClass.class);
                break;
            default:
                break;
        }

    }

}
