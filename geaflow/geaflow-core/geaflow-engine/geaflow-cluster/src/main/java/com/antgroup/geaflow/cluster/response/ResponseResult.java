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

package com.antgroup.geaflow.cluster.response;

import com.antgroup.geaflow.io.CollectType;
import java.util.List;

public class ResponseResult implements IResult<Object> {

    private int collectId;
    private CollectType outputType;
    private List<Object> responses;

    public ResponseResult(int collectId, CollectType outputType, List<Object> responses) {
        this.collectId = collectId;
        this.outputType = outputType;
        this.responses = responses;
    }

    @Override
    public int getId() {
        return collectId;
    }

    @Override
    public List<Object> getResponse() {
        return responses;
    }

    @Override
    public CollectType getType() {
        return outputType;
    }
}

