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

package com.antgroup.geaflow.io;

import com.antgroup.geaflow.shuffle.IOutputDesc;

public class ResponseOutputDesc implements IOutputDesc {

    private int opId;
    private int edgeId;
    private String edgeName;
    private CollectType outputType;

    public ResponseOutputDesc(int opId, int edgeId, String edgeName, CollectType outputType) {
        this.opId = opId;
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.outputType = outputType;
    }

    public int getOpId() {
        return opId;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public String getEdgeName() {
        return edgeName;
    }

    @Override
    public CollectType getType() {
        return outputType;
    }
}
