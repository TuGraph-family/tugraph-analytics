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

package com.antgroup.geaflow.runtime.io;

import java.util.List;

public class RawDataInputDesc<T> implements IInputDesc<T> {

    private int edgeId;
    private String edgeName;
    private List<T> rawData;

    public RawDataInputDesc(int edgeId, String edgeName, List<T> rawData) {
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.rawData = rawData;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public String getName() {
        return edgeName;
    }

    @Override
    public List<T> getInput() {
        return rawData;
    }

    @Override
    public InputType getInputType() {
        return InputType.DATA;
    }
}
