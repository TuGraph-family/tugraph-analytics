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

import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import java.util.List;

public class ShardResult implements IResult<ISliceMeta> {

    /**
     * Use edge id of output info to identify the result.
     */
    private int id;
    private List<ISliceMeta> slices;

    public ShardResult(int id, List<ISliceMeta> slices) {
        this.id = id;
        this.slices = slices;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public List<ISliceMeta> getResponse() {
        return slices;
    }

    @Override
    public ResponseType getType() {
        return ResponseType.SHARD;
    }
}
