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
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import java.util.List;

public class ShardResult implements IResult<ISliceMeta> {

    /**
     * Use edge id of output info to identify the result.
     */
    private int id;
    private CollectType outputType;
    private List<ISliceMeta> slices;
    private long recordNum;
    private long recordBytes;

    public ShardResult(int id, CollectType outputType, List<ISliceMeta> slices) {
        this.id = id;
        this.outputType = outputType;
        this.slices = slices;
        if (slices != null) {
            for (ISliceMeta sliceMeta : slices) {
                recordNum += sliceMeta.getRecordNum();
                recordBytes += sliceMeta.getEncodedSize();
            }
        }
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public List<ISliceMeta> getResponse() {
        return slices;
    }

    public long getRecordNum() {
        return recordNum;
    }

    public long getRecordBytes() {
        return recordBytes;
    }

    @Override
    public CollectType getType() {
        return outputType;
    }
}
