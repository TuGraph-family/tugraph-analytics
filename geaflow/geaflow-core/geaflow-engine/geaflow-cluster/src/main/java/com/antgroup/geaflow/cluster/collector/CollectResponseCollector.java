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

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.collector.AbstractCollector;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.collector.IResultCollector;
import com.antgroup.geaflow.shuffle.ResponseOutputDesc;
import com.antgroup.geaflow.shuffle.desc.OutputType;
import java.util.ArrayList;
import java.util.List;

public class CollectResponseCollector<T> extends AbstractCollector
    implements IResultCollector<ResponseResult>, ICollector<T> {
    private int edgeId;
    private OutputType collectorType;
    private String edgeName;
    private final List<T> buffer;
    private final List<T> result;

    public CollectResponseCollector(ResponseOutputDesc outputDesc) {
        super(outputDesc.getOpId());
        this.edgeId = outputDesc.getEdgeId();
        this.collectorType = outputDesc.getType();
        this.edgeName = outputDesc.getEdgeName();
        this.buffer = new ArrayList<>();
        this.result = new ArrayList<>();
    }

    @Override
    public void partition(T value) {
        buffer.add(value);
        this.outputMeter.mark();
    }

    @Override
    public void finish() {
        result.clear();
        result.addAll(buffer);
        buffer.clear();
    }

    @Override
    public String getTag() {
        return edgeName;
    }

    @Override
    public OutputType getType() {
        return collectorType;
    }

    @Override
    public void broadcast(T value) {

    }

    @Override
    public <KEY> void partition(KEY key, T value) {
        partition(value);
    }

    @Override
    public ResponseResult collectResult() {
        ResponseResult responseResult = new ResponseResult(edgeId, getType(), new ArrayList<>(result));
        result.clear();
        return responseResult;
    }
}