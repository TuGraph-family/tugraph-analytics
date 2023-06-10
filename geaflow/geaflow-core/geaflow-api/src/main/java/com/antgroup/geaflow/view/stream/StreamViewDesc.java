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

package com.antgroup.geaflow.view.stream;

import com.antgroup.geaflow.api.partition.IPartition;
import com.antgroup.geaflow.view.IViewDesc;
import java.util.HashMap;

public class StreamViewDesc implements IViewDesc {

    private String viewName;
    private int shardNum;
    private BackendType backend;
    private IPartition partitioner;
    private HashMap props;

    public StreamViewDesc(String viewName, int shardNum, BackendType backend) {
        this.viewName = viewName;
        this.shardNum = shardNum;
        this.backend = backend;
    }

    public StreamViewDesc(String viewName, int shardNum, BackendType backend,
                          IPartition partitioner, HashMap props) {
        this(viewName, shardNum, backend);
        this.partitioner = partitioner;
        this.props = props;
    }

    @Override
    public String getName() {
        return this.viewName;
    }

    @Override
    public int getShardNum() {
        return this.shardNum;
    }

    @Override
    public DataModel getDataModel() {
        return DataModel.TABLE;
    }

    @Override
    public BackendType getBackend() {
        return backend;
    }

    @Override
    public HashMap getViewProps() {
        return this.props;
    }
}
