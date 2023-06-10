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

package com.antgroup.geaflow.store.context;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.state.schema.GraphDataSchema;
import com.antgroup.geaflow.state.serializer.IKeySerializer;
import com.google.common.base.Preconditions;

public class StoreContext {

    private String name;
    private Configuration config;
    private MetricGroup metricGroup;

    private int shardId;
    private long version;
    private GraphDataSchema graphSchema;
    private IKeySerializer keySerializer;

    public StoreContext(String name) {
        this.name = name;
    }

    public StoreContext withConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public StoreContext withShardId(int shardId) {
        this.shardId = shardId;
        return this;
    }

    public StoreContext withMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public StoreContext withVersion(long version) {
        this.version = version;
        return this;
    }

    public StoreContext withName(String name) {
        this.name = name;
        return this;
    }

    public StoreContext withDataSchema(GraphDataSchema dataSchema) {
        this.graphSchema = dataSchema;
        return this;
    }

    public StoreContext withKeySerializer(IKeySerializer keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public Configuration getConfig() {
        return config;
    }

    public int getShardId() {
        return shardId;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public GraphDataSchema getGraphSchema() {
        return Preconditions.checkNotNull(graphSchema, "GraphMeta must be set");
    }

    public IKeySerializer getKeySerializer() {
        return keySerializer;
    }
}
