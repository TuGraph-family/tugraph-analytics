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

package com.antgroup.geaflow.state.descriptor;

import static com.google.common.base.Preconditions.checkNotNull;

import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.graph.StateMode;
import com.antgroup.geaflow.utils.keygroup.IKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import java.io.Serializable;

public abstract class BaseStateDescriptor implements Serializable {

    protected String name;
    protected String storeType;
    protected KeyGroup keyGroup;
    protected MetricGroup metricGroup;
    protected IKeyGroupAssigner assigner;
    protected StateMode stateMode = StateMode.RW;
    protected DataModel dateModel;

    protected BaseStateDescriptor(String name, String storeType) {
        this.name = name;
        this.storeType = storeType;
    }

    public BaseStateDescriptor withName(String name) {
        this.name = name;
        return this;
    }

    public BaseStateDescriptor withMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public BaseStateDescriptor withKeyGroup(KeyGroup keyGroup) {
        this.keyGroup = keyGroup;
        return this;
    }

    public BaseStateDescriptor withDataModel(DataModel dataModel) {
        this.dateModel = dataModel;
        return this;
    }

    public BaseStateDescriptor withStateMode(StateMode stateMode) {
        this.stateMode = stateMode;
        return this;
    }

    public BaseStateDescriptor withKeyGroupAssigner(IKeyGroupAssigner assigner) {
        this.assigner = assigner;
        return this;
    }

    public StateMode getStateMode() {
        return stateMode;
    }

    public DataModel getDateModel() {
        return dateModel;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public KeyGroup getKeyGroup() {
        return checkNotNull(keyGroup, "keyGroup must be set");
    }

    public String getName() {
        return checkNotNull(name, "descriptor name must be set");
    }

    public String getStoreType() {
        return checkNotNull(storeType, "storeType must be set");
    }

    public IKeyGroupAssigner getAssigner() {
        return assigner;
    }

    public abstract DescriptorType getDescriptorType();
}
