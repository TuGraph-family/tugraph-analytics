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

package com.antgroup.geaflow.operator.impl.window.incremental;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.ReduceFunction;
import com.antgroup.geaflow.api.trait.CheckpointTrait;
import com.antgroup.geaflow.api.trait.TransactionTrait;
import com.antgroup.geaflow.operator.base.window.AbstractOneInputOperator;
import com.antgroup.geaflow.state.KeyValueState;
import com.antgroup.geaflow.state.StateFactory;
import com.antgroup.geaflow.state.descriptor.KeyValueStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.IKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignment;

public class IncrReduceOperator<KEY, T> extends AbstractOneInputOperator<T, ReduceFunction<T>>
    implements TransactionTrait, CheckpointTrait {

    private transient KeyValueState<KEY, T> aggregatingState;
    private final KeySelector<T, KEY> keySelector;

    public IncrReduceOperator(ReduceFunction<T> function, KeySelector<T, KEY> keySelector) {
        super(function);
        this.keySelector = keySelector;
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        KeyValueStateDescriptor descriptor = KeyValueStateDescriptor.build(
            getIdentify(),
            this.runtimeContext.getConfiguration().getString(SYSTEM_STATE_BACKEND_TYPE));
        int taskIndex = this.runtimeContext.getTaskArgs().getTaskIndex();
        int parallelism = this.runtimeContext.getTaskArgs().getParallelism();
        int maxParallelism = this.runtimeContext.getTaskArgs().getMaxParallelism();
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism, parallelism, taskIndex);
        descriptor.withKeyGroup(keyGroup);
        IKeyGroupAssigner keyGroupAssigner = KeyGroupAssignerFactory.createKeyGroupAssigner(
            keyGroup, taskIndex, maxParallelism);
        descriptor.withKeyGroupAssigner(keyGroupAssigner);
        this.aggregatingState = StateFactory.buildKeyValueState(descriptor, this.runtimeContext.getConfiguration());
    }

    @Override
    protected void process(T value) throws Exception {
        KEY key = keySelector.getKey(value);
        T oldValue = aggregatingState.get(key);

        T newValue;
        if (oldValue == null) {
            newValue = value;
        } else {
            newValue = function.reduce(oldValue, value);
        }

        aggregatingState.put(key, newValue);
        if (newValue != null) {
            collectValue(newValue);
        }
    }

    @Override
    public void finish(long windowId) {

    }

    @Override
    public void checkpoint(long windowId) {
        this.aggregatingState.manage().operate().setCheckpointId(windowId);
        this.aggregatingState.manage().operate().finish();
        this.aggregatingState.manage().operate().archive();
    }

    @Override
    public void rollback(long windowId) {
        this.aggregatingState.manage().operate().setCheckpointId(windowId);
        this.aggregatingState.manage().operate().recover();
    }
}
