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

package com.antgroup.geaflow.processor.impl;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.trait.CancellableTrait;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.processor.Processor;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProcessor<T, R, OP extends Operator> implements Processor<T, R>, CancellableTrait {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProcessor.class);

    protected OP operator;
    protected List<ICollector> collectors;
    protected RuntimeContext runtimeContext;

    public AbstractProcessor(OP operator) {
        this.operator = operator;
    }

    public OP getOperator() {
        return operator;
    }

    @Override
    public int getId() {
        return ((AbstractOperator) operator).getOpArgs().getOpId();
    }

    @Override
    public void open(List<ICollector> collectors, RuntimeContext runtimeContext) {
        this.collectors = collectors;
        this.runtimeContext = runtimeContext;
        this.operator.open(new AbstractOperator.DefaultOpContext(collectors, runtimeContext));
    }

    @Override
    public void init(long windowId) {
    }

    @Override
    public void finish(long windowId) {
        operator.finish();
    }

    @Override
    public void close() {
        this.operator.close();
    }

    @Override
    public void cancel() {
        if (this.operator instanceof CancellableTrait) {
            ((CancellableTrait) this.operator).cancel();
        }
    }

    @Override
    public String toString() {
        return operator.getClass().getSimpleName();
    }
}
