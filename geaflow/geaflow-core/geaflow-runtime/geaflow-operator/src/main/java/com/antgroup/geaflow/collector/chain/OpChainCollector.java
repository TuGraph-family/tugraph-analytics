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

package com.antgroup.geaflow.collector.chain;

import com.antgroup.geaflow.collector.AbstractCollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.base.window.OneInputOperator;

public class OpChainCollector<T> extends AbstractCollector implements IChainCollector<T> {

    protected OneInputOperator<T> operator;

    public OpChainCollector(int id, Operator operator) {
        super(id);
        this.operator = (OneInputOperator<T>) operator;
    }

    @Override
    public void process(T value) {
        try {
            this.operator.processElement(value);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public String getTag() {
        return String.format("%s-%s", ((AbstractOperator) operator).getOpArgs().getOpName(),
            ((AbstractOperator) operator).getOpArgs().getOpId());
    }

    @Override
    public CollectType getType() {
        return CollectType.FORWARD;
    }
}
