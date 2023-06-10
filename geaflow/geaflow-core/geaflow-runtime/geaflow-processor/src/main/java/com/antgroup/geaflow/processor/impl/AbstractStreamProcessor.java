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

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.trait.CheckpointTrait;
import com.antgroup.geaflow.api.trait.TransactionTrait;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.utils.CheckpointUtil;
import com.antgroup.geaflow.model.record.BatchRecord;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStreamProcessor<T, R, OP extends Operator> extends AbstractProcessor<T, R, OP> implements TransactionTrait {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamProcessor.class);

    protected Object lock = new Object();
    protected List<TransactionTrait> transactionOpList;
    protected long checkpointDuration;

    public AbstractStreamProcessor(OP operator) {
        super(operator);
        this.transactionOpList = new ArrayList<>();
        addIfTransactionTrait(operator);
    }

    @Override
    public void open(List<ICollector> collectors, RuntimeContext runtimeContext) {
        super.open(collectors, runtimeContext);
        this.checkpointDuration = this.runtimeContext.getConfiguration().getLong(BATCH_NUMBER_PER_CHECKPOINT);
    }

    @Override
    public void finish(long windowId) {
        synchronized (lock) {
            LOGGER.info("{} do finish {}", runtimeContext.getTaskArgs().getTaskId(), windowId);
            for (TransactionTrait transactionTrait : this.transactionOpList) {
                transactionTrait.finish(windowId);
                if (CheckpointUtil.needDoCheckpoint(windowId, this.checkpointDuration)
                    && transactionTrait instanceof CheckpointTrait) {
                    ((CheckpointTrait) transactionTrait).checkpoint(windowId);
                }
            }
            super.finish(windowId);
        }
    }

    @Override
    public void rollback(long windowId) {
        synchronized (lock) {
            LOGGER.info("do rollback {}", windowId);
            for (TransactionTrait transactionTrait : this.transactionOpList) {
                transactionTrait.rollback(windowId);
            }
        }
    }

    @Override
    public R process(T value) {
        synchronized (lock) {
            return processElement((BatchRecord) value);
        }
    }

    protected void addIfTransactionTrait(Operator operator) {
        if (operator == null) {
            return;
        }
        if (operator instanceof TransactionTrait) {
            this.transactionOpList.add((TransactionTrait) operator);
        }
        for (Object subOperator : ((AbstractOperator) operator).getNextOperators()) {
            addIfTransactionTrait((Operator) subOperator);
        }
    }

    protected abstract R processElement(BatchRecord batchRecord);
}
