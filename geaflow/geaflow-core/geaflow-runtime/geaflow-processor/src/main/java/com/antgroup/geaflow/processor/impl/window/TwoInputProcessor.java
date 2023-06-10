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

package com.antgroup.geaflow.processor.impl.window;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.record.BatchRecord;
import com.antgroup.geaflow.operator.base.window.TwoInputOperator;
import com.antgroup.geaflow.processor.impl.AbstractStreamProcessor;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputProcessor<T> extends AbstractStreamProcessor<T,
    Void, TwoInputOperator> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwoInputProcessor.class);

    private String leftStream;
    private String rightStream;

    public TwoInputProcessor(TwoInputOperator operator) {
        super(operator);
    }

    @Override
    public Void processElement(BatchRecord batchRecord) {
        try {
            final Iterator<T> messageIterator = batchRecord.getMessageIterator();
            final String streamName = batchRecord.getStreamName();

            if (leftStream.equals(streamName)) {
                while (messageIterator.hasNext()) {
                    T record = messageIterator.next();
                    operator.processElementOne(record);
                }
            } else if (rightStream.equals(streamName)) {
                while (messageIterator.hasNext()) {
                    T record = messageIterator.next();
                    operator.processElementTwo(record);
                }
            }

            return null;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public void setLeftStream(String leftStream) {
        this.leftStream = leftStream;
    }

    public void setRightStream(String rightStream) {
        this.rightStream = rightStream;
    }
}
