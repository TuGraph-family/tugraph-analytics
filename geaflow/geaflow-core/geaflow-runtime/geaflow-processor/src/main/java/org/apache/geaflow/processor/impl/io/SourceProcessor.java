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

package org.apache.geaflow.processor.impl.io;

import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.operator.base.io.SourceOperator;
import org.apache.geaflow.operator.impl.io.WindowSourceOperator;
import org.apache.geaflow.processor.impl.AbstractWindowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceProcessor<T> extends AbstractWindowProcessor<Void, Boolean, SourceOperator<T,
    Boolean>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceProcessor.class);

    public SourceProcessor(WindowSourceOperator<T> operator) {
        super(operator);
    }


    @Override
    public Boolean process(Void v) {
        try {
            return this.operator.emit(this.windowId);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void finish(long batchId) {
        super.finish(batchId);
    }

}
