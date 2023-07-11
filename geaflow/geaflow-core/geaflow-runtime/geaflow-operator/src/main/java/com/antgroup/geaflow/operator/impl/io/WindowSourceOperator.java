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

package com.antgroup.geaflow.operator.impl.io;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction.SourceContext;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.operator.OpArgs.OpType;
import com.antgroup.geaflow.operator.base.io.SourceOperator;
import com.antgroup.geaflow.operator.base.window.AbstractStreamOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowSourceOperator<OUT> extends AbstractStreamOperator<SourceFunction<OUT>> implements
    SourceOperator<OUT, Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowSourceOperator.class);

    protected transient SourceContext<OUT> sourceCxt;
    protected IWindow<OUT> windowFunction;

    public WindowSourceOperator() {
        super();
        opArgs.setOpType(OpType.MULTI_WINDOW_SOURCE);
    }

    public WindowSourceOperator(SourceFunction<OUT> sourceFunction) {
        super(sourceFunction);
        opArgs.setOpType(OpType.MULTI_WINDOW_SOURCE);
    }

    public WindowSourceOperator(SourceFunction<OUT> sourceFunction, IWindow<OUT> windowFunction) {
        this(sourceFunction);
        this.windowFunction = windowFunction;
        if (windowFunction instanceof AllWindow) {
            opArgs.setOpType(OpType.SINGLE_WINDOW_SOURCE);
        } else {
            opArgs.setOpType(OpType.MULTI_WINDOW_SOURCE);
        }
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.sourceCxt = new StreamSourceContext();
        TaskArgs taskArgs = opContext.getRuntimeContext().getTaskArgs();
        this.function.init(taskArgs.getParallelism(), taskArgs.getTaskIndex());
    }

    @Override
    public Boolean emit(long windowId) throws Exception {
        try {
            this.windowFunction.initWindow(windowId);
            return this.function.fetch(this.windowFunction, sourceCxt);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void close() {
        super.close();
        this.function.close();
    }

    class StreamSourceContext implements SourceContext<OUT> {

        public StreamSourceContext() {
        }

        @Override
        public boolean collect(OUT element) throws Exception {
            collectValue(element);
            return true;
        }

    }
}
