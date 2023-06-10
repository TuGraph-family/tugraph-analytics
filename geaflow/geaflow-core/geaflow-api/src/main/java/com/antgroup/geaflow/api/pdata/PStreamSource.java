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

package com.antgroup.geaflow.api.pdata;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.pdata.base.PSource;
import com.antgroup.geaflow.api.pdata.stream.PStream;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PStreamSource<T> extends PStream<T>, PSource {

    Logger LOGGER = LoggerFactory.getLogger(PStreamSource.class);

    static <T> PStreamSource<T> from(IPipelineContext pipelineContext,
                                     SourceFunction<T> sourceFunction,
                                     IWindow<T> window) {
        LOGGER.info("load PStreamSource SPI Implementation");
        ServiceLoader<PStreamSource> serviceLoader = ServiceLoader.load(PStreamSource.class);
        Iterator<PStreamSource> iterator = serviceLoader.iterator();
        boolean hasImpl = iterator.hasNext();
        if (hasImpl) {
            PStreamSource streamSource = iterator.next();
            return streamSource.build(pipelineContext, sourceFunction, window);
        } else {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(PStreamSource.class.getSimpleName()));
        }
    }

    /**
     * Build source by window.
     * @param pipelineContext
     * @param sourceFunction
     * @param window
     * @return
     */
    PStreamSource<T> build(IPipelineContext pipelineContext, SourceFunction<T> sourceFunction,
                           IWindow<T> window);

    PWindowSource<T> window(IWindow<T> window);

    @Override
    PStreamSource<T> withConfig(Map map);

    @Override
    PStreamSource<T> withConfig(String key, String value);

    @Override
    PStreamSource<T> withName(String name);

    @Override
    PStreamSource<T> withParallelism(int parallelism);

    @Override
    PStreamSource<T> withEncoder(IEncoder<T> encoder);

}
