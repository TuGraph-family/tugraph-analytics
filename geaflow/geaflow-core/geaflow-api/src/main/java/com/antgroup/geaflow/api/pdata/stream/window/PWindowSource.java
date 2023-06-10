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

package com.antgroup.geaflow.api.pdata.stream.window;

import com.antgroup.geaflow.api.pdata.PStreamSource;
import com.antgroup.geaflow.common.encoder.IEncoder;
import java.util.Map;

public interface PWindowSource<T> extends PWindowStream<T>, PStreamSource<T> {

    @Override
    PWindowSource<T> withConfig(Map map);

    @Override
    PWindowSource<T> withConfig(String key, String value);

    @Override
    PWindowSource<T> withName(String name);

    @Override
    PWindowSource<T> withParallelism(int parallelism);

    @Override
    PWindowSource<T> withEncoder(IEncoder<T> encoder);

}
