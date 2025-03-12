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

package com.antgroup.geaflow.store.paimon.proxy;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.store.paimon.PaimonTableRWHandle;

public class PaimonProxyBuilder {

    public static <K, VV, EV> IGraphPaimonProxy<K, VV, EV> build(
        Configuration config, PaimonTableRWHandle vertexRWHandle,
        PaimonTableRWHandle edgeRWHandle, int[] projection,
        IGraphKVEncoder<K, VV, EV> encoder) {
        // TODO: add readonly proxy.
        return new PaimonGraphRWProxy(vertexRWHandle, edgeRWHandle, projection, encoder);
    }

}
