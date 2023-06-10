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

package com.antgroup.geaflow.common.encoder;

import com.antgroup.geaflow.common.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public interface IEncoder<T> extends Serializable {

    /**
     * Init with config.
     *
     * @param config config
     */
    void init(Configuration config);

    /**
     * Encode an object to output stream.
     *
     * @param data data
     * @param outputStream output stream
     * @throws IOException IO exception
     */
    void encode(T data, OutputStream outputStream) throws IOException;

    /**
     * Decode an object from input stream.
     *
     * @param inputStream input stream
     * @return data
     * @throws IOException IO exception
     */
    T decode(InputStream inputStream) throws IOException;

}
