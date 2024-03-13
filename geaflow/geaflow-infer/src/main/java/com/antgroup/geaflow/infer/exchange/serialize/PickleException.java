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

package com.antgroup.geaflow.infer.exchange.serialize;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

public class PickleException extends GeaflowRuntimeException {

    private static final long serialVersionUID = -5870448664938735316L;

    public PickleException(String message, Throwable cause) {
        super(message, cause);
    }

    public PickleException(String message) {
        super(message);
    }
}
