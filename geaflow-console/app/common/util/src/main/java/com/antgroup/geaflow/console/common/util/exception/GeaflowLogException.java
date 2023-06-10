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

package com.antgroup.geaflow.console.common.util.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeaflowLogException extends GeaflowException {

    public GeaflowLogException(String fmt, Object... args) {
        super(fmt, args);
        log.error(getMessage(), getCause());
    }
}
