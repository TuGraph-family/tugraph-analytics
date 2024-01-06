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

package com.antgroup.geaflow.analytics.service.query;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.Serializable;
import java.util.Objects;

public class QueryError implements Serializable {

    private static final int DEFAULT_ERROR_CODE = 0;
    private static final String DELIMITER = ":";
    private final int code;
    private final String name;

    public QueryError(String name, int code) {
        if (code < 0) {
            throw new GeaflowRuntimeException("query error code is negative");
        }
        this.name = name;
        this.code = code;
    }

    public QueryError(String name) {
        this.name = name;
        this.code = DEFAULT_ERROR_CODE;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryError that = (QueryError) obj;
        return Objects.equals(this.code, that.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code);
    }

    @Override
    public String toString() {
        return name + DELIMITER + code;
    }
}
