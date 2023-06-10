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

package com.antgroup.geaflow.state.pushdown.filter;

import com.antgroup.geaflow.state.data.DataType;

public class EmptyFilter implements IFilter {

    private static final EmptyFilter FILTER = new EmptyFilter();

    public static EmptyFilter of() {
        return FILTER;
    }

    @Override
    public boolean filter(Object value) {
        return true;
    }

    @Override
    public DataType dateType() {
        return DataType.OTHER;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.EMPTY;
    }

    @Override
    public String toString() {
        return getFilterType().name();
    }
}
