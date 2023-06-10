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
import java.io.Serializable;

/**
 * The Filter interface is used for condition pushdown.
 */
public interface IFilter<T> extends Serializable {

    /**
     * Filter the specific value, true means keep.
     */
    boolean filter(T value);

    /**
     * Returns the filter value type {@link DataType}.
     */
    DataType dateType();

    /**
     * Returns the filter's type {@link FilterType}.
     */
    default FilterType getFilterType() {
        return FilterType.OTHER;
    }

    /**
     * Returns the logical and filter of two filter.
     */
    default AndFilter and(IFilter filter) {
        return new AndFilter(this, filter);
    }

    /**
     * Returns the logical or filter of two filter.
     */
    default OrFilter or(IFilter filter) {
        return new OrFilter(this, filter);
    }
}
