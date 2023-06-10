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

package com.antgroup.geaflow.state.pushdown.filter.inner;

import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.OrFilter;
import com.google.common.base.Preconditions;

public class FilterHelper {

    public static boolean isSingleLimit(IFilter[] filters) {
        int orNumber = 0;
        int singleOrNumber = 0;
        for (IFilter filter: filters) {
            if (filter.getFilterType() == FilterType.OR) {
                orNumber++;
                if (((OrFilter)filter).isSingleLimit()) {
                    singleOrNumber++;
                }
            }
        }
        Preconditions.checkArgument(singleOrNumber == 0 || orNumber == singleOrNumber,
            "some or filter is not single");
        return singleOrNumber > 0;
    }
}
