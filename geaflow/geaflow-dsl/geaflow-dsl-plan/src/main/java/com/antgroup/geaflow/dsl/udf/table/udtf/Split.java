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

package com.antgroup.geaflow.dsl.udf.table.udtf;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDTF;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang.StringUtils;

@Description(name = "split", description = "Split string and expand it.")
public class Split extends UDTF {

    private static final String DEFAULT_SEP = ",";

    public void eval(String str) {
        eval(str, DEFAULT_SEP);
    }

    public void eval(String str, String separator) {
        String[] lines = StringUtils.split(str, separator);
        for (String line : lines) {
            collect(new Object[]{line});
        }
    }

    @Override
    public List<Class<?>> getReturnType(List<Class<?>> paramTypes, List<String> udtfReturnFields) {
        List<Class<?>> returnTypes = Lists.newArrayList();
        for (int i = 0; i < udtfReturnFields.size(); i++) {
            returnTypes.add(String.class);
        }
        return returnTypes;
    }
}
