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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.common.type.IType;

public class GlobalVariable {

    private final String name;

    private final int index;

    private final IType<?> type;

    private int addFieldIndex = -1;

    public GlobalVariable(String name, int index, IType<?> type) {
        this.name = name;
        this.index = index;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public IType<?> getType() {
        return type;
    }

    public int getAddFieldIndex() {
        return addFieldIndex;
    }

    public void setAddFieldIndex(int addFieldIndex) {
        this.addFieldIndex = addFieldIndex;
    }
}
