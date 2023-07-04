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

package com.antgroup.geaflow.dsl.common.data;

import com.antgroup.geaflow.common.type.IType;
import java.util.Collection;
import java.util.List;

public interface Path extends Row {

    Row getField(int i, IType<?> type);

    List<Row> getPathNodes();

    void addNode(Row node);

    void remove(int index);

    Path copy();

    int size();

    Path subPath(Collection<Integer> indices);

    Path subPath(int[] indices);

    long getId();

    void setId(long id);
}
