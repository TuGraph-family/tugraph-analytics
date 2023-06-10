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
import com.antgroup.geaflow.common.utils.ArrayUtil;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.DefaultPath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FieldAlignPath implements Path {

    private final Path basePath;

    private final int[] fieldMapping;

    public FieldAlignPath(Path basePath, int[] fieldMapping) {
        this.basePath = basePath;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public void addNode(Row node) {
        throw new IllegalArgumentException("Read only path, addNode() method is not supported");
    }

    @Override
    public void remove(int index) {
        throw new IllegalArgumentException("Read only path, remove() method is not supported");
    }

    @Override
    public Path copy() {
        return new FieldAlignPath(basePath.copy(), fieldMapping);
    }

    @Override
    public int size() {
        return fieldMapping.length;
    }

    @Override
    public Path subPath(Collection<Integer> indices) {
        return subPath(ArrayUtil.toIntArray(indices));
    }

    @Override
    public Path subPath(int[] indices) {
        Path subPath = new DefaultPath();
        for (int index : indices) {
            subPath.addNode(getField(index, null));
        }
        return subPath;
    }

    @Override
    public Row getField(int i, IType<?> type) {
        int mappingIndex = fieldMapping[i];
        if (mappingIndex < 0) {
            return null;
        }
        return basePath.getField(mappingIndex, type);
    }

    @Override
    public List<Row> getPathNodes() {
        List<Row> pathNodes = new ArrayList<>(fieldMapping.length);
        for (int i = 0; i < fieldMapping.length; i++) {
            pathNodes.add(getField(i, null));
        }
        return pathNodes;
    }
}
