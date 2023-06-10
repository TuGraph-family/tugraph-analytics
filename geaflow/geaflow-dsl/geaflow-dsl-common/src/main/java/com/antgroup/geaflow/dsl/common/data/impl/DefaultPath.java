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

package com.antgroup.geaflow.dsl.common.data.impl;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DefaultPath implements Path {

    private final List<Row> pathNodes;

    public DefaultPath(List<Row> pathNodes) {
        this.pathNodes = Objects.requireNonNull(pathNodes);
    }

    public DefaultPath(Row[] pathNodes) {
        this(Lists.newArrayList(pathNodes));
    }

    public DefaultPath() {
        this(new ArrayList<>());
    }

    @Override
    public Row getField(int i, IType<?> type) {
        return pathNodes.get(i);
    }

    @Override
    public List<Row> getPathNodes() {
        return pathNodes;
    }

    @Override
    public void addNode(Row node) {
        pathNodes.add(node);
    }

    @Override
    public void remove(int index) {
        pathNodes.remove(index);
    }

    @Override
    public Path copy() {
        return new DefaultPath(Lists.newArrayList(pathNodes));
    }

    @Override
    public int size() {
        return pathNodes.size();
    }

    @Override
    public Path subPath(Collection<Integer> indices) {
        List<Integer> indexList = new ArrayList<>(indices);
        Collections.sort(indexList);

        Path subPath = new DefaultPath();
        for (Integer index : indexList) {
            subPath.addNode(pathNodes.get(index));
        }
        return subPath;
    }

    @Override
    public Path subPath(int[] indices) {
        Path subPath = new DefaultPath();
        for (Integer index : indices) {
            subPath.addNode(pathNodes.get(index));
        }
        return subPath;
    }

    @Override
    public String toString() {
        return "DefaultPath{"
            + "pathNodes=" + pathNodes
            + '}';
    }
}
