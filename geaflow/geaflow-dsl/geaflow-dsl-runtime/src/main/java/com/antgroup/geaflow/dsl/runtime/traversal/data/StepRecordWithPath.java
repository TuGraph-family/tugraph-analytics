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

import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath.PathFilterFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath.PathMapFunction;
import java.util.List;
import java.util.function.Function;

public interface StepRecordWithPath extends StepRecord {

    ITreePath getPathById(Object vertexId);

    Iterable<ITreePath> getPaths();

    Iterable<Object> getVertexIds();

    StepRecordWithPath filter(PathFilterFunction function, int[] refPathIndices);

    StepRecordWithPath mapPath(PathMapFunction<Path> function, int[] refPathIndices);

    StepRecordWithPath mapTreePath(Function<ITreePath, ITreePath> function);

    <O> List<O> map(PathMapFunction<O> function, int[] refPathIndices);

    StepRecordWithPath subPathSet(int[] pathIndices);

    boolean isPathEmpty();
}
