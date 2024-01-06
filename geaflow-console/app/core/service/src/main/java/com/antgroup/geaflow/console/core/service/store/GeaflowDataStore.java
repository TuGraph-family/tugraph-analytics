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

package com.antgroup.geaflow.console.core.service.store;

import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import java.util.Date;

public interface GeaflowDataStore {

    Long queryStorageUsage(GeaflowTask task);

    Long queryFileCount(GeaflowTask task);

    Date queryModifyTime(GeaflowTask task);

    void cleanTaskData(GeaflowTask task);

    void cleanGraphData(GeaflowGraph graph);

}
