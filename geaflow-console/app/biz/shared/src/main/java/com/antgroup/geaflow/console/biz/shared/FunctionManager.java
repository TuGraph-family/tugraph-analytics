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

package com.antgroup.geaflow.console.biz.shared;

import com.antgroup.geaflow.console.biz.shared.view.FunctionView;
import com.antgroup.geaflow.console.common.dal.model.FunctionSearch;
import org.springframework.web.multipart.MultipartFile;

public interface FunctionManager extends DataManager<FunctionView, FunctionSearch> {

    String createFunction(String instanceName, FunctionView view, MultipartFile functionFile, String fileId);

    boolean updateFunction(String instanceName, String functionName, FunctionView view, MultipartFile functionFile);

    boolean deleteFunction(String instanceName, String functionName);
}
