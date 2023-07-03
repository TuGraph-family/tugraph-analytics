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

import com.antgroup.geaflow.console.biz.shared.view.RemoteFileView;
import com.antgroup.geaflow.console.common.dal.model.RemoteFileSearch;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.multipart.MultipartFile;

public interface RemoteFileManager extends NameManager<RemoteFileView, RemoteFileSearch> {

    String create(RemoteFileView view, MultipartFile multipartFile);

    boolean upload(String remoteFileId, MultipartFile multipartFile);

    boolean download(String remoteFileId, HttpServletResponse response);

    boolean delete(String remoteFileId);

    void deleteFunctionJar(String jarId, String functionId);

    void deleteJobJar(String jarId, String jobId);

    void deleteVersionJar(String id);
}
