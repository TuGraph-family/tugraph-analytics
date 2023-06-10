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

package com.antgroup.geaflow.console.common.service.integration.engine;

import com.antgroup.geaflow.console.common.util.proxy.ProxyClass;
import java.util.List;

@ProxyClass("com.antgroup.geaflow.file.IPersistentIO")
public interface IPersistentIO {

    List<String> listFile(FsPath path);

    long getFileCount(FsPath path);

    boolean exists(FsPath path);

    boolean rename(FsPath from, FsPath to);

    void delete(FsPath path, boolean recursive);

}
