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

package com.antgroup.geaflow.dsl.connector.file.source;

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_JAVA_RESOURCE;
import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_S3_RESOURCE;

public class FileReadHandlers {

    public static FileReadHandler from(String path) {
        if (path.startsWith(PREFIX_JAVA_RESOURCE)) {
            return new JarFileReadHandler();
        } if (path.startsWith(PREFIX_S3_RESOURCE)) {
            return new S3FileReadHandler();
        }
        return new DfsFileReadHandler();
    }
}
