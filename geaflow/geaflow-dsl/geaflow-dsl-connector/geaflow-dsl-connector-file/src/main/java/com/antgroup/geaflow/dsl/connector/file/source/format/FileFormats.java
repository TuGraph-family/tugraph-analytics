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

package com.antgroup.geaflow.dsl.connector.file.source.format;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import java.util.ServiceLoader;

public class FileFormats {

    @SuppressWarnings("unchecked")
    public static <T> FileFormat<T> loadFileFormat(String formatName) {
        ServiceLoader<FileFormat> formats = ServiceLoader.load(FileFormat.class);
        FileFormat<T> currentFormat = null;
        for (FileFormat<T> format : formats) {
            if (format.getFormat().equalsIgnoreCase(formatName)) {
                currentFormat = format;
                break;
            }
        }
        if (currentFormat == null) {
            throw new GeaFlowDSLException("File format '{}' is not found", formatName);
        }
        return currentFormat;
    }
}
