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

package com.antgroup.geaflow.console.common.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class FileUtil {

    public static boolean exist(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        return new File(path).exists();
    }

    public static void mkdir(String path) throws IOException {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        FileUtils.forceMkdir(new File(path));
    }

    public static boolean delete(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        return FileUtils.deleteQuietly(new File(path));
    }

    public static String readFileContent(String path) throws IOException {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        return FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8.name());
    }

    public static InputStream readFileStream(String path) throws IOException {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        return FileUtils.openInputStream(new File(path));
    }

    public static void writeFile(String path, String content) throws IOException {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        FileUtils.write(new File(path), content, StandardCharsets.UTF_8);
    }

    public static void writeFile(String path, InputStream stream) throws IOException {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        File file = new File(path);

        try (InputStream in = stream;
            OutputStream out = FileUtils.openOutputStream(file)) {
            IOUtils.copy(in, out, 1024 * 1024 * 8);
        }
    }

}
