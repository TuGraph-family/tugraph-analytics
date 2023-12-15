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

package com.antgroup.geaflow.common.utils;

import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtil {

    public static String concatPath(String baseDir, String fileName) {
        if (baseDir == null || fileName == null) {
            throw new NullPointerException();
        }
        if (baseDir.endsWith("/")) {
            return baseDir + fileName;
        }
        return baseDir + "/" + fileName;
    }

    public static String constitutePath(String... args) {
        return File.separator + Joiner.on(File.separator).join(args);
    }

    public static String getContentFromFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            StringBuilder content = new StringBuilder();
            String line;
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file)))) {
                while ((line = reader.readLine()) != null) {
                    content.append(line).append(System.lineSeparator());
                }
            } catch (IOException e) {
                throw new RuntimeException("Error read file content.", e);
            }
            return content.toString();
        }
        return null;
    }
}
