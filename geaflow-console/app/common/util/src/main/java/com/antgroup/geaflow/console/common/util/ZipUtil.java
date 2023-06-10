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

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;

public class ZipUtil {

    public static void zipToFile(String fileName, List<GeaflowZipEntry> entries) throws IOException {
        try (InputStream zipInputStream = buildZipInputStream(entries)) {
            FileUtils.copyInputStreamToFile(zipInputStream, new File(fileName));
        }
    }

    public static InputStream buildZipInputStream(List<GeaflowZipEntry> entries) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writeZipStream(bos, entries);
            byte[] zipBytes = bos.toByteArray();
            return new ByteArrayInputStream(zipBytes);
        }
    }

    private static void writeZipStream(OutputStream bos, List<GeaflowZipEntry> entries) throws IOException {
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(bos)) {
            for (GeaflowZipEntry entry : entries) {
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(entry.getContent().getBytes())) {
                    zipOutputStream.putNextEntry(new ZipEntry(entry.getFileName()));
                    byte[] buff = new byte[1024];
                    int len = 0;
                    while ((len = inputStream.read(buff)) > -1) {
                        zipOutputStream.write(buff, 0, len);
                    }
                    zipOutputStream.closeEntry();
                }
            }
            zipOutputStream.flush();
        }
    }

    public static void unzip(File file) {
        String dir = file.getParent();
        try (ZipInputStream zipInputStream = new ZipInputStream(Files.newInputStream(file.toPath()))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                String filePath = dir + "/" + entry.getName();
                File outFile = new File(filePath);
                if (entry.isDirectory()) {
                    if (!outFile.exists()) {
                        outFile.mkdirs();
                    }
                    continue;
                }
                try (FileOutputStream fileOutputStream = new FileOutputStream(filePath)) {
                    byte[] buf = new byte[1024 * 1024];
                    int num;
                    while ((num = zipInputStream.read(buf, 0, buf.length)) > -1) {
                        fileOutputStream.write(buf, 0, num);
                    }
                    fileOutputStream.flush();
                }
                zipInputStream.closeEntry();
            }
        } catch (IOException e) {
            throw new GeaflowException("Unzip file {} failed", file.getPath(), e);
        }
    }

    @Setter
    @Getter
    public static class GeaflowZipEntry {

        private String fileName;
        private String content;

        public GeaflowZipEntry(String fileName, String content) {
            this.fileName = fileName;
            this.content = content;
        }
    }
}
