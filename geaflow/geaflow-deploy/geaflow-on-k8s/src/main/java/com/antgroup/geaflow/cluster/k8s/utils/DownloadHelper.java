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

package com.antgroup.geaflow.cluster.k8s.utils;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.utils.download.DownloadUtil;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadHelper.class);
    private static final String FILE_PATH_SEPARATOR = "/";
    private static final String FILE_LIST_SEPARATOR = ",";
    private static final String FILE_PROTOCOL = "file:";
    private static final String ZIP_SUFFIX = ".zip";
    private static final String JAR_SUFFIX = ".jar";

    public static List<URL> downloadJarPackage(String packageDir, String fileList) {
        String[] files = fileList.split(FILE_LIST_SEPARATOR);
        return downloadJarPackage(packageDir, Arrays.asList(files));
    }

    public static List<URL> downloadJarPackage(String packageDir, List<String> jarList) {
        List<URL> jarURLs = new ArrayList<>();
        for (String jarUrl : jarList) {
            try {
                String fileName = getFileNameFromUrl(jarUrl);
                String targetFileName = FileUtils.getFile(packageDir, fileName).toString();
                DownloadUtil.downloadJarFile(jarUrl, targetFileName);
                if (fileName.contains(ZIP_SUFFIX)) {
                    unzipFileToLocal(targetFileName, packageDir);
                } else if (fileName.endsWith(JAR_SUFFIX)) {
                    jarURLs.add(new URL(FILE_PROTOCOL + targetFileName));
                }
            } catch (Exception e) {
                LOGGER.error("download {} failed:{}", jarUrl, e.getMessage(), e);
                throw new GeaflowRuntimeException(e);
            }
        }
        return jarURLs;
    }

    private static String getFileNameFromUrl(String jarUrl) {
        int index = jarUrl.lastIndexOf(FILE_PATH_SEPARATOR);
        if (index < 0) {
            throw new GeaflowRuntimeException("illegal jar url:" + jarUrl);
        }
        return jarUrl.substring(index + 1);
    }

    public static void unzipFileToLocal(String fileName, String outputDir) throws IOException {
        LOGGER.info("unzip {} to {}", fileName, outputDir);
        try (java.util.zip.ZipFile zipFile = new ZipFile(fileName)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                File entryDestination = new File(outputDir,  entry.getName());
                if (entry.isDirectory()) {
                    entryDestination.mkdirs();
                } else {
                    entryDestination.getParentFile().mkdirs();
                    try (InputStream in = zipFile.getInputStream(entry);
                        OutputStream out = new FileOutputStream(entryDestination)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("unzip file {} failed:{}", fileName, e.getMessage(), e);
            throw e;
        }
    }

}
