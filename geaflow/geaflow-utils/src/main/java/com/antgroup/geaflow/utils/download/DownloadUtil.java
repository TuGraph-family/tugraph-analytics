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

package com.antgroup.geaflow.utils.download;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadUtil.class);

    public static void downloadJarFile(String jarUrl, String destination) throws Exception {
        InputStream is = null;
        try {
            LOGGER.info("download {} to local {}", jarUrl, destination);
            File targetFile = new File(destination);
            if (targetFile.exists() && targetFile.isFile()) {
                LOGGER.info("file is already downloaded");
                return;
            }
            is = getDownloadInputStream(jarUrl);
            FileUtils.copyInputStreamToFile(is, targetFile);
            LOGGER.info("download done");
        } catch (Exception e) {
            LOGGER.error("download {} failed: {}", jarUrl, e.getMessage(), e);
            throw e;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    private static InputStream getDownloadInputStream(String uploadUrl) throws Exception {
        InputStream is;
        HttpURLConnection connection = (HttpURLConnection) new URL(uploadUrl).openConnection();
        int status = connection.getResponseCode();
        if (status == 403) {
            String msg = String.format("Down jar failed, invalid url: %s", uploadUrl);
            LOGGER.error(msg);
            throw new GeaflowRuntimeException("DOWNLOAD_JAR_ERROR:" + msg);
        }
        is = connection.getInputStream();
        return is;
    }

}
