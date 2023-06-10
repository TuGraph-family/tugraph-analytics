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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObject;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OssUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(OssUtil.class);

    private final OSSClient ossClient;
    private final String downLoadUrl;
    private final OssConf ossConf;

    public OssUtil(OssConf ossConf) {
        this.ossClient = new OSSClient(ossConf.getEndPoint(),
            new DefaultCredentialProvider(ossConf.getAccessKeyId(), ossConf.getSecretAccessKey())
            , null);
        this.ossConf = ossConf;
        this.downLoadUrl = getDownLoadUrl("");
    }

    public InputStream download(String path) throws Exception {
        if (path.startsWith("http")) {
            if (path.startsWith(downLoadUrl)) {
                path = path.substring(downLoadUrl.length());
            } else {
                LOGGER.error("download oss path is wrong,path is {}",path);
                throw new RuntimeException("ERROR_UPLOAD_PATH,path:" + path);
            }
        }
        OSSObject ossObject = this.ossClient.getObject(ossConf.getBucket(), path);
        return ossObject.getObjectContent();
    }

    private String getDownLoadUrl(String path) {
        StringBuilder sb = new StringBuilder("http://").append(this.ossConf.getEndPoint());
        sb.insert(sb.indexOf("//") + 2, this.ossConf.getBucket() + ".");
        sb.append("/");
        sb.append(path);
        return sb.toString();
    }
}
