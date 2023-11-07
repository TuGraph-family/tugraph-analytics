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

package com.antgroup.geaflow.dsl.connector.file.sink;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class S3FileWriteHandler extends LocalFileWriteHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileWriteHandler.class);

    private static final String TMP_PATH = "/tmp/";

    protected String path;
    protected AWSCredentials credentials;
    protected String serviceEndpoint;

    protected AmazonS3 s3;

    public S3FileWriteHandler(String baseDir) {
        super(TMP_PATH + UUID.randomUUID());
        path = baseDir;
    }

    @Override
    public void init(Configuration tableConf, StructType schema, int taskIndex) {
        super.init(tableConf, schema, taskIndex);
        this.credentials = FileConnectorUtil.getS3Credentials(tableConf);
        this.serviceEndpoint = FileConnectorUtil.getS3ServiceEndpoint(tableConf);
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSCredentialsProvider() {
                    @Override
                    public AWSCredentials getCredentials() {
                        return credentials;
                    }

                    @Override
                    public void refresh() {
                    }
                })
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, null))
                .build();

    }

    @Override
    public void flush() throws IOException {
        super.flush();
        File file = new File(targetFile);
        s3.putObject(FileConnectorUtil.getBucket(path), FileConnectorUtil.getKey(path) + "/" + file.getName(), file);
    }

    @Override
    public void close() throws IOException {
        super.close();
        s3.shutdown();
    }
}
