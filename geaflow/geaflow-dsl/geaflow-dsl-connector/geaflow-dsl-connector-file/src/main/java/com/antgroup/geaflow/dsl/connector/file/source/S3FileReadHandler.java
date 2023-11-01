package com.antgroup.geaflow.dsl.connector.file.source;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class S3FileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileReadHandler.class);

    protected String path;

    protected AWSCredentials credentials;
    protected String serviceEndpoint;

    protected AmazonS3 s3;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        super.init(tableConf, tableSchema, path);
        this.path = path;
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
    public List<Partition> listPartitions() {
        List<Partition> partitions = new ArrayList<>();
        try {
            ListObjectsV2Result result = s3.listObjectsV2(
                    FileConnectorUtil.getBucket(path),
                    FileConnectorUtil.getKey(path)
            );


            result.getObjectSummaries()
                    .forEach((S3ObjectSummary obj) -> {
                        ResourceFileSplit split = new ResourceFileSplit(obj.getBucketName(), obj.getKey());
                        split.setS3(s3);
                        partitions.add(split);
                    });
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Cannot get partitions with path: " + path, e);
        }
        return partitions;
    }

    public static class ResourceFileSplit extends FileTableSource.FileSplit {

        private AmazonS3 s3;

        private String object;

        private String key;

        void setS3(AmazonS3 s3) {
            this.s3 = s3;
        }

        public ResourceFileSplit(String baseDir, String relativePath) {
            super(baseDir, relativePath);
            this.object = baseDir;
            this.key = relativePath;
        }

        @Override
        public InputStream openStream(Configuration conf) throws IOException {
            S3Object obj = s3.getObject(object, key);
            return obj.getObjectContent();
        }
    }

}
