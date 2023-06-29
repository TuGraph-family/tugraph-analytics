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

package com.antgroup.geaflow.dsl.connector.socket;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.ISkipOpenAndClose;
import com.antgroup.geaflow.dsl.connector.api.Offset;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.socket.server.NettySourceClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketTableSource implements TableSource, ISkipOpenAndClose {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketTableSource.class.getName());

    private Configuration tableConf;

    private LinkedBlockingQueue<String> dataQueue;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
    }

    @Override
    public void open(RuntimeContext context) {
        String host = tableConf.getString(SocketConfigKeys.GEAFLOW_DSL_SOCKET_HOST);
        int port = tableConf.getInteger(SocketConfigKeys.GEAFLOW_DSL_SOCKET_PORT);
        this.dataQueue = new LinkedBlockingQueue<>();
        while (true) {
            try {
                NettySourceClient client = new NettySourceClient(host, port, dataQueue);
                client.run();
                break;
            } catch (Exception e) {
                LOGGER.info("Attempt to connect source netty server.");
                SleepUtils.sleepSecond(5);
            }
        }
    }

    @Override
    public List<Partition> listPartitions() {
        return Collections.singletonList(new SocketPartition(new ArrayList<>()));
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new TextDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  long windowSize) throws IOException {
        try {
            List<String> fetchData = new ArrayList<>();
            if (windowSize == Windows.SIZE_OF_ALL_WINDOW) {
                throw new GeaFlowDSLException("Socket doesn't support ALL_WINDOW mode");
            } else {
                for (int i = 0; i < windowSize; i++) {
                    fetchData.add(dataQueue.take());
                }
            }
            return (FetchData<T>) FetchData.createStreamFetch(fetchData, new SocketOffset(), false);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {

    }

    public static class SocketPartition implements Partition {

        private List<String> data;

        public SocketPartition(List<String> data) {
            this.data = data;
        }

        public List<String> getData() {
            return data;
        }

        @Override
        public String getName() {
            return String.valueOf(data.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SocketPartition that = (SocketPartition) o;
            return Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }

    public static class SocketOffset implements Offset {

        public SocketOffset() {

        }

        @Override
        public String humanReadable() {
            return "None";
        }

        @Override
        public long getOffset() {
            return -1;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
