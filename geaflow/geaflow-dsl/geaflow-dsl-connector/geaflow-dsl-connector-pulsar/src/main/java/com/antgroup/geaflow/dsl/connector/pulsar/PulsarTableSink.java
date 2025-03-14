/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.connector.pulsar;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.connector.pulsar.utils.PulsarConstants;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class PulsarTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSink.class);

    private Configuration tableConf;
    private StructType schema;
    private String servers;
    private String topic;
    private int maxPendingMessage;
    private int maxMessages;
    private int maxPublishDelay;

    private transient PulsarClient pulsarClient;
    private transient Producer<String> producer;

    private MessageRoutingMode messageRoutingMode;
    private String separator;

    private void createPulsarProducer() {

        if (messageRoutingMode == null) {
            messageRoutingMode = MessageRoutingMode.SinglePartition;
        }
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(servers).build();
            producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .maxPendingMessages(maxPendingMessage)
                    .messageRoutingMode(messageRoutingMode)
                    .batchingMaxMessages(maxMessages)
                    .batchingMaxPublishDelay(maxPublishDelay, TimeUnit.MILLISECONDS)
                    .create();
        } catch (PulsarClientException e) {
            throw new GeaFlowDSLException("create pulsar producer error, exception is {}", e);
        }
    }

    @Override
    public void init(Configuration conf, StructType schema) {
        tableConf = conf;
        String port = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_PORT);
        String[] serversAddress = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS).split(",");
        servers = "pulsar://" + String.join(":" + port + ",", serversAddress);

        topic = conf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC);
        this.schema = schema;
        separator = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
        maxPendingMessage = PulsarConstants.PULSAR_MAX_PENDING_MESSAGES;
        maxMessages = PulsarConstants.PULSAR_BATCHING_MAX_MESSAGES;
        maxPublishDelay = PulsarConstants.PULSAR_BATCHING_MAX_PUBLISH_DELAY;
    }

    @Override
    public void open(RuntimeContext context) {
        createPulsarProducer();
    }

    @Override
    public void write(Row row) {
        Object[] values  = row.getFields(schema.getTypes());
        StringBuilder line = new StringBuilder();
        for (Object value : values) {
            if (line.length() > 0) {
                line.append(separator);
            }
            line.append(value);
        }
        try {
            producer.send(line.toString());
        } catch (PulsarClientException e) {
            throw new GeaFlowDSLException("pulsar producer send message error, exception is {}", e);
        }

    }

    @Override
    public void finish() throws IOException {
        if (producer != null) {
            producer.flush();
        } else {
            assert producer != null;
            LOGGER.warn("Producer is null.");
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.close();
                pulsarClient.close();
            } catch (PulsarClientException e) {
                throw new GeaFlowDSLException("pulsar client close error, exception is {}", e);
            }
        }
        LOGGER.info("close pulsar client");
    }



}
