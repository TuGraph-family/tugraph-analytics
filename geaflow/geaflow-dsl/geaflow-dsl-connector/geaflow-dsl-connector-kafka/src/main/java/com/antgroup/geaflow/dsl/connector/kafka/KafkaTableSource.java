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

package com.antgroup.geaflow.dsl.connector.kafka;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.utils.DateTimeUtil;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.Offset;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.api.util.ConnectorConstants;
import com.antgroup.geaflow.dsl.connector.kafka.utils.KafkaConstants;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTableSource.class);

    private static final Duration OPERATION_TIMEOUT =
        Duration.ofSeconds(KafkaConstants.KAFKA_OPERATION_TIMEOUT_SECONDS);
    private static final Duration POLL_TIMEOUT =
        Duration.ofSeconds(KafkaConstants.KAFKA_DATA_TIMEOUT_SECONDS);

    private String topic;
    private long windowSize;
    private int startTime;
    private Properties props;

    private transient KafkaConsumer<String, String> consumer;

    @Override
    public void init(Configuration conf, TableSchema tableSchema) {

        final String servers =
            conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_SERVERS);
        topic = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_TOPIC);
        final String groupId = conf.getString(KafkaConfigKeys.GEAFLOW_DSL_KAFKA_GROUP_ID);
        final String valueDeserializerClassString = KafkaConstants.KAFKA_VALUE_DESERIALIZER_CLASS;
        this.windowSize = conf.getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE);
        if (this.windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            throw new GeaFlowDSLException("Kafka cannot support all window");
        } else if (windowSize <= 0) {
            throw new GeaFlowDSLException("Invalid window size: {}", windowSize);
        }
        final String startTimeStr = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_START_TIME,
            (String) ConnectorConfigKeys.GEAFLOW_DSL_START_TIME.getDefaultValue());
        if (startTimeStr.equalsIgnoreCase(KafkaConstants.KAFKA_BEGIN)) {
            startTime = 0;
        } else {
            startTime = DateTimeUtil.toUnixTime(startTimeStr, ConnectorConstants.START_TIME_FORMAT);
        }

        this.props = new Properties();
        props.setProperty(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, servers);
        props.setProperty(KafkaConstants.KAFKA_KEY_DESERIALIZER,
            valueDeserializerClassString);
        props.setProperty(KafkaConstants.KAFKA_VALUE_DESERIALIZER,
            valueDeserializerClassString);
        props.setProperty(KafkaConstants.KAFKA_MAX_POLL_RECORDS,
            String.valueOf(windowSize));
        props.setProperty(KafkaConstants.KAFKA_GROUP_ID, groupId);
        LOGGER.info("open kafka, servers is: {}, topic is:{}, config is:{}, schema is: {}",
            servers, topic, conf, tableSchema);
    }

    @Override
    public void open(RuntimeContext context) {
        consumer = new KafkaConsumer<>(props);
        LOGGER.info("consumer opened, topic: {}", topic);
    }

    @Override
    public List<Partition> listPartitions() {
        KafkaConsumer tmpConsumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitions = tmpConsumer.partitionsFor(topic, OPERATION_TIMEOUT);
        tmpConsumer.close();
        return partitions.stream().map(
            partition -> new KafkaPartition(topic, partition.partition())
        ).collect(Collectors.toList());
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new TextDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  long newWindowSize) throws IOException {
        KafkaPartition kafkaPartition = (KafkaPartition) partition;
        if (newWindowSize == Windows.SIZE_OF_ALL_WINDOW) {
            throw new GeaFlowDSLException("Kafka cannot support all window");
        } else if (newWindowSize <= 0) {
            throw new GeaFlowDSLException("Invalid window size: {}", newWindowSize);
        }
        this.windowSize = newWindowSize;

        TopicPartition topicPartition = new TopicPartition(kafkaPartition.getTopic(),
            kafkaPartition.getPartition());
        Set<TopicPartition> singletonPartition = Collections.singleton(topicPartition);

        Set<TopicPartition> currentAssignment = consumer.assignment();
        if (currentAssignment.size() != 1 || !currentAssignment.contains(topicPartition)) {
            consumer.assign(singletonPartition);
        }

        KafkaOffset reqKafkaOffset;
        if (startOffset.isPresent()) {
            reqKafkaOffset = (KafkaOffset) startOffset.get();
            consumer.seek(topicPartition, reqKafkaOffset.getKafkaOffset());
        } else {
            if (startTime == 0) {
                Map<TopicPartition, Long> partition2Offset =
                    consumer.beginningOffsets(singletonPartition, OPERATION_TIMEOUT);
                Long beginningOffset = partition2Offset.get(topicPartition);
                if (beginningOffset == null) {
                    throw new GeaFlowDSLException("Cannot get beginning offset for partition: {}, "
                        + "startTime: {}.", topicPartition, startTime);
                } else {
                    consumer.seek(topicPartition, beginningOffset);
                }
            } else {
                Map<TopicPartition, OffsetAndTimestamp> partitionOffset =
                    consumer.offsetsForTimes(Collections.singletonMap(topicPartition,
                        (long)startTime), OPERATION_TIMEOUT);
                OffsetAndTimestamp offset = partitionOffset.get(topicPartition);
                if (offset == null) {
                    throw new GeaFlowDSLException("Cannot get offset for partition: {}, "
                        + "startTime: {}.", topicPartition, startTime);
                } else {
                    consumer.seek(topicPartition, offset.offset());
                }
            }
        }

        Iterator<ConsumerRecord<String, String>> recordIterator = consumer.poll(POLL_TIMEOUT).iterator();
        List<String> dataList = new ArrayList<>();
        long responseMaxTimestamp = -1;
        while (recordIterator.hasNext()) {
            ConsumerRecord<String, String> record = recordIterator.next();
            assert record.topic().equals(this.topic) : "Illegal topic";
            dataList.add(record.value());
            if (record.timestamp() > responseMaxTimestamp) {
                responseMaxTimestamp = record.timestamp();
            }
        }
        //reload cursor
        long nextOffset = consumer.position(topicPartition, OPERATION_TIMEOUT);
        KafkaOffset nextKafkaOffset;
        if (responseMaxTimestamp >= 0) {
            nextKafkaOffset = new KafkaOffset(nextOffset, responseMaxTimestamp);
        } else {
            nextKafkaOffset = new KafkaOffset(nextOffset, System.currentTimeMillis());
        }
        return (FetchData<T>) FetchData.createStreamFetch(dataList, nextKafkaOffset, false);
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        LOGGER.info("close");
    }

    public static class KafkaPartition implements Partition {

        private final String topic;
        private final int partitionId;

        public KafkaPartition(String topic, int partitionId) {
            this.topic = topic;
            this.partitionId = partitionId;
        }

        @Override
        public String getName() {
            return topic + "-" + partitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partitionId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KafkaPartition)) {
                return false;
            }
            KafkaPartition that = (KafkaPartition) o;
            return Objects.equals(topic, that.topic) && Objects.equals(
                partitionId, that.partitionId);
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partitionId;
        }
    }


    public static class KafkaOffset implements Offset {

        private final long offset;

        private final long humanReadableTime;

        public KafkaOffset(long offset, long humanReadableTime) {
            this.offset = offset;
            this.humanReadableTime = humanReadableTime;
        }

        @Override
        public String humanReadable() {
            return DateTimeUtil.fromUnixTime(humanReadableTime, ConnectorConstants.START_TIME_FORMAT);
        }

        @Override
        public long getOffset() {
            return humanReadableTime;
        }

        public long getKafkaOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return true;
        }
    }
}
