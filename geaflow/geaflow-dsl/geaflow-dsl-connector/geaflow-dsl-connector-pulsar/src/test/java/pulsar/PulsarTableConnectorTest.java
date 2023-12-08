package pulsar;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.*;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.pulsar.PulsarConfigKeys;
import com.antgroup.geaflow.dsl.connector.pulsar.PulsarTableConnector;
import com.antgroup.geaflow.dsl.connector.pulsar.PulsarTableSource.PulsarOffset;
import com.antgroup.geaflow.dsl.connector.pulsar.PulsarTableSource.PulsarPartition;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class PulsarTableConnectorTest {
    public static final String server = "pulsar://localhost:6650";
    public static final String topic = "persistent://test/test_pulsar_connector/non_partition_topic";
    public static final String partitionTopic = "persistent://test/test_pulsar_connector/partition_topic";


    @Test
    public void testPulsarPartition() {
        PulsarPartition partition = new PulsarPartition("topic");
        Assert.assertEquals(partition.getName(), "topic");

        PulsarPartition partition2 = new PulsarPartition("topic");
        Assert.assertEquals(partition.hashCode(), partition2.hashCode());
        Assert.assertEquals(partition, partition);
        Assert.assertEquals(partition, partition2);
        Assert.assertNotEquals(partition, null);
    }

    @Test
    public void testPulsarOffset() {
        PulsarOffset offsetByMessageId = new PulsarOffset(MessageId.earliest);
        Assert.assertEquals(offsetByMessageId.getMessageId(),
                DefaultImplementation.newMessageId(-1L, -1L, -1));
        Assert.assertEquals(offsetByMessageId.getOffset(), 0L);

        PulsarOffset offsetByTimeStamp = new PulsarOffset(11111111L);
        Assert.assertEquals(offsetByTimeStamp.getOffset(), 11111111L);
        Assert.assertNull(offsetByTimeStamp.getMessageId());

    }

    @Test
    public void testTableSourceConnect(){

        TableSchema tableSchema = new TableSchema();
        PulsarTableConnector pulsarTableConnector = new PulsarTableConnector();
        Assert.assertEquals(pulsarTableConnector.getType(), "PULSAR");
        Configuration tableConf = new Configuration();
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS.getKey(), server);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC.getKey(), topic);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION.getKey(), "earliest");
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME.getKey(), "testPulsarConnect");

        TableSource tableSource = pulsarTableConnector.createSource(tableConf);
        Assert.assertEquals(tableSource.getDeserializer(tableConf).getClass(), TextDeserializer.class);

        tableSource.init(tableConf, tableSchema);
        tableSource.open(new DefaultRuntimeContext(tableConf));

        Assert.assertEquals(tableSource.getDeserializer(tableConf).getClass(), TextDeserializer.class);

        tableSource.close();

    }

    @Test
    public void testListPartition() throws PulsarClientException, ExecutionException, InterruptedException {
        // test for non-partition topic
        TableSchema tableSchema = new TableSchema();
        PulsarTableConnector pulsarTableConnector = new PulsarTableConnector();
        Assert.assertEquals(pulsarTableConnector.getType(), "PULSAR");
        Configuration tableConf = new Configuration();
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS.getKey(), server);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC.getKey(), topic);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION.getKey(), "earliest");
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME.getKey(), "testPulsarConnect");

        TableSource tableSource = pulsarTableConnector.createSource(tableConf);
        Assert.assertEquals(tableSource.getDeserializer(tableConf).getClass(), TextDeserializer.class);

        tableSource.init(tableConf, tableSchema);
        tableSource.open(new DefaultRuntimeContext(tableConf));

        // expected partition result
        List<Partition> topicPartitionList = new ArrayList<>();
        topicPartitionList.add(new PulsarPartition(topic));

        // Obtain partitions through tableSource
        List<Partition> partitionListGetByTableSource = tableSource.listPartitions();
        Assert.assertEquals(partitionListGetByTableSource.size(), 1L);
        for (int i = 0; i < partitionListGetByTableSource.size(); i++) {
            Assert.assertEquals(partitionListGetByTableSource.get(i), topicPartitionList.get(i));
        }

        tableSource.close();

        // test for partition topic
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC.getKey(), partitionTopic);
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(server).build();
        List<String> partitionTopicGetByPulsarClient =  pulsarClient.getPartitionsForTopic(partitionTopic).get();

        topicPartitionList.clear();
        for (String s : partitionTopicGetByPulsarClient) {
            topicPartitionList.add(new PulsarPartition(s));
        }
        TableSource tableSourceWithPartitionTopic = pulsarTableConnector.createSource(tableConf);

        tableSourceWithPartitionTopic.init(tableConf, tableSchema);
        tableSourceWithPartitionTopic.open(new DefaultRuntimeContext(tableConf));

        partitionListGetByTableSource = tableSourceWithPartitionTopic.listPartitions();
        Assert.assertEquals(topicPartitionList.size(), partitionListGetByTableSource.size());

        for (int i = 0; i < partitionListGetByTableSource.size(); i++) {
            Assert.assertEquals(partitionListGetByTableSource.get(i), topicPartitionList.get(i));
        }
        tableSourceWithPartitionTopic.close();
        pulsarClient.close();


    }

    @Test
    public void testFetchDataByNonPartition() throws IOException {
        TableSchema tableSchema = new TableSchema();
        PulsarTableConnector pulsarTableConnector = new PulsarTableConnector();
        Assert.assertEquals(pulsarTableConnector.getType(), "PULSAR");
        Configuration tableConf = new Configuration();
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS.getKey(), server);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC.getKey(), topic);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION.getKey(), "earliest");
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME.getKey(), "testPulsarConnect");

        TableSource tableSource = pulsarTableConnector.createSource(tableConf);
        Assert.assertEquals(tableSource.getDeserializer(tableConf).getClass(), TextDeserializer.class);

        tableSource.init(tableConf, tableSchema);
        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitionListGetByTableSource = tableSource.listPartitions();
        Partition partition = partitionListGetByTableSource.get(0);

        // Create producer production data
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(server).build();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("testProducer")
                .create();

        // Record the Id of the start message
        List<String> originData = new ArrayList<>();
        originData.add("test String " + System.currentTimeMillis());
        MessageId messageIdStart = producer.send(originData.get(0));
        originData.add("test String " + System.currentTimeMillis());
        producer.send(originData.get(1));
        originData.add("test String " + System.currentTimeMillis());
        producer.send(originData.get(2));
        producer.close();
        pulsarClient.close();

        try {
            // fetch data from the first data
            FetchData<Object> data = tableSource.fetch(partition,
                    Optional.of(new PulsarOffset(messageIdStart)), 1);
            Iterator<Object> dataIterator = data.getDataIterator();
            Assert.assertEquals(originData.get(1), dataIterator.next().toString());
            // fetch the next data
            data = tableSource.fetch(partition, Optional.of(data.getNextOffset()), 1);
            dataIterator = data.getDataIterator();
            Assert.assertEquals(originData.get(2), dataIterator.next().toString());

            // track back to the first data, fetch data with size 2
            data = tableSource.fetch(partition, Optional.of(new PulsarOffset(messageIdStart)), 2);
            dataIterator = data.getDataIterator();
            Assert.assertEquals(originData.get(1), dataIterator.next().toString());
            Assert.assertEquals(originData.get(2), dataIterator.next().toString());

        } catch (IOException o){
            throw new IOException("IO error");
        }

        try {
            tableSource.fetch(partition, Optional.empty(), -1);
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), GeaFlowDSLException.class);
        }

        tableSource.close();

    }

    @Test
    public void testFetchDataByPartition() throws IOException, ExecutionException, InterruptedException {
        TableSchema tableSchema = new TableSchema();
        PulsarTableConnector pulsarTableConnector = new PulsarTableConnector();
        Assert.assertEquals(pulsarTableConnector.getType(), "PULSAR");
        Configuration tableConf = new Configuration();
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS.getKey(), server);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC.getKey(), partitionTopic);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION.getKey(), "earliest");
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME.getKey(), "testPulsarConnect");

        TableSource tableSource = pulsarTableConnector.createSource(tableConf);
        Assert.assertEquals(tableSource.getDeserializer(tableConf).getClass(), TextDeserializer.class);

        tableSource.init(tableConf, tableSchema);
        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitionListGetByTableSource = tableSource.listPartitions();

        // Using the API in Pulsar's consumer to read the first data of each partition
        // and compare it with fetching the first data of each partition
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(server).build();
        Consumer<String> consumer = null;

        List<String> partitionTopicGetByPulsarClient =  pulsarClient.getPartitionsForTopic(partitionTopic).get();

        List<String> PartitionListData = new ArrayList<>();

        for (String s: partitionTopicGetByPulsarClient) {
            if (consumer != null) {
                consumer.close();
            }
            consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(s)
                    .subscriptionName("sub_test")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
            consumer.seek(MessageId.earliest);
            PartitionListData.add(consumer.receive().getValue());
        }
       if (consumer != null) {
           consumer.close();
       }
       pulsarClient.close();

        List<String> result = new ArrayList<>();
        Offset offset = new PulsarOffset(MessageId.earliest);
        // Pull one piece of data from each partition
        for (int i = 0; i < partitionListGetByTableSource.size(); i++) {
            Partition partition = partitionListGetByTableSource.get(i);
            FetchData<Object> data = tableSource.fetch(partition, Optional.of(offset), 1);
            Iterator<Object> dataIterator = data.getDataIterator();
            result.add(dataIterator.next().toString());
            Assert.assertEquals(PartitionListData.get(i), result.get(i));

        }
        tableSource.close();

    }

    @Test
    public void testTableSinkConnect() throws IOException {

        // Write a message，1\tjim
        PulsarTableConnector pulsarTableConnector = new PulsarTableConnector();
        Assert.assertEquals(pulsarTableConnector.getType(), "PULSAR");
        Configuration tableConf = new Configuration();
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVERS.getKey(), server);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC.getKey(), topic);
        tableConf.put(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION.getKey(), "earliest");
        tableConf.put(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR.getKey(), "\t");

        TableSink tableSink = pulsarTableConnector.createSink(tableConf);
        TableSchema sinkSchema = new TableSchema(new TableField("id", IntegerType.INSTANCE, true),
                new TableField("name", StringType.INSTANCE, true));
        tableSink.init(tableConf, sinkSchema);
        tableSink.open(new DefaultRuntimeContext(tableConf));

        tableSink.write(ObjectRow.create(1, "jim"));
        tableSink.finish();
        tableSink.close();

        // Create consumers based on the topic and obtain their final Message Id
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(server).build();
        Consumer<String> consumer =  pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub_test")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        MessageId messageId = consumer.getLastMessageId();
        consumer.seek(messageId);
        Assert.assertEquals("1\tjim", consumer.receive().getValue());
        consumer.close();
        pulsarClient.close();

    }

}
