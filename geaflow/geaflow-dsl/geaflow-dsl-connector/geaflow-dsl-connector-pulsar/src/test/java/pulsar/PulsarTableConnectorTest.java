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
    
}
