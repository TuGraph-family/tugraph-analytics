package com.antgroup.geaflow.dsl.connector.api;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.type.primitive.BinaryStringType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.JsonDeserializer;
import org.junit.Test;
import org.testng.Assert;

import java.util.Collections;
import java.util.List;

public class JsonDeserializerTest {

    @Test
    public void testDeserialize() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
                new TableField("id", IntegerType.INSTANCE, false),
                new TableField("name", BinaryStringType.INSTANCE, true),
                new TableField("age", IntegerType.INSTANCE, false)
        );
        deserializer.init(new Configuration(), dataSchema);
        List<Row> row = deserializer.deserialize("{\"id\":1, \"name\":\"amy\", \"age\":10}");
        List<Row>  rowWithNull = deserializer.deserialize("{\"id\":1, \"name\":\"amy\"}");
        Assert.assertEquals(row.get(0).getField(0, IntegerType.INSTANCE), 1);
        Assert.assertEquals(row.get(0).getField(1, BinaryStringType.INSTANCE).toString(), "amy");
        Assert.assertEquals(row.get(0).getField(2, IntegerType.INSTANCE), 10);
        Assert.assertEquals(rowWithNull.get(0).getField(0, IntegerType.INSTANCE), 1);
        Assert.assertEquals(rowWithNull.get(0).getField(1, BinaryStringType.INSTANCE).toString(), "amy");
        Assert.assertEquals(rowWithNull.get(0).getField(2, IntegerType.INSTANCE), null);

    }


    @Test
    public void testDeserializeEmptyString() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
                new TableField("id", IntegerType.INSTANCE, false),
                new TableField("name", BinaryStringType.INSTANCE, true),
                new TableField("age", IntegerType.INSTANCE, false)
        );
        deserializer.init(new Configuration(), dataSchema);
        List<Row> rows = deserializer.deserialize("");
        List<Row> testNullRows = deserializer.deserialize(null);
        Assert.assertEquals(rows, Collections.emptyList());
        Assert.assertEquals(testNullRows, Collections.emptyList());

    }

    @Test(expected = GeaflowRuntimeException.class)
    public void testDeserializeParseError() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
                new TableField("id", IntegerType.INSTANCE, false),
                new TableField("name", BinaryStringType.INSTANCE, true),
                new TableField("age", IntegerType.INSTANCE, false)
        );
        deserializer.init(new Configuration(), dataSchema);
        List<Row> rows = deserializer.deserialize("test");
    }

    @Test
    public void testDeserializeIgnoreParseError() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
                new TableField("id", IntegerType.INSTANCE, false),
                new TableField("name", BinaryStringType.INSTANCE, true),
                new TableField("age", IntegerType.INSTANCE, false)
        );
        Configuration conf = new Configuration();
        conf.put("geaflow.dsl.connector.format.json.ignore-parse-error", "true");
        deserializer.init(conf, dataSchema);
        List<Row> rows = deserializer.deserialize("test");
        Assert.assertEquals(rows, Collections.emptyList());
    }

    @Test(expected = GeaflowRuntimeException.class)
    public void testDeserializeFailOnMissingField() {
        JsonDeserializer deserializer = new JsonDeserializer();
        StructType dataSchema = new StructType(
                new TableField("id", IntegerType.INSTANCE, false),
                new TableField("name", BinaryStringType.INSTANCE, true),
                new TableField("age", IntegerType.INSTANCE, false)
        );
        Configuration conf = new Configuration();
        conf.put("geaflow.dsl.connector.format.json.fail-on-missing-field", "true");
        deserializer.init(conf, dataSchema);
        List<Row>  rowWithMissingField = deserializer.deserialize("{\"id\":1, \"name\":\"amy\"}");

    }




}
