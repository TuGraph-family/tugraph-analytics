package com.antgroup.geaflow.dsl.connector.api.serde.impl;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.types.ObjectType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class JsonDeserializer implements TableDeserializer<String> {

    private StructType schema;

    private ObjectMapper mapper;



    @Override
    public void init(Configuration conf, StructType schema) {
        this.schema = Objects.requireNonNull(schema);
        this.mapper = new ObjectMapper();
    }

    @Override
    public List<Row> deserialize(String record) {
        if (record == null || record.isEmpty()) {
            return Collections.emptyList();
        }
        Object[] values = new Object[schema.size()];
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(record);
        } catch (JsonProcessingException e) {
            // handle exception according to configuration
        }
        // if json node is null
        for(int i = 0 ; i < schema.size() ; i++) {
            JsonNode value = jsonNode.get(schema.getFieldNames().get(i));
            IType<?> type = schema.getType(i);
            // cast the value to the type defined in the schema.
            values[i] = TypeCastUtil.cast(value.asText(), type);
        }
        return  Collections.singletonList(ObjectRow.create(values));
    }
}
