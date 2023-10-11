package com.antgroup.geaflow.dsl.runtime.query;

import org.testng.annotations.Test;

public class TypesTest {

    @Test
    public void testBooleanType_001() throws Exception  {
        QueryTester
            .build()
            .withQueryPath("/query/type_boolean_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testTimestampType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_timestamp_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDateType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_date_001.sql")
            .execute()
            .checkSinkResult();
    }
}
