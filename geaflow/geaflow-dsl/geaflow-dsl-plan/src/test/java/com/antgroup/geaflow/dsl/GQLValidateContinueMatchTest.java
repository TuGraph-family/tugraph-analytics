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

package com.antgroup.geaflow.dsl;

import org.testng.annotations.Test;

public class GQLValidateContinueMatchTest {

    @Test
    public void testContinueMatch_001() {
        String script = "Match(a)-[e1]->(b)"
            + " Match(b) <-[e2] - (c)"
            + "RETURN a, b, c";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) c)");
    }

    @Test
    public void testContinueMatch_002() {
        String script = "Match(a)-[e1]->(b)"
            + " Let a.weight = a.age / 100"
            + " Match(b) <-[e2] - (c)"
            + "RETURN a, b, c";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age, "
                    + "JavaType(class java.lang.Integer) weight) a, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) c)");
    }
}
