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

public class GQLValidateGraphAlgorithmTest {

    @Test
    public void testGraphAlgorithm() {
        String script1 = "CALL SSSP(1) YIELD (vid, distance)\n" + "RETURN vid, distance";

        PlanTester.build()
            .gql(script1)
            .validate()
            .expectValidateType(
                "RecordType(VARCHAR vid, BIGINT distance)");

        String script2 = "CALL SSSP() YIELD (vid, distance)\n" + "RETURN vid, distance";

        PlanTester.build()
            .gql(script2)
            .validate()
            .expectValidateType(
                "RecordType(VARCHAR vid, BIGINT distance)");
    }
}
