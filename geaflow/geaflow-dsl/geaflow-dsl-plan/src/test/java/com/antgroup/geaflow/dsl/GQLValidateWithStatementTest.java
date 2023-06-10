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

public class GQLValidateWithStatementTest {

    @Test
    public void testWithStatement_001() {
        String script = "With p as (Select 1 as id)\n"
            + "Match (a where a.id = p.id) -[e] -> (b)\n"
            + "Return a.id as a_id, b.id as b_id";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(BIGINT a_id, BIGINT b_id)");
    }
}
