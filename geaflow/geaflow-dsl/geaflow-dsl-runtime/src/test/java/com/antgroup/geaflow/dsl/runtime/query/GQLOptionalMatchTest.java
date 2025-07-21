/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.runtime.query;

import org.testng.annotations.Test;

public class GQLOptionalMatchTest {

    /**
     * 该测试用例旨在验证一个核心的 OPTIONAL MATCH 场景：
     * 查找所有用户，并可选地返回他们“认识”的朋友。
     * 这将覆盖找到匹配（有朋友）和未找到匹配（没有朋友，返回 null）两种情况。
     *
     * 预期行为：此测试在物理计划和运行时算子完全支持前会失败。
     * 失败时的报错信息将指引下一步需要开发的组件。
     */
    @Test
    public void testOptionalMatch_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_optional_match_001.sql")
            .execute()
            .checkSinkResult();
    }
}
