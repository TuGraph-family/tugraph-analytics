/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl;

import org.testng.annotations.Test;

public class GQLValidateOptionalMatchTest {

        /**
         * 定义一个全面的图模式（Graph Schema），覆盖所有测试用例。
         */
        private static final String graphDDL = "CREATE GRAPH g1("
                        + "  Vertex user (id bigint ID, name varchar, age int),"
                        + "  Vertex person (id bigint ID, name varchar, age int),"
                        + "  Vertex movie (id bigint ID, title varchar),"
                        + "  Vertex book (id bigint ID, title varchar),"
                        + "  Vertex author (id bigint ID, name varchar),"
                        + "  Vertex show (id bigint ID, name varchar),"
                        + "  Edge knows (src_id bigint SOURCE ID, target_id bigint DESTINATION ID, weight double),"
                        + "  Edge rates (src_id bigint SOURCE ID, target_id bigint DESTINATION ID, score int),"
                        + "  Edge wrote (src_id bigint SOURCE ID, target_id bigint DESTINATION ID),"
                        + "  Edge adapted_from (src_id bigint SOURCE ID, target_id bigint DESTINATION ID),"
                        + "  Edge friends_with (src_id bigint SOURCE ID, target_id bigint DESTINATION ID)"
                        + ")";

        /**
         * 测试1 (源自大作业验收标准): 验证 "MATCH ... OPTIONAL MATCH ... RETURN" 的核心模式。
         * 修正：移除不被支持的 WITH 关键字。
         */
        @Test
        public void testFromAssignment() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (p:person WHERE id = 1) "
                                                + "OPTIONAL MATCH (p)-[r:knows]->(c:person) "
                                                + "RETURN p.name AS p_name, c.name AS c_name")
                                .validate()
                                .expectValidateType("RecordType(VARCHAR p_name, VARCHAR c_name)");
        }

        /**
         * 测试2: 基础的OPTIONAL MATCH
         * 修正：移除 WITH 并返回属性，这是目前最能兼容验证器的写法。
         */
        @Test
        public void testBasicOptionalMatch() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (a:user) "
                                                + "OPTIONAL MATCH (a)-[e:knows]->(b:user) "
                                                + "RETURN a.name as a_name, b.name as b_name")
                                .validate()
                                .expectValidateType("RecordType(VARCHAR a_name, VARCHAR b_name)");
        }

        /**
         * 测试3: 带有属性过滤的OPTIONAL MATCH
         * 修正：移除不被支持的 WITH 关键字。
         */
        @Test
        public void testOptionalMatchWithPropertyFilter() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (a:user WHERE a.name = 'marko') "
                                                + "OPTIONAL MATCH (a)-[e:knows WHERE e.weight > 0.8]->(b:user) "
                                                + "RETURN a.id AS a_id, e.weight AS weight, b.id AS b_id")
                                .validate()
                                .expectValidateType("RecordType(BIGINT a_id, DOUBLE weight, BIGINT b_id)");
        }

        /**
         * 测试4: 带有外部WHERE子句的OPTIONAL MATCH
         */
        @Test
        public void testOptionalMatchWithOuterWhere() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (a:user) "
                                                + "OPTIONAL MATCH (a)-[r:rates]->(m:movie) "
                                                + "WHERE r.score > 4 "
                                                + "RETURN a.name AS user_name, m.title AS movie_title, r.score as score")
                                .validate()
                                .expectValidateType(
                                                "RecordType(VARCHAR user_name, VARCHAR movie_title, INTEGER score)");
        }

        /**
         * 测试5: 链式的OPTIONAL MATCH
         * 修正：移除不被支持的 WITH 关键字。
         */
        @Test
        public void testChainedOptionalMatch() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (p:author WHERE p.name = 'George R. R. Martin') "
                                                + "OPTIONAL MATCH (p)-[:wrote]->(b:book) "
                                                + "OPTIONAL MATCH (b)<-[:adapted_from]-(s:show) "
                                                + "RETURN p.name AS author_name, b.title AS book_title, s.name AS show_name")
                                .validate()
                                .expectValidateType(
                                                "RecordType(VARCHAR author_name, VARCHAR book_title, VARCHAR show_name)");
        }

        /**
         * 测试6: 在复杂MATCH后使用OPTIONAL MATCH
         */
        @Test
        public void testOptionalMatchAfterComplexMatch() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (a:user)-[:knows]->(b:user), (a)-[:knows]->(c:user) "
                                                + "OPTIONAL MATCH (b)-[e:friends_with]->(c) "
                                                + "RETURN b.id AS b_id, c.id AS c_id, e.src_id IS NOT NULL AS is_friend")
                                .validate()
                                .expectValidateType("RecordType(BIGINT b_id, BIGINT c_id, BOOLEAN is_friend)");
        }

        /**
         * 测试7: OPTIONAL MATCH + 聚合函数
         * 验证 GeaFlow DSL 中 RETURN 子句的 GROUP BY 能否正常解析
         */
        @Test
        public void testOptionalMatchWithAggregation() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (a:user) "
                                                + "OPTIONAL MATCH (a)-[:knows]->(b:user) "
                                                + "RETURN a.id AS a_id, COUNT(b.id) AS b_count GROUP BY a.id")
                                .validate()
                                .expectValidateType("RecordType(BIGINT a_id, BIGINT b_count)");
        }

        /**
         * 测试8: OPTIONAL MATCH 与 UNION 联合使用
         * 验证类型推导在 UNION 场景下是否兼容
         */
        @Test
        public void testOptionalMatchWithUnion() {
                PlanTester.build().registerGraph(graphDDL)
                                .gql("MATCH (a:user WHERE a.id = 1) "
                                                + "OPTIONAL MATCH (a)-[:knows]->(b:user) "
                                                + "RETURN a.name AS name1, b.name AS name2 "
                                                + "UNION "
                                                + "MATCH (c:user WHERE c.id = 2) "
                                                + "RETURN c.name AS name1, CAST(NULL AS VARCHAR) AS name2")
                                .validate()
                                .expectValidateType("RecordType(VARCHAR name1, VARCHAR name2)");
        }

}