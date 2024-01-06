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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.parser.GeaFlowDSLParser;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.testng.Assert;

public class PlanTester {

    public static final String defaultGraphDDL =
        "create graph g0("
        + "vertex user("
        + " id bigint ID,"
        + "name varchar,"
        + "age integer"
        + "),"
        + "edge knows("
        + " src_id bigint SOURCE ID,"
        + " target_id bigint DESTINATION ID,"
        + " weight double"
        + ")"
        + ")";

    public static final String defaultTableDDL =
        "create table t0 (id bigint, name varchar, age int)";

    private String gql;

    private SqlNode validateNode;

    private Exception validateException;

    private RelNode relNode;

    private final GeaFlowDSLParser parser = new GeaFlowDSLParser();

    private final GQLContext gqlContext = GQLContext.create(new Configuration(), false);

    private PlanTester() {
        try {
            GeaFlowDSLParser parser = new GeaFlowDSLParser();
            SqlCreateGraph createGraph = (SqlCreateGraph) parser.parseStatement(defaultGraphDDL);
            GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
            gqlContext.registerGraph(graph);
            gqlContext.setCurrentGraph(graph.getName());

            SqlCreateTable createTable = (SqlCreateTable) parser.parseStatement(defaultTableDDL);
            GeaFlowTable table = gqlContext.convertToTable(createTable);
            gqlContext.registerTable(table);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    public PlanTester registerGraph(String ddl) {
        try {
            GeaFlowDSLParser parser = new GeaFlowDSLParser();
            SqlCreateGraph createGraph = (SqlCreateGraph) parser.parseStatement(ddl);
            GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
            gqlContext.registerGraph(graph);
            gqlContext.setCurrentGraph(graph.getName());
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            this.validateException = e;
        }
        return this;
    }

    public PlanTester setCurrentGraph(String graphName) {
        gqlContext.setCurrentGraph(graphName);
        return this;
    }

    public static PlanTester build() {
        return new PlanTester();
    }

    public PlanTester gql(String gql) {
        if (this.gql != null) {
            throw new IllegalArgumentException("Duplicate setting for gql.");
        }
        this.gql = gql;
        return this;
    }

    public PlanTester validate() {
        if (validateException != null || validateNode != null) {
            return this;
        }
        try {
            SqlNode sqlNode = parser.parseStatement(gql);
            validateNode = gqlContext.validate(sqlNode);
        } catch (Exception e) {
            this.validateException = e;
        }
        return this;
    }

    public PlanTester toRel() {
        if (relNode != null) {
            return this;
        }
        validate();
        if (validateException == null) {
            this.relNode = gqlContext.toRelNode(validateNode);
        }
        return this;
    }

    public void expectValidateType(String expectType) {
        if (validateException != null) {
            throw new GeaFlowDSLException(validateException);
        }
        RelDataType dataType = gqlContext.getValidator().getValidatedNodeType(validateNode);
        Assert.assertEquals(dataType.toString(), expectType);
    }

    public void expectException(String expectErrorMsg) {
        Assert.assertNotNull(validateException);
        Assert.assertEquals(validateException.getMessage(), expectErrorMsg);
    }

    public void checkRelNode(String expectPlan) {
        if (validateException != null) {
            throw new GeaFlowDSLException(validateException);
        }
        String actualPlan = RelOptUtil.toString(relNode);
        Assert.assertEquals(actualPlan, expectPlan);
    }

    public String getDefaultGraphDDL() {
        return defaultGraphDDL;
    }
}
