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

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "SyntaxTest")
public class MatchReturnSyntaxTest extends BaseDslTest {

    @Test
    public void testGQLBaseMatchPattern() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLBaseMatchPattern.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLComplexMatch() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLComplexMatch.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLBaseMatchReturnStatements() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLBaseMatchReturnStatements.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLSelectFromMatch() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLSelectFromMatch.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLFilterStatement() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLFilterStatement.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLLet() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLLetStatement.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLSubQuery() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLSubQuery.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLMatchOrder() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLMatchOrder.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }
}
