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

package com.antgroup.geaflow.analytics.service.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ResultTest {

    private static final String ERROR_MSG = "server execute failed";

    @Test
    public void testQueryError() {
        QueryError queryError = new QueryError(ERROR_MSG);
        Assert.assertEquals(queryError.getCode(), 0);
        Assert.assertEquals(queryError.getName(), ERROR_MSG);
        queryError = new QueryError(ERROR_MSG, 1);
        Assert.assertEquals(queryError.getCode(), 1);
        Assert.assertEquals(queryError.getName(), ERROR_MSG);
    }

    @Test
    public void testQueryResult() {
        String queryId = "1";
        QueryResults queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        Assert.assertNull(queryResults.getData());
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertEquals(queryResults.getError().getName(), ERROR_MSG);
        queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertEquals(queryResults.getError().getName(), ERROR_MSG);
    }

    @Test
    public void testQueryStatus() {
        String queryId = "1";
        String result = "result";
        QueryResults queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        Assert.assertNull(queryResults.getData());
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertEquals(queryResults.getError().getName(), ERROR_MSG);
        Assert.assertFalse(queryResults.isQueryStatus());

        queryResults = new QueryResults(queryId, result);
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertTrue(queryResults.isQueryStatus());
    }

}
