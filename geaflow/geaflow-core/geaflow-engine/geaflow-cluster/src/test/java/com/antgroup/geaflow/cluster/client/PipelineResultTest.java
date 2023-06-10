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

package com.antgroup.geaflow.cluster.client;

import com.antgroup.geaflow.cluster.rpc.impl.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.rpc.proto.Driver;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.testng.annotations.Test;

public class PipelineResultTest {

    @Test
    public void testResult() {
        List<Driver.PipelineRes> res = new ArrayList();
        res.add(Driver.PipelineRes.newBuilder().setPayload(RpcMessageEncoder.encode("test")).build());
        PipelineResult result = new PipelineResult(res.iterator());
        Assert.assertEquals("test", result.get());
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class,
        expectedExceptionsMessageRegExp = ".*not found pipeline result.*")
    public void testNotHasResult() {
        List<Driver.PipelineRes> res = new ArrayList();
        PipelineResult result = new PipelineResult(res.iterator());
        Assert.assertTrue(result.isSuccess());
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class,
        expectedExceptionsMessageRegExp = ".*not support more than one result yet.*")
    public void testTooMuchResult() {
        List<Driver.PipelineRes> res = new ArrayList();
        res.add(Driver.PipelineRes.newBuilder().setPayload(RpcMessageEncoder.encode("test")).build());
        res.add(Driver.PipelineRes.newBuilder().build());
        PipelineResult result = new PipelineResult(res.iterator());
        Assert.assertTrue(result.isSuccess());
    }
}
