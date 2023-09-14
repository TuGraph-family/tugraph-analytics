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

package com.antgroup.geaflow.dsl.runtime;

import com.antgroup.geaflow.dsl.common.compile.CompileContext;
import com.antgroup.geaflow.dsl.common.compile.CompileResult;
import com.antgroup.geaflow.dsl.common.compile.FunctionInfo;
import com.antgroup.geaflow.dsl.common.compile.QueryCompiler;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompilerTest {

    @Test
    public void testCompile() throws IOException {
        QueryCompiler compiler = new QueryClient();
        String script = IOUtils.resourceToString("/query/compile.sql", Charset.defaultCharset());
        CompileContext context = new CompileContext();
        context.setConfig(new HashMap<>());
        context.setParallelisms(new HashMap<>());

        CompileResult result = compiler.compile(script, context);
        Assert.assertEquals(result.getSourceGraphs().stream().map(g -> g.getGraphName()).collect(
            Collectors.toSet()), Sets.newHashSet("modern"));
        Assert.assertEquals(result.getTargetGraphs(), Sets.newHashSet());

        Assert.assertEquals(result.getSourceTables(), Sets.newHashSet());
        Assert.assertEquals(result.getTargetTables().stream().map(t -> t.getTableName()).collect(
            Collectors.toSet()), Sets.newHashSet("tbl_result"));
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(result.getPhysicPlan()),
            "{\"vertices\":{\"1\":{\"vertexType\":\"source\",\"id\":\"1\",\"parallelism\":1,\"parents\":[],"
                + "\"innerPlan\":{\"vertices\":{\"1-1\":{\"id\":\"1-1\",\"parallelism\":1,"
                + "\"operator\":\"WindowSourceOperator\",\"operatorName\":\"1\",\"parents\":[]},"
                + "\"1-4\":{\"id\":\"1-4\",\"parallelism\":1,\"operator\":\"KeySelectorOperator\","
                + "\"operatorName\":\"4\",\"parents\":[{\"id\":\"1-1\"}]}}}},\"2\":{\"vertexType\":\"source\","
                + "\"id\":\"2\",\"parallelism\":1,\"parents\":[],"
                + "\"innerPlan\":{\"vertices\":{\"2-2\":{\"id\":\"2-2\",\"parallelism\":1,"
                + "\"operator\":\"WindowSourceOperator\",\"operatorName\":\"2\",\"parents\":[]},"
                + "\"2-5\":{\"id\":\"2-5\",\"parallelism\":1,\"operator\":\"KeySelectorOperator\","
                + "\"operatorName\":\"5\",\"parents\":[{\"id\":\"2-2\"}]}}}},"
                + "\"3\":{\"vertexType\":\"vertex_centric\",\"id\":\"3\",\"parallelism\":2,"
                + "\"operator\":\"StaticGraphVertexCentricTraversalAllOp\","
                + "\"operatorName\":\"GeaFlowStaticVCTraversal\",\"parents\":[{\"id\":\"3\","
                + "\"partitionType\":\"key\"},{\"id\":\"2\",\"partitionType\":\"key\"},{\"id\":\"1\","
                + "\"partitionType\":\"key\"}]},\"6\":{\"vertexType\":\"process\",\"id\":\"6\",\"parallelism\":2,"
                + "\"parents\":[{\"id\":\"3\",\"partitionType\":\"forward\"}],"
                + "\"innerPlan\":{\"vertices\":{\"6-7\":{\"id\":\"6-7\",\"parallelism\":2,\"operator\":\"MapOperator\","
                + "\"operatorName\":\"Project-1\",\"parents\":[{\"id\":\"6-6\"}]},\"6-8\":{\"id\":\"6-8\","
                + "\"parallelism\":2,\"operator\":\"SinkOperator\",\"operatorName\":\"TableSink-2\","
                + "\"parents\":[{\"id\":\"6-7\"}]},\"6-6\":{\"id\":\"6-6\",\"parallelism\":2,"
                + "\"operator\":\"FlatMapOperator\",\"operatorName\":\"TraversalResponseToRow-0\",\"parents\":[]}}}}}}"
        );
    }

    @Test
    public void testFindUnResolvedFunctions() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "create function f0 as 'com.antgroup.udf.TestUdf';"
            + "select f1(name), substr(name, 1, 10) from t0;"
            + "use instance instance0;"
            + "select f2(id), max(id) from t0;"
            + "create view v0(c0, c1) as select f3(id) as c0, c1 from t1";

        Set<FunctionInfo> unResolvedFunctions = compiler.getUnResolvedFunctions(script, context);
        Assert.assertEquals(unResolvedFunctions.size(), 4);
        List<String> functions =
            unResolvedFunctions.stream().map(FunctionInfo::toString).collect(Collectors.toList());
        Assert.assertEquals(functions.get(0), "instance0.f3");
        Assert.assertEquals(functions.get(1), "instance0.f2");
        Assert.assertEquals(functions.get(2), "default.f1");
        Assert.assertEquals(functions.get(3), "default.f0");
    }
}
