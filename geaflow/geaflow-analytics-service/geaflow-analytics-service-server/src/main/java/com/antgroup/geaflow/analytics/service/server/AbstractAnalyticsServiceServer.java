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

package com.antgroup.geaflow.analytics.service.server;

import static com.antgroup.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_COMPILE_SCHEMA_ENABLE;
import static com.antgroup.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_QUERY;
import static com.antgroup.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE;
import static com.antgroup.geaflow.common.config.keys.DSLConfigKeys.GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE;

import com.antgroup.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import com.antgroup.geaflow.analytics.service.query.QueryError;
import com.antgroup.geaflow.analytics.service.query.QueryInfo;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.common.blocking.map.BlockingMap;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.dsl.common.compile.CompileContext;
import com.antgroup.geaflow.dsl.common.compile.CompileResult;
import com.antgroup.geaflow.dsl.runtime.QueryClient;
import com.antgroup.geaflow.metaserver.internal.MetaServerClient;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import com.antgroup.geaflow.pipeline.service.IPipelineServiceExecutorContext;
import com.antgroup.geaflow.pipeline.service.IServiceServer;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.plan.PipelinePlanBuilder;
import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;
import com.antgroup.geaflow.runtime.pipeline.PipelineContext;
import com.antgroup.geaflow.runtime.pipeline.service.PipelineServiceContext;
import com.antgroup.geaflow.runtime.pipeline.service.PipelineServiceExecutorContext;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAnalyticsServiceServer implements IServiceServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractAnalyticsServiceServer.class);

    private static final String ANALYTICS_SERVICE_PREFIX = "analytics-service-";
    private static final String SERVER_EXECUTOR = "server-executor";

    protected int port;
    protected int maxRequests;
    protected boolean running;
    protected PipelineService pipelineService;
    protected PipelineServiceExecutorContext serviceExecutorContext;
    protected BlockingQueue<QueryInfo> requestBlockingQueue;
    protected BlockingMap<String, Future<IExecutionResult>> responseBlockingMap;
    protected BlockingQueue<Long> cancelRequestBlockingQueue;
    protected BlockingQueue<Object> cancelResponseBlockingQueue;
    protected MetaServerClient metaServerClient;
    protected Semaphore semaphore;
    private ExecutorService executorService;

    protected Configuration configuration;
    protected boolean enableCompileSchema;

    @Override
    public void init(IPipelineServiceExecutorContext context) {
        this.serviceExecutorContext = (PipelineServiceExecutorContext) context;
        this.pipelineService = this.serviceExecutorContext.getPipelineService();
        this.configuration = context.getConfiguration();
        this.port = configuration.getInteger(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT);
        this.maxRequests = configuration.getInteger(
            AnalyticsServiceConfigKeys.MAX_REQUEST_PER_SERVER);
        this.requestBlockingQueue = new LinkedBlockingQueue<>(maxRequests);
        this.responseBlockingMap = new BlockingMap<>();
        this.cancelRequestBlockingQueue = new LinkedBlockingQueue<>(maxRequests);
        this.cancelResponseBlockingQueue = new LinkedBlockingQueue<>(maxRequests);
        this.semaphore = new Semaphore(maxRequests);
        this.executorService = Executors.newFixedThreadPool(this.maxRequests,
            ThreadUtil.namedThreadFactory(true, SERVER_EXECUTOR,
                new ComponentUncaughtExceptionHandler()));
        this.enableCompileSchema = configuration.getBoolean(ANALYTICS_COMPILE_SCHEMA_ENABLE);
    }

    @Override
    public void stopServer() {
        this.running = false;
        if (this.metaServerClient != null) {
            this.metaServerClient.close();
        }
    }

    public static QueryResults getQueryResults(QueryInfo queryInfo,
                                               BlockingMap<String, Future<IExecutionResult>> responseBlockingMap) throws Exception {
        Future<IExecutionResult> resultFuture = responseBlockingMap.get(queryInfo.getQueryId());
        IExecutionResult result = resultFuture.get();
        QueryResults queryResults;
        String queryId = queryInfo.getQueryId();
        if (result.isSuccess()) {
            List<List<ResponseResult>> responseResult = (List<List<ResponseResult>>) result.getResult();
            queryResults = new QueryResults(queryId, responseResult);
        } else {
            String errorMsg = result.getError().toString();
            queryResults = new QueryResults(queryId, new QueryError(errorMsg));
        }
        queryResults.setResultMeta(queryInfo.getScriptSchema());
        return queryResults;
    }

    protected void waitForExecuted() {
        registerServiceInfo();

        while (this.running) {
            try {
                QueryInfo queryInfo = requestBlockingQueue.take();
                String queryScript = queryInfo.getQueryScript();
                String queryId = queryInfo.getQueryId();
                try {
                    if (enableCompileSchema) {
                        CompileResult compileResult = compileQuerySchema(queryInfo.getQueryScript(), configuration);
                        RelDataType relDataType = compileResult.getCurrentResultType();
                        queryInfo.setScriptSchema(relDataType);
                    }
                    Future<IExecutionResult> future = executorService.submit(() -> executeQuery(queryScript));
                    responseBlockingMap.put(queryId, future);
                } catch (Throwable t) {
                    LOGGER.error("execute query: {} failed", queryInfo, t);
                    QueryResults queryResults = new QueryResults(queryId,
                        new QueryError(t.getMessage()));
                    Future<IExecutionResult> future = new FutureTask<>(
                        () -> (IExecutionResult) queryResults);
                    responseBlockingMap.put(queryId, future);
                }
            } catch (Throwable t) {
                if (this.running) {
                    LOGGER.error("analytics service abnormal {}", t.getMessage(), t);
                }
            }
        }
    }

    protected static CompileResult compileQuerySchema(String query, Configuration configuration) {
        QueryClient queryManager = new QueryClient();
        CompileContext compileContext = new CompileContext();
        compileContext.setConfig(configuration.getConfigMap());
        compileContext.getConfig().put(GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE.getKey(),
            Boolean.FALSE.toString());
        return queryManager.compile(query, compileContext);
    }

    private void registerServiceInfo() {
        // First initialize analytics service instance and only in service 0.
        if (serviceExecutorContext.getDriverIndex() == 0) {
            String analyticsQuery = serviceExecutorContext.getPipelineContext().getConfig()
                .getString(ANALYTICS_QUERY);
            Preconditions.checkArgument(analyticsQuery != null, "analytics query must be not null");
            executeQuery(analyticsQuery);
            LOGGER.info("service index {} analytics query execute successfully",
                serviceExecutorContext.getDriverIndex());
        }
        // Register analytics service info.
        if (serviceExecutorContext.getConfiguration()
            .getBoolean(ANALYTICS_SERVICE_REGISTER_ENABLE)) {
            metaServerClient = new MetaServerClient(serviceExecutorContext.getConfiguration());
            metaServerClient.registerService(NamespaceType.DEFAULT,
                ANALYTICS_SERVICE_PREFIX + serviceExecutorContext.getDriverIndex(),
                new HostAndPort(ProcessUtil.getHostIp(), port));
            LOGGER.info("service index {} register analytics service {}:{}",
                serviceExecutorContext.getDriverIndex(), ProcessUtil.getHostIp(), port);
        }
        this.running = true;
    }

    private IExecutionResult executeQuery(String query) {
        // User pipeline Task.
        PipelineContext pipelineContext = new PipelineContext(
            serviceExecutorContext.getPipelineContext().getName(),
            serviceExecutorContext.getPipelineContext().getConfig());
        serviceExecutorContext.getPipelineContext().getViewDescMap().forEach(
            (s, iViewDesc) -> pipelineContext.addView(iViewDesc));
        PipelineServiceContext serviceContext = new PipelineServiceContext(
            System.currentTimeMillis(), pipelineContext, query);
        pipelineService.execute(serviceContext);
        PipelinePlanBuilder pipelinePlanBuilder = new PipelinePlanBuilder();
        // 1. Build pipeline graph plan.
        PipelineGraph pipelineGraph = pipelinePlanBuilder.buildPlan(pipelineContext);
        // 2. Opt pipeline graph plan.
        pipelinePlanBuilder.optimizePlan(pipelineContext.getConfig());
        // 3. Execute query.
        IExecutionResult result = this.serviceExecutorContext.getPipelineRunner()
            .runPipelineGraph(pipelineGraph, serviceExecutorContext);
        return result;
    }
}
