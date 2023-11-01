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

package com.antgroup.geaflow.dashboard.agent.handler;

import static com.antgroup.geaflow.dashboard.agent.util.FileUtil.checkPaginationRequest;

import com.antgroup.geaflow.cluster.web.api.ApiResponse;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dashboard.agent.model.FileInfo;
import com.antgroup.geaflow.dashboard.agent.model.PaginationRequest;
import com.antgroup.geaflow.dashboard.agent.model.PaginationResponse;
import com.antgroup.geaflow.dashboard.agent.util.FileUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/logs")
public class LogRestHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogRestHandler.class);

    private final String deployLogPath;

    private final String runtimeLogDirPath;

    private final Pattern logPattern;

    public LogRestHandler(String deployLogPath, String runtimeLogDirPath) {
        this.deployLogPath = deployLogPath;
        this.runtimeLogDirPath = runtimeLogDirPath;
        this.logPattern = Pattern.compile(String.format("%s.*\\.log(\\.\\d*)?", this.runtimeLogDirPath));
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<List<FileInfo>> getLogList() {
        try {
            List<FileInfo> logs = new ArrayList<>();
            File file = new File(runtimeLogDirPath);
            String[] fileList = file.list();
            for (String f : fileList) {
                String logPath = runtimeLogDirPath + File.separator + f;
                File logFile = new File(logPath);
                if (logFile.isFile() && logPattern.matcher(logPath).matches()) {
                    FileInfo fileInfo = FileUtil.buildFileInfo(logFile, logPath);
                    logs.add(fileInfo);
                }
            }
            file = new File(deployLogPath);
            if (file.exists() && file.isFile()) {
                FileInfo fileInfo = new FileInfo();
                fileInfo.setPath(deployLogPath);
                fileInfo.setSize(file.length());
                logs.add(fileInfo);
            }
            return ApiResponse.success(logs);
        } catch (Throwable t) {
            LOGGER.error("Query log file list failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/content")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<PaginationResponse<String>> getLogContent(@QueryParam("path") String logPath,
                                                                 @QueryParam("pageNo") int pageNo,
                                                                 @QueryParam("pageSize") int pageSize) {
        try {
            checkLogPath(logPath);
            PaginationRequest request = new PaginationRequest(pageNo, pageSize);
            checkPaginationRequest(request);
            PaginationResponse<String> response = FileUtil.getFileContent(request, logPath);
            if (response == null) {
                throw new GeaflowRuntimeException(
                    String.format("Log file %s not exists.", logPath));
            }
            return ApiResponse.success(response);
        } catch (Throwable t) {
            LOGGER.error("Query log content {} failed. {}", logPath, t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    private void checkLogPath(String logPath) {
        if (logPath == null || (!logPath.equals(deployLogPath) && !logPattern.matcher(logPath).matches())) {
            throw new GeaflowRuntimeException(String.format("Log path %s is invalid.", logPath));
        }
    }
}
