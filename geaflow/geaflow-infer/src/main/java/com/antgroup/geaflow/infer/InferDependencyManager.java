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

package com.antgroup.geaflow.infer;

import static com.antgroup.geaflow.infer.util.InferFileUtils.MODEL_FILE_EXTENSION;
import static com.antgroup.geaflow.infer.util.InferFileUtils.MODEL_NAME;
import static com.antgroup.geaflow.infer.util.InferFileUtils.PY_FILE_EXTENSION;
import static com.antgroup.geaflow.infer.util.InferFileUtils.REQUIREMENTS_TXT;
import static com.antgroup.geaflow.infer.util.InferFileUtils.getPythonFilesByCondition;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.infer.util.InferFileUtils;
import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferDependencyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferDependencyManager.class);

    private static final String ENV_RUNNER_SH = "infer/env/install-infer-env.sh";

    private static final String INFER_RUNTIME_PATH = "infer/inferRuntime";

    private static final String FILE_IN_JAR_PREFIX = "/";

    private final InferEnvironmentContext environmentContext;

    private final Configuration config;

    private String buildInferEnvShellPath;
    private String inferEnvRequirementsPath;

    public InferDependencyManager(InferEnvironmentContext environmentContext) {
        this.environmentContext = environmentContext;
        this.config = environmentContext.getJobConfig();
        init();
    }

    private void init() {
        List<String> inferRuntimeFiles = buildInferRuntimeFiles();
        for (String inferRuntimeFile : inferRuntimeFiles) {
            InferFileUtils.copyInferFileByURL(environmentContext.getInferFilesDirectory(), inferRuntimeFile);
        }
        String pythonFilesDirectory = environmentContext.getInferFilesDirectory();
        InferFileUtils.prepareInferFilesFromJars(pythonFilesDirectory);
        this.inferEnvRequirementsPath = pythonFilesDirectory + File.separator + REQUIREMENTS_TXT;
        this.buildInferEnvShellPath = InferFileUtils.copyInferFileByURL(environmentContext.getVirtualEnvDirectory(), ENV_RUNNER_SH);
    }

    public String getBuildInferEnvShellPath() {
        return buildInferEnvShellPath;
    }

    public String getInferEnvRequirementsPath() {
        return inferEnvRequirementsPath;
    }

    private List<String> buildInferRuntimeFiles() {
        List<String> runtimeFiles;
        try {
            List<Path> filePaths = InferFileUtils.getPathsFromResourceJAR(INFER_RUNTIME_PATH);
            runtimeFiles = filePaths.stream().map(path -> {
                String filePath = path.toString();
                if (filePath.startsWith(FILE_IN_JAR_PREFIX)) {
                    filePath = filePath.substring(1);
                }
                LOGGER.info("infer runtime file name is {}", filePath);
                return filePath;
            }).collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.error("get infer runtime files error", e);
            throw new GeaflowRuntimeException("get infer runtime files failed", e);
        }
        return runtimeFiles;
    }

    private void prepareInferFiles(InferEnvironmentContext environmentContext) {
        String pythonFilesDirectory = environmentContext.getInferFilesDirectory();
        List<File> modelFile = getPythonFilesByCondition(pathname -> {
            String fileName = pathname.getName();
            String fileExtension = FilenameUtils.getExtension(fileName);
            return pathname.isFile() && MODEL_FILE_EXTENSION.equals(fileExtension);
        });

        Preconditions.checkState(!modelFile.isEmpty(), "model(.pt) file is not exist, please upload model.pt file");
        Preconditions.checkState(modelFile.size() == 1, "upload model.pt num more than 1");
        InferFileUtils.copyPythonFile(pythonFilesDirectory, modelFile.get(0), MODEL_NAME);

        List<File> requirementsFile = getPythonFilesByCondition(pathname -> {
            String fileName = pathname.getName();
            return pathname.isFile() && fileName.equals(REQUIREMENTS_TXT);
        });

        Preconditions.checkState(!requirementsFile.isEmpty(), "please upload requirements.txt "
            + "(build infer env) file");
        Preconditions.checkState(requirementsFile.size() == 1, "upload requirements.txt num more than 1");
        InferFileUtils.copyPythonFile(environmentContext.getVirtualEnvDirectory(), requirementsFile.get(0));

        List<File> pythonFiles = getPythonFilesByCondition(pathname -> {
            String fileName = pathname.getName();
            String fileExtension = FilenameUtils.getExtension(fileName);
            return pathname.isFile() && PY_FILE_EXTENSION.equals(fileExtension);
        });
        Preconditions.checkState(!pythonFiles.isEmpty(), "infer files is empty, please upload "
            + "infer files");
        pythonFiles.forEach(f -> InferFileUtils.copyPythonFile(pythonFilesDirectory, f));
    }
}
