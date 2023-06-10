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

package com.antgroup.geaflow.console.common.util.exception;

import com.antgroup.geaflow.console.common.util.Fmt;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

public class GeaflowCompileException extends GeaflowException {

    public GeaflowCompileException(String fmt, Object... args) {
        super(fmt, args);
    }

    public String getDisplayMessage() {
        List<CauseInfo> causes = new ArrayList<>();
        for (Throwable error = getCause(); error != null; error = error.getCause()) {
            String clazz = error.getClass().getSimpleName();
            String message = StringUtils.substringBefore(error.getMessage(), "\n");
            causes.add(new CauseInfo(clazz, message));
        }

        StringBuilder sb = new StringBuilder();
        sb.append(getMessage());
        sb.append("\nCaused by:");
        for (int i = 0; i < causes.size(); i++) {
            CauseInfo causeInfo = causes.get(i);
            String align = StringUtils.repeat(">", i + 1);
            sb.append(Fmt.as("\n{} [{}]: {}", align, causeInfo.getClassName(), causeInfo.getMessage()));
        }

        return sb.toString();
    }

    @Getter
    @AllArgsConstructor
    private static class CauseInfo {

        private String className;

        private String message;

    }
}
