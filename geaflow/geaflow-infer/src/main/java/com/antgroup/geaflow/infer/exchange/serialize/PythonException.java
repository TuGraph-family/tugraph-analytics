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

package com.antgroup.geaflow.infer.exchange.serialize;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.HashMap;
import java.util.List;

public class PythonException extends GeaflowRuntimeException {

    private static final long serialVersionUID = 4884843316742683086L;

    private static final String TRACEBACK = "_pyroTraceback";

    public String errorTraceback;

    public String pythonExceptionType;

    public PythonException(String message, Throwable cause) {
        super(message, cause);
    }

    public PythonException(String message) {
        super(message);
    }

    public PythonException(Throwable cause) {
        super(cause);
    }

    public void setState(HashMap<String, Object> args) {
        Object tb = args.get(TRACEBACK);
        if (tb instanceof List) {
            StringBuilder sb = new StringBuilder();
            for (Object line : (List<?>) tb) {
                sb.append(line);
            }
            errorTraceback = sb.toString();
        } else {
            errorTraceback = (String) tb;
        }
    }
}
