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

package com.antgroup.geaflow.common.exception;

public class GeaflowRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 8832569372505798566L;

    private String errorMessage;

    public GeaflowRuntimeException(Throwable e) {
        super(e);
    }

    public GeaflowRuntimeException(String errorMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
    }

    public GeaflowRuntimeException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

}
