package com.antgroup.geaflow.metaserver.model.protocal.response;

import com.antgroup.geaflow.metaserver.model.protocal.MetaResponse;

public class DefaultResponse implements MetaResponse {

    protected boolean success;
    protected String message;

    public DefaultResponse(boolean success) {
        this.success = success;
    }

    public DefaultResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
