package com.antgroup.geaflow.metaserver;

import com.antgroup.geaflow.common.config.Configuration;

public class MetaServerContext {

    private Configuration configuration;
    private boolean isRecover;

    public MetaServerContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean isRecover() {
        return isRecover;
    }

    public void setRecover(boolean recover) {
        isRecover = recover;
    }
}
