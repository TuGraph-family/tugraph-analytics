package com.antgroup.geaflow.common.mode;


import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;

public enum JobMode {
    /**
     * compute job mode.
     */
    COMPUTE,

    /**
     * Olap service job mode.
     */
    OLAP_SERVICE,

    /**
     * State service job mode.
     */
    STATE_SERVICE;

    public static JobMode getJobMode(Configuration configuration) {
        String jobMode = configuration.getString(ExecutionConfigKeys.JOB_MODE);
        return valueOf(jobMode.toUpperCase());
    }
}
