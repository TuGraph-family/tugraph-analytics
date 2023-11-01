package com.antgroup.geaflow.cluster.master;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.mode.JobMode;

public class MasterFactory {

    public static synchronized AbstractMaster create(Configuration configuration) {

        AbstractMaster master;
        if (JobMode.getJobMode(configuration) == JobMode.OLAP_SERVICE
            || JobMode.getJobMode(configuration) == JobMode.STATE_SERVICE) {
            master = new ServiceMaster();
        } else {
            master = new JobMaster();
        }
        return master;
    }

}
