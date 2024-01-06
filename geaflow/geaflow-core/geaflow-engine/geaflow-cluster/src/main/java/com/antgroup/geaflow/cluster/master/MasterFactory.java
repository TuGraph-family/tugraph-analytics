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
