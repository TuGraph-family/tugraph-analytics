package com.antgroup.geaflow.shuffle.service.impl;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.shuffle.message.PipelineInfo;
import com.antgroup.geaflow.shuffle.message.ShuffleId;
import com.antgroup.geaflow.shuffle.service.IShuffleMaster;

public class AutoShuffleMaster implements IShuffleMaster {

    @Override
    public void init(Configuration jobConfig) {
    }

    @Override
    public void clean(ShuffleId shuffleId) {
    }

    @Override
    public void clean(PipelineInfo jobInfo) {
    }

    @Override
    public void close() {
    }

}
