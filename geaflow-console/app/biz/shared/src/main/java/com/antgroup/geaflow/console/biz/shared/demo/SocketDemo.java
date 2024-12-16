package com.antgroup.geaflow.console.biz.shared.demo;

import com.antgroup.geaflow.console.common.util.VelocityUtil;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowProcessJob;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SocketDemo extends DemoJob {

    private static final String DEMO_JOB_TEMPLATE = "template/demoJob.vm";

    public GeaflowJob build() {
        GeaflowProcessJob job = new GeaflowProcessJob();
        String code = VelocityUtil.applyResource(DEMO_JOB_TEMPLATE, new HashMap<>());
        job.setUserCode(code);
        job.setName("demoJob");
        return job;
    }
}
