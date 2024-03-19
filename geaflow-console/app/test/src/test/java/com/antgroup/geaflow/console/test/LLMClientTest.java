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

package com.antgroup.geaflow.console.test;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.llm.CodefuseConfigArgsClass;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import com.antgroup.geaflow.console.core.model.llm.OpenAIConfigArgsClass;
import com.antgroup.geaflow.console.core.service.llm.CodefuseClient;
import com.antgroup.geaflow.console.core.service.llm.LLMClient;
import com.antgroup.geaflow.console.core.service.llm.LocalClient;
import com.antgroup.geaflow.console.core.service.llm.OpenAiClient;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class LLMClientTest {

    @Test(enabled = false)
    public void testLocal() {
        LLMClient llmClient = LocalClient.getInstance();

        GeaflowLLM geaflowLLM = new GeaflowLLM();
        geaflowLLM.setUrl("http://127.0.0.1:8000/completion");
        String answer = llmClient.call(geaflowLLM, "找出小红的10个朋友?");
        log.info(answer);
    }

    @Test(enabled = false)
    public void testOpenAI() {

        LLMClient llmClient = OpenAiClient.getInstance();

        GeaflowLLM geaflowLLM = new GeaflowLLM();
        geaflowLLM.setUrl("https://api.openai.com/v1/chat/completions");

        GeaflowConfig geaflowConfig = new GeaflowConfig();
        
        OpenAIConfigArgsClass configArgsClass = new OpenAIConfigArgsClass();
        // set own sk
        String sk = "sk-xxxx";
        configArgsClass.setApiKey(sk);
        
        Assert.assertThrows(GeaflowException.class, () -> geaflowConfig.parse(OpenAIConfigArgsClass.class));
        
        configArgsClass.setModelId("ft:gpt-3.5-turbo-1106:personal:geaflow:8zLbe4Ua");
        geaflowLLM.setArgs(configArgsClass.build());
        
        for (int i = 0; i < 5; i++) {
            String answer = llmClient.call(geaflowLLM, "找出小红的10个朋友?");
            log.info(answer);
        }


    }

    @Test(enabled = false)
    public void testCodefuse() {
        LLMClient llmClient = CodefuseClient.getInstance();

        GeaflowLLM geaflowLLM = new GeaflowLLM();
        geaflowLLM.setUrl("https://riskautopilot-pre.alipay.com/v1/gpt/codegpt/task");
        
        CodefuseConfigArgsClass configArgsClass = new CodefuseConfigArgsClass();
        configArgsClass.setChainName("v18");
        configArgsClass.setSceneName("codegpt_single_finetune_v18");
        geaflowLLM.setArgs(configArgsClass.build());
        
        String answer = llmClient.call(geaflowLLM, "找出小红的10个朋友?");
        log.info(answer);
    }


}
