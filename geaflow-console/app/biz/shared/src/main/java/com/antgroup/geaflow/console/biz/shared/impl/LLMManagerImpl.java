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

package com.antgroup.geaflow.console.biz.shared.impl;

import com.antgroup.geaflow.console.biz.shared.LLMManager;
import com.antgroup.geaflow.console.biz.shared.convert.LLMViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.LLMView;
import com.antgroup.geaflow.console.common.dal.model.LLMSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowLLMType;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import com.antgroup.geaflow.console.core.service.LLMService;
import com.antgroup.geaflow.console.core.service.NameService;
import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LLMManagerImpl extends NameManagerImpl<GeaflowLLM, LLMView, LLMSearch> implements
    LLMManager {

    @Autowired
    private LLMService llmService;

    @Autowired
    private LLMViewConverter llmViewConverter;

    @Override
    protected NameService<GeaflowLLM, ?, LLMSearch> getService() {
        return llmService;
    }

    @Override
    protected NameViewConverter<GeaflowLLM, LLMView> getConverter() {
        return llmViewConverter;
    }

    @Override
    protected List<GeaflowLLM> parse(List<LLMView> views) {
        return ListUtil.convert(views, llmViewConverter::convert);
    }

    @Override
    public List<GeaflowLLMType> getLLMTypes() {
        return Arrays.asList(GeaflowLLMType.values());
    }
}
