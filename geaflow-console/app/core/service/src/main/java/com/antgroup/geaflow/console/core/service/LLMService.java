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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.LLMDao;
import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.entity.LLMEntity;
import com.antgroup.geaflow.console.common.dal.model.LLMSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import com.antgroup.geaflow.console.core.service.converter.LLMConverter;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LLMService extends NameService<GeaflowLLM, LLMEntity, LLMSearch> {

    @Autowired
    private LLMDao llmDao;

    @Autowired
    private LLMConverter llmConverter;

    @Override
    protected List<GeaflowLLM> parse(List<LLMEntity> entities) {
        return ListUtil.convert(entities, llmConverter::convert);
    }

    @Override
    protected NameDao<LLMEntity, LLMSearch> getDao() {
        return llmDao;
    }

    @Override
    protected NameConverter<GeaflowLLM, LLMEntity> getConverter() {
        return llmConverter;
    }

}

