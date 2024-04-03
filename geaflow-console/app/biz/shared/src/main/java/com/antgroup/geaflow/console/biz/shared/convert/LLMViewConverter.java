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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.biz.shared.view.LLMView;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class LLMViewConverter extends NameViewConverter<GeaflowLLM, LLMView> {

    @Override
    public void merge(LLMView view, LLMView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getType()).ifPresent(view::setType);
        Optional.ofNullable(updateView.getUrl()).ifPresent(view::setUrl);
        Optional.ofNullable(updateView.getArgs()).ifPresent(view::setArgs);
    }

    @Override
    protected LLMView modelToView(GeaflowLLM model) {
        LLMView view = super.modelToView(model);
        view.setType(model.getType());
        view.setUrl(model.getUrl());
        view.setArgs(JSON.toJSONString(model.getArgs()));
        return view;
    }

    @Override
    protected GeaflowLLM viewToModel(LLMView view) {
        GeaflowLLM model = super.viewToModel(view);
        model.setType(view.getType());
        model.setUrl(view.getUrl());

        GeaflowConfig config = Optional.ofNullable(JSON.parseObject(view.getArgs(), GeaflowConfig.class)).orElse(new GeaflowConfig());
        model.setArgs(config);
        return model;
    }

    public GeaflowLLM convert(LLMView view) {
        return viewToModel(view);
    }

}
