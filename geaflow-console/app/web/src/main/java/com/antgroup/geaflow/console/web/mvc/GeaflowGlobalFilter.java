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

package com.antgroup.geaflow.console.web.mvc;

import com.antgroup.geaflow.console.common.util.Fmt;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;

@Slf4j
@WebFilter(urlPatterns = {"/auth/*", "/api/*"})
public class GeaflowGlobalFilter implements Filter, Ordered {

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
        throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        try {
            ContextHolder.init();
            ContextHolder.get().setRequest(request);
            filterChain.doFilter(request, response);

        } catch (Exception e) {
            String mesage = Fmt.as("Request url {} failed", request.getRequestURI());
            log.info(mesage);
            log.error(mesage, e.getCause());
            GeaflowApiResponse.error(e.getCause()).write(response);

        } finally {
            ContextHolder.destroy();
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
