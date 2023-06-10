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

package com.antgroup.geaflow.console.common.dal.mapper;

import com.antgroup.geaflow.console.common.dal.entity.InstanceEntity;
import com.antgroup.geaflow.console.common.dal.entity.ResourceCount;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface InstanceMapper extends GeaflowBaseMapper<InstanceEntity> {

    @Select("<script>\n"
        + "select 'TABLE' as type, name, count(*) as count from geaflow_table where instance_id = #{instanceId} group by name having name"
        + " in \n"
        + "<foreach collection='names' item='item' open='(' separator=',' close=')'> #{item} </foreach> union \n"
        + "select 'GRAPH' as type, name, count(*) as count from geaflow_graph where instance_id = #{instanceId} group by name having name"
        + " in \n"
        + "<foreach collection='names' item='item' open='(' separator=',' close=')'> #{item} </foreach> union \n"
        + "select 'VERTEX' as type, name, count(*) as count from geaflow_vertex where instance_id = #{instanceId} group by name having "
        + "name in \n"
        + "<foreach collection='names' item='item' open='(' separator=',' close=')'> #{item} </foreach> union \n"
        + "select 'EDGE' as type, name, count(*) as count from geaflow_edge where instance_id = #{instanceId} group by name having name "
        + "in \n"
        + "<foreach collection='names' item='item' open='(' separator=',' close=')'> #{item} </foreach> union \n"
        + "select 'FUNCTION' as type, name, count(*) as count from geaflow_function where instance_id = #{instanceId} group by name "
        + "having name in \n"
        + "<foreach collection='names' item='item' open='(' separator=',' close=')'> #{item} </foreach> \n"
        + "</script>")
    List<ResourceCount> getResourceCount(@Param("instanceId") String instanceId, @Param("names") List<String> names);
}
