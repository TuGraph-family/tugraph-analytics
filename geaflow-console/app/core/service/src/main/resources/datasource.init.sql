/******************************************/
/*   TableName = geaflow_audit   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_audit` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `tenant_id` char(64) NOT NULL COMMENT '租户ID',
  `guid` char(64) NOT NULL COMMENT 'ID',
  `creator_id` char(64) NOT NULL COMMENT '创建人ID',
  `modifier_id` char(64) NOT NULL COMMENT '修改人ID',
  `resource_id` char(64) NOT NULL COMMENT '资源id',
  `resource_type` varchar(64) NOT NULL COMMENT '资源类型',
  `operation_type` varchar(64) NOT NULL COMMENT '操作',
  `detail` text DEFAULT NULL COMMENT '其他信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_resource` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '审计'
;

/******************************************/
/*   TableName = geaflow_authorization   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_authorization` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人ID',
`modifier_id` char(64) NOT NULL COMMENT '修改人ID',
`user_id` char(64) NOT NULL COMMENT '用户ID',
`authority_type` char(32) NOT NULL COMMENT '角色类型',
`resource_type` char(32) NOT NULL COMMENT '资源类型',
`resource_id` char(64) NOT NULL COMMENT '资源ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_resource` (`tenant_id`, `user_id`, `authority_type`, `resource_type`, `resource_id`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '权限表'
;

/******************************************/
/*   TableName = geaflow_cluster   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_cluster` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`type` char(32) NOT NULL COMMENT '集群类型',
`config` mediumtext NOT NULL COMMENT '集群配置',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '集群表'
;

/******************************************/
/*   TableName = geaflow_edge   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_edge` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`instance_id` char(64) NOT NULL COMMENT '实例id',
`direction` varchar(32) NOT NULL DEFAULT 'BOTH' COMMENT '边方向',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '边表'
;

/******************************************/
/*   TableName = geaflow_field   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_field` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`type` varchar(32) NOT NULL COMMENT '字段类型(String、boolean)',
`category` varchar(1024) NOT NULL COMMENT '字段类别(点、边)',
`resource_id` char(64) NOT NULL COMMENT '资源id',
`resource_type` varchar(32) NOT NULL COMMENT '资源类型',
`sort_key` int(10) unsigned NOT NULL COMMENT '排序键',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`resource_id`, `resource_type`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '字段'
;

/******************************************/
/*   TableName = geaflow_function   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_function` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`instance_id` char(64) NOT NULL COMMENT '实例id',
`jar_package_id` char(64) DEFAULT NULL COMMENT 'jar包id',
`entry_class` varchar(256) DEFAULT NULL COMMENT '入口方法',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '函数'
;

/******************************************/
/*   TableName = geaflow_graph   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_graph` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`plugin_config_id` char(64) NOT NULL COMMENT '配置id',
`instance_id` char(64) NOT NULL COMMENT '实例id',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '图'
;

/******************************************/
/*   TableName = geaflow_graph_struct_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_graph_struct_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`graph_id` char(64) NOT NULL COMMENT '图id',
`resource_id` char(64) NOT NULL COMMENT '资源id（点、边）',
`resource_type` varchar(32) NOT NULL COMMENT '资源类型',
`sort_key` int(10) unsigned NOT NULL COMMENT '排序键',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`graph_id`, `resource_id`, `resource_type`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '图点边映射表'
;

/******************************************/
/*   TableName = geaflow_instance   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_instance` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建者id',
`modifier_id` char(64) NOT NULL COMMENT '修改者id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '实例表'
;

/******************************************/
/*   TableName = geaflow_job   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_job` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`instance_id` char(64) NOT NULL COMMENT '实例id',
`type` varchar(32) NOT NULL COMMENT '作业类型',
`user_code` text DEFAULT NULL COMMENT '代码',
`struct_mappings` text DEFAULT NULL COMMENT '映射',
`sla_id` char(64) DEFAULT NULL COMMENT '高保',
`jar_package_id` char(64) DEFAULT NULL COMMENT 'jar包id',
`entry_class` varchar(256) DEFAULT NULL COMMENT '入口方法',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '作业'
;

/******************************************/
/*   TableName = geaflow_job_resource_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_job_resource_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`job_id` char(64) NOT NULL COMMENT '表id',
`resource_name` varchar(128) NOT NULL COMMENT '资源id（点、边）',
`resource_type` varchar(32) NOT NULL COMMENT '资源类型',
`instance_id` char(64) NOT NULL COMMENT '实例id',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`job_id`, `resource_name`, `resource_type`, `instance_id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'job和资源映射'
;

/******************************************/
/*   TableName = geaflow_plugin   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_plugin` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`plugin_type` varchar(32) NOT NULL COMMENT '插件类型',
`plugin_category` varchar(32) NOT NULL COMMENT '插件类型',
`jar_package_id` char(64) DEFAULT NULL COMMENT 'jar id',
`version` varchar(128) DEFAULT NULL COMMENT '版本',
`data_plugin_id` char(64) DEFAULT NULL COMMENT '存储配置id(OSS,pangu)',
`system` tinyint(4) DEFAULT NULL COMMENT '是否系统级别',
`entry_class` varchar(256) DEFAULT NULL COMMENT '入口方法',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '插件'
;

/******************************************/
/*   TableName = geaflow_plugin_config   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_plugin_config` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`type` varchar(32) NOT NULL COMMENT '插件类型',
`config` text DEFAULT NULL COMMENT '配置',
`category` varchar(32) NOT NULL COMMENT '插件种类（图，表）',
`system` tinyint(4) DEFAULT NULL COMMENT '是否系统级别',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '插件配置'
;

/******************************************/
/*   TableName = geaflow_release   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_release` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`job_id` char(64) NOT NULL COMMENT '作业id',
`cluster_id` char(64) NOT NULL COMMENT '集群id',
`job_plan` text NOT NULL COMMENT '执行计划',
`job_config` text NOT NULL COMMENT '作业参数',
`cluster_config` text NOT NULL COMMENT '集群参数',
`version_id` char(64) NOT NULL COMMENT '引擎id',
`version` int(10) unsigned NOT NULL COMMENT '版本号',
`url` varchar(1024) NOT NULL COMMENT 'Package地址',
`md5` varchar(32) NOT NULL COMMENT 'Package MD5',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '发布'
;

/******************************************/
/*   TableName = geaflow_remote_file   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_remote_file` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`type` varchar(32) NOT NULL COMMENT '类型',
`url` varchar(1024) NOT NULL COMMENT '地址',
`md5` char(32) NOT NULL COMMENT 'md5',
`system` tinyint(4) NOT NULL COMMENT '是否系统级别',
`path` varchar(1024) NOT NULL COMMENT '路径',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '文件'
;

/******************************************/
/*   TableName = geaflow_system_config   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_system_config` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建者id',
`modifier_id` char(64) NOT NULL COMMENT '修改者id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`value` mediumtext DEFAULT NULL COMMENT '配置值',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '系统配置表'
;

/******************************************/
/*   TableName = geaflow_table   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_table` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`plugin_config_id` char(64) NOT NULL COMMENT '配置id',
`instance_id` char(64) NOT NULL COMMENT '实例id',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '表'
;

/******************************************/
/*   TableName = geaflow_task   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_task` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`release_id` char(64) NOT NULL COMMENT '作业发布包id',
`start_time` timestamp NULL DEFAULT NULL COMMENT '开始时间',
`end_time` timestamp NULL DEFAULT NULL COMMENT '停止时间',
`type` varchar(32) NOT NULL COMMENT '任务类型',
`status` varchar(32) NOT NULL COMMENT '任务状态',
`runtime_meta_config_id` char(64) NOT NULL COMMENT '运行时元数据',
`ha_meta_config_id` char(64) NOT NULL COMMENT 'ha',
`metric_config_id` char(64) NOT NULL COMMENT '作业指标',
`data_config_id` char(64) NOT NULL COMMENT '存储配置',
`job_id` char(64) NOT NULL COMMENT '作业id',
`token` char(64) DEFAULT NULL COMMENT 'token',
`host` varchar(256) DEFAULT NULL COMMENT '调度主机',
`handle` text DEFAULT NULL COMMENT '句柄',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '任务'
;

/******************************************/
/*   TableName = geaflow_tenant   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_tenant` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建者id',
`modifier_id` char(64) NOT NULL COMMENT '修改者id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`quota_id` char(64) DEFAULT NULL COMMENT '配额ID',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '租户表'
;

/******************************************/
/*   TableName = geaflow_tenant_user_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_tenant_user_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建者id',
`modifier_id` char(64) NOT NULL COMMENT '修改者id',
`user_id` char(64) NOT NULL COMMENT '用户ID',
`active` tinyint(4) NOT NULL COMMENT '默认租户',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `user_id`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '租户用户映射表'
;

/******************************************/
/*   TableName = geaflow_user   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_user` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) DEFAULT NULL COMMENT '创建人ID',
`modifier_id` char(64) DEFAULT NULL COMMENT '修改人ID',
`name` varchar(128) NOT NULL COMMENT '用户名',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`password_sign` varchar(128) DEFAULT NULL COMMENT '密码签名',
`phone` char(32) DEFAULT NULL COMMENT '电话',
`email` char(128) DEFAULT NULL COMMENT '邮箱',
`session_token` char(64) DEFAULT NULL COMMENT '会话令牌',
`system_session` tinyint(4) DEFAULT NULL COMMENT '是否系统会话',
`access_time` timestamp NULL DEFAULT NULL COMMENT '上次访问时间',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`name`),
UNIQUE KEY `uk_session_token` (`session_token`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '用户表'
;

/******************************************/
/*   TableName = geaflow_user_role_mapping   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_user_role_mapping` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户ID',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人ID',
`modifier_id` char(64) NOT NULL COMMENT '修改人ID',
`user_id` char(64) NOT NULL COMMENT '用户ID',
`role_type` char(32) NOT NULL COMMENT '角色类型',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_mapping` (`tenant_id`, `user_id`, `role_type`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '用户角色映射表'
;

/******************************************/
/*   TableName = geaflow_version   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_version` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) DEFAULT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'id',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`engine_jar_id` char(64) DEFAULT NULL COMMENT '引擎jar',
`lang_jar_id` char(64) DEFAULT NULL COMMENT 'dsl jar',
`publish` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '是否发布',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_guid` (`guid`),
UNIQUE KEY `uk_name` (`tenant_id`, `name`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '作业'
;

/******************************************/
/*   TableName = geaflow_vertex   */
/******************************************/
CREATE TABLE IF NOT EXISTS `geaflow_vertex` (
`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
`gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`tenant_id` char(64) NOT NULL COMMENT '租户id',
`guid` char(64) NOT NULL COMMENT 'ID',
`creator_id` char(64) NOT NULL COMMENT '创建人id',
`modifier_id` char(64) NOT NULL COMMENT '修改人id',
`name` varchar(128) NOT NULL COMMENT '名字',
`comment` varchar(1024) DEFAULT NULL COMMENT '描述',
`instance_id` char(64) NOT NULL COMMENT '实例id',
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name` (`tenant_id`, `instance_id`, `name`),
UNIQUE KEY `uk_guid` (`guid`)
) DEFAULT CHARSET = utf8mb4 COMMENT = '点表'
;