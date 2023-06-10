/******************************************/
/*   TableName = backend_meta   */
/******************************************/
CREATE TABLE IF NOT EXISTS `backend_meta` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `pk` varchar(255) NOT NULL COMMENT 'Meta key',
  `value` mediumblob DEFAULT NULL COMMENT 'Meta value',
  `gmt_create` timestamp NULL DEFAULT NULL COMMENT '创建时间',
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pk` (`pk`),
  KEY `idx_gmt_modified` (`gmt_modified`)
)  DEFAULT CHARSET = utf8mb4 COMMENT = 'GeaFlow作业运行时Meta信息'
;