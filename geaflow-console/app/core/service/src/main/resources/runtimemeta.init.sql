/******************************************/
/*   TableName = backend_meta   */
/******************************************/
CREATE TABLE IF NOT EXISTS `backend_meta` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',
  `pk` varchar(255) NOT NULL COMMENT 'Meta key',
  `value` mediumblob DEFAULT NULL COMMENT 'Meta value',
  `gmt_create` timestamp NULL DEFAULT NULL COMMENT 'Create Time',
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Modify Time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pk` (`pk`),
  KEY `idx_gmt_modified` (`gmt_modified`)
)  DEFAULT CHARSET = utf8mb4 COMMENT = 'GeaFlow Task Runtime Meta'
;