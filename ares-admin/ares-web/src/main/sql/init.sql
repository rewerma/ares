sql-- ----------------------------
-- Table structure for t_datasource
-- ----------------------------
DROP TABLE IF EXISTS `t_datasource`;
CREATE TABLE `t_datasource`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `code` varchar(63) NOT NULL,
  `ds_type` varchar(63) NOT NULL,
  `name` varchar(255) NOT NULL,
  `params` varchar(1024) NOT NULL,
  `c_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_ds_code`(`code` ASC) USING BTREE,
  INDEX `idx_ds_name`(`name` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 COMMENT = 'datasource' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for t_system_config
-- ----------------------------
DROP TABLE IF EXISTS `t_system_config`;
CREATE TABLE `t_system_config`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `key` varchar(255) NOT NULL,
  `value` varchar(1024) NULL DEFAULT NULL,
  `system_env` tinyint NOT NULL,
  `c_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for t_task_definition
-- ----------------------------
DROP TABLE IF EXISTS `t_task_definition`;
CREATE TABLE `t_task_definition`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `code` varchar(63) NOT NULL,
  `name` varchar(255) NOT NULL,
  `task_type` varchar(63) NULL DEFAULT NULL,
  `ds_code` varchar(1024) NULL DEFAULT NULL,
  `env_params` varchar(1024) NULL DEFAULT NULL,
  `task_content` longtext NULL,
  `in_params` varchar(1024) NULL DEFAULT NULL,
  `out_params` varchar(1024) NULL DEFAULT NULL,
  `c_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_td_code`(`code` ASC) USING BTREE,
  INDEX `idx_td_name`(`name` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 COMMENT = 'task definition' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for t_task_instance
-- ----------------------------
DROP TABLE IF EXISTS `t_task_instance`;
CREATE TABLE `t_task_instance`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `batch_code` varchar(63) NOT NULL,
  `task_code` varchar(63) NULL DEFAULT NULL,
  `task_name` varchar(255) NOT NULL,
  `start_time` timestamp NULL DEFAULT NULL,
  `end_time` timestamp NULL DEFAULT NULL,
  `status` int NOT NULL,
  `executor_host` varchar(255) NULL DEFAULT NULL,
  `log_path` varchar(1024) NULL DEFAULT NULL,
  `in_params` varchar(1024) NULL DEFAULT NULL,
  `out_params` varchar(1024) NULL DEFAULT NULL,
  `exe_result` longtext NULL,
  `c_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_ti_td_code`(`task_code` ASC) USING BTREE,
  INDEX `idx_ti_bat_code`(`batch_code` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 COMMENT = 'task definition' ROW_FORMAT = Dynamic;
