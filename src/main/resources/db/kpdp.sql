SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS `tb_voucher_order`;
DROP TABLE IF EXISTS `tb_order_outbox`;
DROP TABLE IF EXISTS `tb_seckill_voucher`;
DROP TABLE IF EXISTS `tb_user`;

CREATE TABLE `tb_user` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `phone` varchar(11) NOT NULL COMMENT '手机号',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

INSERT INTO `tb_user` (`id`, `phone`, `create_time`, `update_time`) VALUES
(1, '13800000001', '2026-04-25 00:00:00', '2026-04-25 00:00:00'),
(2, '13800000002', '2026-04-25 00:00:00', '2026-04-25 00:00:00');

CREATE TABLE `tb_seckill_voucher` (
  `voucher_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '秒杀券 ID',
  `title` varchar(255) NOT NULL COMMENT '秒杀券标题',
  `stock` int(8) NOT NULL COMMENT '库存',
  `begin_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `end_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '结束时间',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`voucher_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='秒杀券表';

INSERT INTO `tb_seckill_voucher` (`voucher_id`, `title`, `stock`, `begin_time`, `end_time`, `create_time`, `update_time`) VALUES
(1, '100元秒杀券', 50, '2026-04-25 00:00:00', '2030-12-31 23:59:59', '2026-04-25 00:00:00', '2026-04-25 00:00:00');

CREATE TABLE `tb_voucher_order` (
  `id` bigint(20) NOT NULL COMMENT '订单 ID',
  `user_id` bigint(20) UNSIGNED NOT NULL COMMENT '下单用户 ID',
  `voucher_id` bigint(20) UNSIGNED NOT NULL COMMENT '秒杀券 ID',
  `status` tinyint(4) NOT NULL DEFAULT 1 COMMENT '订单状态：1-处理中 2-已完成 3-已取消 4-失败',
  `finish_time` timestamp NULL DEFAULT NULL COMMENT '订单结束时间',
  `fail_code` varchar(64) DEFAULT NULL COMMENT '失败码',
  `fail_reason` varchar(512) DEFAULT NULL COMMENT '失败原因',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='秒杀订单表';

CREATE TABLE `tb_order_outbox` (
  `id` bigint(20) NOT NULL COMMENT 'Outbox主键，直接复用订单ID',
  `user_id` bigint(20) UNSIGNED NOT NULL COMMENT '用户ID',
  `voucher_id` bigint(20) UNSIGNED NOT NULL COMMENT '秒杀券ID',
  `topic` varchar(128) NOT NULL COMMENT 'RocketMQ目标Topic',
  `payload` text NOT NULL COMMENT '消息体JSON',
  `status` tinyint(4) NOT NULL DEFAULT 1 COMMENT 'Outbox状态：1-待发送 2-发送中 3-已发送 4-重试中 5-死亡',
  `retry_count` int(11) NOT NULL DEFAULT 0 COMMENT '已重试次数',
  `next_retry_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '下次允许重试时间',
  `mq_msg_id` varchar(128) DEFAULT NULL COMMENT 'RocketMQ消息ID',
  `last_error` varchar(512) DEFAULT NULL COMMENT '最后一次错误信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_status_retry` (`status`, `next_retry_time`),
  KEY `idx_status_update` (`status`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单消息Outbox表';

SET FOREIGN_KEY_CHECKS = 1;
