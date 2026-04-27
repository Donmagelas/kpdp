仅上传核心秒杀模块强化内容
  + Caffeine 本地缓存
  + 本地 BloomFilter
  + Redis 滑动窗口限流
  + P95 动态降级
  + RocketMQ 异步消费
  + Outbox 本地消息表
  + Redis ZSet 驱动 Outbox 投递
  + 死信队列处理
  + 超时订单取消
  + ShardingSphere-JDBC 分库分表
  + 雪花 ID
  + canal监听binlog发RocketMQ同步缓存




用户请求
  + 初始限流 + P95动态限流
  + 登录态
  + Caffeine / Bloom / Redis / DB 查券
  + Java 校验时间
  + Redis Lua 预扣库存 + 一人一单
  + 本地事务写 PROCESSING 订单 + Outbox
  + Redis ZSet 触发 Outbox 投递
  + RocketMQ
  + 消费者扣 MySQL 库存
  + CAS 更新订单 COMPLETED

可靠性
  + Outbox 发送失败 → RETRYING / DEAD
  + MQ 消费失败 → RocketMQ 重试
  + MQ 死信 → 标记订单 FAILED + 回滚 Redis 资格
  + 订单长时间 PROCESSING → CANCELLED + 回滚 Redis 资格
