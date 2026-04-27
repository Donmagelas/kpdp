package com.kpdp.utils;

/**
 * Redis Key 常量。
 */
public final class RedisConstants {

    private RedisConstants() {
    }

    /**
     * 登录 token 前缀。
     */
    public static final String LOGIN_USER_KEY = "login:token:";

    /**
     * 登录 token 过期时间，单位分钟。
     */
    public static final Long LOGIN_USER_TTL = 36000L;

    /**
     * 秒杀库存前缀。
     */
    public static final String SECKILL_STOCK_KEY = "seckill:stock:";

    /**
     * 单个秒杀券缓存 key 前缀。
     */
    public static final String SECKILL_VOUCHER_KEY = "cache:seckill:voucher:";

    /**
     * 单个秒杀券缓存过期时间，单位分钟。
     */
    public static final Long SECKILL_VOUCHER_TTL = 10L;

    /**
     * 空值缓存过期时间，单位分钟。
     */
    public static final Long CACHE_NULL_TTL = 2L;

    /**
     * Outbox 待发送/待重试就绪索引。
     *
     * <p>高频任务只扫这个 ZSet，避免每 50ms 广播扫分片表。</p>
     */

    /**
     * Outbox 发送中租约索引。
     *
     * <p>成员分数表示“发送中”租约到期时间，用于超时恢复。</p>
     */
    /**
     * Outbox 发送调度索引。
     *
     * <p>这个 ZSet 里只允许放两类记录：</p>
     * <p>1. 状态为“发送中”的租约记录，用于发送超时恢复。</p>
     * <p>2. 状态为“重试中”的调度记录，用于失败后的再次发送。</p>
     */
    public static final String OUTBOX_SENDING_KEY = "outbox:sending";
}
