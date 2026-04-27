package com.kpdp.ratelimit;

/**
 * 限流与降级相关的业务常量。
 */
public final class RateLimitConstants {

    /**
     * 秒杀接口的全局限流业务 key。
     */
    public static final String SECKILL_GLOBAL_KEY = "coupon:seckill:global";

    /**
     * 秒杀接口的用户维度限流业务 key。
     */
    public static final String SECKILL_USER_KEY = "coupon:seckill:user";

    /**
     * 秒杀接口在 Micrometer 指标中的 URI 模板。
     */
    public static final String SECKILL_URI_PATTERN = "/voucher-order/seckill/{id}";

    private RateLimitConstants() {
    }
}
