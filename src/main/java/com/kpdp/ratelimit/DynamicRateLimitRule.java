package com.kpdp.ratelimit;

/**
 * 动态限流规则快照。
 *
 * @param windowSeconds 滑动窗口秒数
 * @param limit 窗口内允许的最大请求数
 */
public record DynamicRateLimitRule(long windowSeconds, long limit) {
}
