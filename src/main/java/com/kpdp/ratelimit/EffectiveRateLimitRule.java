package com.kpdp.ratelimit;

/**
 * 限流切面实际使用的生效规则。
 *
 * @param windowSeconds 最终生效的滑动窗口秒数
 * @param limit 最终生效的最大请求数
 * @param dynamic 是否来自动态覆盖配置
 */
public record EffectiveRateLimitRule(long windowSeconds, long limit, boolean dynamic) {
}
