package com.kpdp.ratelimit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kpdp.annotation.RateLimiter;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * 动态限流配置服务。
 *
 * <p>这里把 Redis 中的动态限流配置做一层很轻的本地缓存，避免每个请求都额外访问 Redis。</p>
 */
@Component
public class RateLimitConfigService {

    /**
     * Redis 动态限流配置 key 前缀。
     */
    private static final String RATE_LIMIT_CONFIG_KEY_PREFIX = "rate:config:";

    /**
     * 本地缓存 5 秒，兼顾多实例同步时效和请求侧开销。
     */
    private final Cache<String, Optional<DynamicRateLimitRule>> dynamicRuleCache = Caffeine.newBuilder()
            .maximumSize(128)
            .expireAfterWrite(Duration.ofSeconds(5))
            .build();

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 解析限流切面最终应使用的规则。
     *
     * @param rateLimiter 注解默认规则
     * @param method 当前方法
     * @return 生效规则；如果不存在动态配置，则退回注解默认值
     */
    public EffectiveRateLimitRule resolveEffectiveRule(RateLimiter rateLimiter, Method method) {
        String businessKey = resolveBusinessKey(rateLimiter, method);
        Optional<DynamicRateLimitRule> dynamicRule = getDynamicRuleOptional(businessKey, rateLimiter.type());
        if (dynamicRule.isPresent()) {
            DynamicRateLimitRule rule = dynamicRule.get();
            return new EffectiveRateLimitRule(rule.windowSeconds(), rule.limit(), true);
        }
        return new EffectiveRateLimitRule(rateLimiter.window(), rateLimiter.limit(), false);
    }

    /**
     * 保存动态限流规则，并同步刷新当前实例本地缓存。
     *
     * @param businessKey 业务 key
     * @param type 限流维度
     * @param windowSeconds 滑动窗口秒数
     * @param limit 最大请求数
     */
    public void saveDynamicRule(String businessKey, RateLimiter.LimitType type, long windowSeconds, long limit) {
        String configKey = buildDynamicConfigKey(businessKey, type);
        stringRedisTemplate.opsForHash().put(configKey, "window", String.valueOf(windowSeconds));
        stringRedisTemplate.opsForHash().put(configKey, "limit", String.valueOf(limit));
        stringRedisTemplate.opsForHash().put(configKey, "updatedAt", String.valueOf(System.currentTimeMillis()));
        dynamicRuleCache.put(configKey, Optional.of(new DynamicRateLimitRule(windowSeconds, limit)));
    }

    /**
     * 删除动态限流规则，让切面回退到注解默认值。
     *
     * @param businessKey 业务 key
     * @param type 限流维度
     */
    public void clearDynamicRule(String businessKey, RateLimiter.LimitType type) {
        String configKey = buildDynamicConfigKey(businessKey, type);
        stringRedisTemplate.delete(configKey);
        dynamicRuleCache.invalidate(configKey);
    }

    /**
     * 读取当前动态限流规则。
     *
     * @param businessKey 业务 key
     * @param type 限流维度
     * @return 动态规则；不存在时返回空
     */
    public Optional<DynamicRateLimitRule> getDynamicRuleOptional(String businessKey, RateLimiter.LimitType type) {
        String configKey = buildDynamicConfigKey(businessKey, type);
        Optional<DynamicRateLimitRule> cached = dynamicRuleCache.getIfPresent(configKey);
        if (cached != null) {
            return cached;
        }
        Optional<DynamicRateLimitRule> loaded = loadDynamicRuleFromRedis(configKey);
        dynamicRuleCache.put(configKey, loaded);
        return loaded;
    }

    /**
     * 解析业务 key；若注解未显式配置，则退回到“类名:方法名”。
     *
     * @param rateLimiter 注解规则
     * @param method 当前方法
     * @return 业务 key
     */
    public String resolveBusinessKey(RateLimiter rateLimiter, Method method) {
        String key = rateLimiter.key();
        if (key == null || key.isBlank()) {
            return method.getDeclaringClass().getSimpleName() + ":" + method.getName();
        }
        return key;
    }

    /**
     * 拼接 Redis 动态限流配置 key。
     *
     * @param businessKey 业务 key
     * @param type 限流维度
     * @return Redis key
     */
    public String buildDynamicConfigKey(String businessKey, RateLimiter.LimitType type) {
        return RATE_LIMIT_CONFIG_KEY_PREFIX + businessKey + ":" + type.name().toLowerCase();
    }

    /**
     * 从 Redis 加载动态限流规则。
     *
     * @param configKey Redis 配置 key
     * @return 动态规则；不存在时返回空
     */
    private Optional<DynamicRateLimitRule> loadDynamicRuleFromRedis(String configKey) {
        Map<Object, Object> configMap = stringRedisTemplate.opsForHash().entries(configKey);
        if (configMap == null || configMap.isEmpty()) {
            return Optional.empty();
        }
        Object windowValue = configMap.get("window");
        Object limitValue = configMap.get("limit");
        if (windowValue == null || limitValue == null) {
            return Optional.empty();
        }
        long windowSeconds = Long.parseLong(windowValue.toString());
        long limit = Long.parseLong(limitValue.toString());
        return Optional.of(new DynamicRateLimitRule(windowSeconds, limit));
    }
}
