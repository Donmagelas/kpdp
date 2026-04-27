package com.kpdp.aspect;

import com.kpdp.annotation.RateLimiter;
import com.kpdp.dto.Result;
import com.kpdp.dto.UserDTO;
import com.kpdp.ratelimit.EffectiveRateLimitRule;
import com.kpdp.ratelimit.RateLimitConfigService;
import com.kpdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的接口限流切面。
 */
@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    /**
     * Redis 限流脚本，负责滑动窗口统计和原子限流判断。
     */
    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT;

    /**
     * Redis 限流 key 前缀。
     */
    private static final String RATE_LIMIT_KEY_PREFIX = "rate:limit:";

    static {
        RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("rate-limit.lua"));
        RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RateLimitConfigService rateLimitConfigService;

    /**
     * 拦截带有限流注解的方法，并按声明顺序逐条执行限流规则。
     *
     * @param joinPoint 切点
     * @return 原方法返回结果
     * @throws Throwable 原方法抛出的异常
     */
    @Around("@annotation(com.kpdp.annotation.RateLimiter) || @annotation(com.kpdp.annotation.RateLimiters)")
    public Object doRateLimit(ProceedingJoinPoint joinPoint) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        RateLimiter[] rateLimiters = method.getAnnotationsByType(RateLimiter.class);
        for (RateLimiter rateLimiter : rateLimiters) {
            Object blockedResult = tryAcquire(rateLimiter, method);
            if (blockedResult != null) {
                return blockedResult;
            }
        }
        return joinPoint.proceed();
    }

    /**
     * 尝试获取单条限流规则的访问资格。
     *
     * @param rateLimiter 限流注解
     * @param method 当前方法
     * @return 被限流时返回失败结果，通过时返回 null
     */
    private Object tryAcquire(RateLimiter rateLimiter, Method method) {
        String identifier = resolveIdentifier(rateLimiter.type());
        if (identifier == null) {
            return Result.fail("请先登录");
        }

        String businessKey = rateLimitConfigService.resolveBusinessKey(rateLimiter, method);
        EffectiveRateLimitRule effectiveRule = rateLimitConfigService.resolveEffectiveRule(rateLimiter, method);
        String limitKey = buildLimitKey(businessKey, rateLimiter.type(), identifier);
        long windowMillis = TimeUnit.SECONDS.toMillis(effectiveRule.windowSeconds());
        long nowMillis = System.currentTimeMillis();
        Long allowed = stringRedisTemplate.execute(
                RATE_LIMIT_SCRIPT,
                Collections.singletonList(limitKey),
                String.valueOf(windowMillis),
                String.valueOf(effectiveRule.limit()),
                String.valueOf(nowMillis),
                UUID.randomUUID().toString()
        );
        if (allowed == null) {
            log.error("执行限流脚本失败，key={}", limitKey);
            return Result.fail("系统繁忙，请稍后再试");
        }
        if (allowed == 0L) {
            log.warn(
                    "触发接口限流，key={}, windowSeconds={}, limit={}",
                    limitKey,
                    effectiveRule.windowSeconds(),
                    effectiveRule.limit()
            );
            return Result.fail(rateLimiter.message());
        }
        return null;
    }

    /**
     * 解析限流维度对应的标识。
     *
     * @param type 限流维度
     * @return 限流标识，无法解析时返回 null
     */
    private String resolveIdentifier(RateLimiter.LimitType type) {
        if (type == RateLimiter.LimitType.GLOBAL) {
            return "global";
        }
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            return null;
        }
        return String.valueOf(user.getId());
    }

    /**
     * 构造 Redis 限流 key。
     *
     * @param rateLimiter 限流规则
     * @param method 当前方法
     * @param identifier 维度标识
     * @return Redis key
     */
    private String buildLimitKey(String businessKey, RateLimiter.LimitType type, String identifier) {
        return RATE_LIMIT_KEY_PREFIX
                + businessKey
                + ":"
                + type.name().toLowerCase()
                + ":"
                + identifier;
    }
}
