package com.kpdp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 限流注解容器，用于支持在同一个方法上叠加多条限流规则。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimiters {

    /**
     * 多条限流规则。
     *
     * @return 限流规则数组
     */
    RateLimiter[] value();
}
