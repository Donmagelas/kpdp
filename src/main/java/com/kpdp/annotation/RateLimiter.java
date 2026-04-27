package com.kpdp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 接口限流注解，支持全局和用户两个维度。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(RateLimiters.class)
public @interface RateLimiter {

    /**
     * 限流 key 前缀，同一个业务接口应保持稳定。
     *
     * @return 业务 key 前缀
     */
    String key();

    /**
     * 统计窗口大小，单位秒。
     *
     * @return 窗口秒数
     */
    long window();

    /**
     * 窗口内允许的最大请求数。
     *
     * @return 最大请求数
     */
    long limit();

    /**
     * 限流触发时返回给调用方的提示信息。
     *
     * @return 限流提示语
     */
    String message() default "请求过于频繁，请稍后再试";

    /**
     * 限流维度。
     *
     * @return 限流维度
     */
    LimitType type();

    /**
     * 限流维度枚举。
     */
    enum LimitType {

        /**
         * 全局维度，整条接口共用一套计数。
         */
        GLOBAL,

        /**
         * 用户维度，按登录用户单独计数。
         */
        USER
    }
}
