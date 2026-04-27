package com.kpdp.config;

import com.kpdp.utils.LoginInterceptor;
import com.kpdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import jakarta.annotation.Resource;

/**
 * MVC 配置，仅保留秒杀链路所需的鉴权拦截器。
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 先刷新 token 并把用户放入 ThreadLocal。
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
                .addPathPatterns("/**")
                .order(0);

        // 再拦截需要登录才能访问的接口。
        registry.addInterceptor(new LoginInterceptor())
                .excludePathPatterns(
                        "/user/login",
                        "/voucher/seckill",
                        "/voucher/seckill/list",
                        "/voucher/seckill/**"
                )
                .order(1);
    }
}
