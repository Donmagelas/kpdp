package com.kpdp.config;

import com.kpdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一异常处理，避免秒杀接口直接抛出堆栈到前端。
 */
@Slf4j
@RestControllerAdvice
public class WebExceptionAdvice {

    @ExceptionHandler(RuntimeException.class)
    public Result handleRuntimeException(RuntimeException e) {
        log.error("服务异常", e);
        return Result.fail(e.getMessage() == null ? "服务器异常" : e.getMessage());
    }
}
