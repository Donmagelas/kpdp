package com.kpdp.dto;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 创建秒杀券请求。
 */
@Data
public class SeckillVoucherRequest {

    /**
     * 券标题。
     */
    private String title;

    /**
     * 初始库存。
     */
    private Integer stock;

    /**
     * 秒杀开始时间。
     */
    private LocalDateTime beginTime;

    /**
     * 秒杀结束时间。
     */
    private LocalDateTime endTime;
}
