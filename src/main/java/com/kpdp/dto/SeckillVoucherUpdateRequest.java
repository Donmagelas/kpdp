package com.kpdp.dto;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 更新秒杀券请求。
 */
@Data
public class SeckillVoucherUpdateRequest {

    /**
     * 秒杀券 ID。
     */
    private Long voucherId;

    /**
     * 券标题。
     */
    private String title;

    /**
     * 库存。
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
