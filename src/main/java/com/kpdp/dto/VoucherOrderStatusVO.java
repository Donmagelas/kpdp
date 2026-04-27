package com.kpdp.dto;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 秒杀订单轮询状态视图。
 */
@Data
public class VoucherOrderStatusVO {

    /**
     * 订单 ID。
     */
    private Long orderId;

    /**
     * 秒杀券 ID。
     */
    private Long voucherId;

    /**
     * 订单状态。
     */
    private Integer orderStatus;

    /**
     * 订单状态描述。
     */
    private String orderStatusDesc;

    /**
     * 订单结束时间。
     */
    private LocalDateTime finishTime;

    /**
     * 失败码。
     */
    private String failCode;

    /**
     * 失败原因。
     */
    private String failReason;

    /**
     * Outbox 状态。
     */
    private Integer outboxStatus;

    /**
     * Outbox 状态描述。
     */
    private String outboxStatusDesc;
}
