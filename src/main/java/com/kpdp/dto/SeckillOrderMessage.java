package com.kpdp.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * 秒杀订单异步消息体。
 */
@Data
public class SeckillOrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 订单 ID。
     */
    private Long orderId;

    /**
     * 下单用户 ID。
     */
    private Long userId;

    /**
     * 秒杀券 ID。
     */
    private Long voucherId;
}
