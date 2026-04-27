package com.kpdp.dto;

import lombok.Data;

/**
 * 秒杀订单死信消息视图。
 */
@Data
public class SeckillOrderDeadLetterVO {

    /**
     * 死信主题名称。
     */
    private String topic;

    /**
     * RocketMQ 消息 ID。
     */
    private String msgId;

    /**
     * 消息所在 Broker。
     */
    private String brokerName;

    /**
     * 队列编号。
     */
    private Integer queueId;

    /**
     * 队列内偏移量。
     */
    private Long queueOffset;

    /**
     * 已重试次数。
     */
    private Integer reconsumeTimes;

    /**
     * 业务 keys。
     */
    private String keys;

    /**
     * 消息 tags。
     */
    private String tags;

    /**
     * 出生时间戳。
     */
    private Long bornTimestamp;

    /**
     * 存储时间戳。
     */
    private Long storeTimestamp;

    /**
     * 原始消息体，便于直接查看。
     */
    private String rawBody;

    /**
     * 解析后的订单消息体。
     */
    private SeckillOrderMessage orderMessage;

    /**
     * 通用 JSON 解析结果，便于查看非订单类死信消息。
     */
    private Object jsonBody;
}
