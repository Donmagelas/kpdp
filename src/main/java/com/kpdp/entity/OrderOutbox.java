package com.kpdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 订单消息 Outbox。
 *
 * <p>这里专门记录“订单消息是否已经成功投递到 RocketMQ”，
 * 和订单业务状态分离，避免把消息投递语义塞进订单表。</p>
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_order_outbox")
public class OrderOutbox implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待发送。
     */
    public static final Integer STATUS_PENDING = 1;

    /**
     * 发送中。
     */
    public static final Integer STATUS_SENDING = 2;

    /**
     * 已发送。
     *
     * <p>表示 Broker 已确认接收消息，不代表消费者已经处理完成。</p>
     */
    public static final Integer STATUS_SENT = 3;

    /**
     * 重试中。
     */
    public static final Integer STATUS_RETRYING = 4;

    /**
     * 死亡。
     *
     * <p>表示超过最大重试次数，人工排查前不再自动投递。</p>
     */
    public static final Integer STATUS_DEAD = 5;

    /**
     * Outbox 主键。
     *
     * <p>这里直接复用订单 ID，便于订单和消息一一对应。</p>
     */
    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    /**
     * 用户 ID。
     *
     * <p>作为分片键使用，确保和订单表落到同一路由。</p>
     */
    private Long userId;

    /**
     * 秒杀券 ID。
     */
    private Long voucherId;

    /**
     * 目标 Topic。
     */
    private String topic;

    /**
     * 消息体 JSON。
     */
    private String payload;

    /**
     * 投递状态。
     */
    private Integer status;

    /**
     * 已重试次数。
     */
    private Integer retryCount;

    /**
     * 下次允许重试的时间。
     */
    private LocalDateTime nextRetryTime;

    /**
     * RocketMQ 返回的消息 ID。
     */
    private String mqMsgId;

    /**
     * 最后一次错误信息。
     */
    private String lastError;

    /**
     * 创建时间。
     */
    private LocalDateTime createTime;

    /**
     * 更新时间。
     */
    private LocalDateTime updateTime;
}
