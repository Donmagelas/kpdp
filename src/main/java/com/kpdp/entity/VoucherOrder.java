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
 * 秒杀订单。
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_voucher_order")
public class VoucherOrder implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 订单处理中。
     *
     * <p>表示订单已经通过资格校验，并且已经成功发送到消息队列，
     * 但消费者还没有完成最终处理。</p>
     */
    public static final Integer STATUS_PROCESSING = 1;

    /**
     * 订单已完成。
     *
     * <p>表示消息队列消费者已经完成订单处理并成功落库。</p>
     */
    public static final Integer STATUS_COMPLETED = 2;

    /**
     * 订单已取消。
     *
     * <p>预留给超时关闭或人工关闭场景使用。</p>
     */
    public static final Integer STATUS_CANCELLED = 3;

    /**
     * 订单失败。
     *
     * <p>表示消息最终处理失败，通常需要人工排查。</p>
     */
    public static final Integer STATUS_FAILED = 4;

    /**
     * 订单 ID。
     */
    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    /**
     * 下单用户 ID。
     */
    private Long userId;

    /**
     * 秒杀券 ID。
     */
    private Long voucherId;

    /**
     * 订单状态。
     */
    private Integer status;

    /**
     * 订单结束时间。
     *
     * <p>订单成功、取消或失败时都会写入该时间。</p>
     */
    private LocalDateTime finishTime;

    /**
     * 失败码。
     *
     * <p>仅在失败或特殊取消场景下使用，便于检索问题类型。</p>
     */
    private String failCode;

    /**
     * 失败原因。
     *
     * <p>仅保存适合展示和排查的短原因，不保存完整异常栈。</p>
     */
    private String failReason;

    /**
     * 创建时间。
     */
    private LocalDateTime createTime;
}
