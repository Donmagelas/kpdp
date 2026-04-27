package com.kpdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.kpdp.dto.Result;
import com.kpdp.entity.VoucherOrder;

/**
 * 秒杀订单服务。
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    /**
     * 发起秒杀。
     *
     * @param voucherId 秒杀券 ID
     * @return 下单结果
     */
    Result seckillVoucher(Long voucherId);

    /**
     * 查询当前登录用户的订单状态。
     *
     * @param orderId 订单 ID
     * @return 订单状态
     */
    Result queryOrderStatus(Long orderId);

    /**
     * 查看秒杀订单消费者对应的死信消息。
     *
     * @param limit 最多返回多少条
     * @return 死信消息列表
     */
    Result queryDeadLetters(Integer limit);
}
