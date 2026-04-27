package com.kpdp.controller;

import com.kpdp.annotation.RateLimiter;
import com.kpdp.dto.Result;
import com.kpdp.ratelimit.RateLimitConstants;
import com.kpdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;

import jakarta.annotation.Resource;

/**
 * 秒杀下单接口。
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    /**
     * 发起秒杀下单。
     *
     * @param voucherId 秒杀券 ID
     * @return 订单结果
     */
    @RateLimiter(
            key = RateLimitConstants.SECKILL_GLOBAL_KEY,
            window = 1,
            limit = 200,
            message = "当前抢购人数过多，请稍后再试",
            type = RateLimiter.LimitType.GLOBAL
    )
    @RateLimiter(
            key = RateLimitConstants.SECKILL_USER_KEY,
            window = 5,
            limit = 3,
            message = "请求过于频繁，请稍后再试",
            type = RateLimiter.LimitType.USER
    )
    @PostMapping("/seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 轮询查询当前登录用户的订单状态。
     *
     * <p>这里用数字路径约束，避免和固定路径 `/voucher-order/dlq` 产生路由冲突。</p>
     *
     * @param orderId 订单 ID
     * @return 订单状态
     */
    @GetMapping("/{orderId:\\d+}")
    public Result queryOrderStatus(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.queryOrderStatus(orderId);
    }

    /**
     * 查看秒杀订单消费者的死信消息。
     *
     * @param limit 最多返回多少条
     * @return 死信消息列表
     */
    @GetMapping("/dlq")
    public Result queryDeadLetters(@RequestParam(value = "limit", required = false) Integer limit) {
        return voucherOrderService.queryDeadLetters(limit);
    }
}
