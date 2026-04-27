package com.kpdp.controller;

import com.kpdp.dto.Result;
import com.kpdp.dto.SeckillVoucherRequest;
import com.kpdp.dto.SeckillVoucherUpdateRequest;
import com.kpdp.service.ISeckillVoucherService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;

/**
 * 秒杀券接口。
 */
@RestController
@RequestMapping("/voucher")
public class VoucherController {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /**
     * 创建秒杀券并同步初始化 Redis 库存。
     *
     * @param request 秒杀券请求
     * @return 新建秒杀券 ID
     */
    @PostMapping("/seckill")
    public Result addSeckillVoucher(@RequestBody SeckillVoucherRequest request) {
        Long voucherId = seckillVoucherService.addSeckillVoucher(request);
        return Result.ok(voucherId);
    }

    /**
     * 查询当前可供演示的秒杀券。
     *
     * @return 秒杀券列表
     */
    @GetMapping("/seckill/list")
    public Result querySeckillVouchers() {
        return seckillVoucherService.querySeckillVouchers();
    }

    /**
     * 查询单个秒杀券，优先读取 Redis 缓存。
     *
     * @param voucherId 秒杀券 ID
     * @return 秒杀券详情
     */
    @GetMapping("/seckill/{id}")
    public Result querySeckillVoucher(@PathVariable("id") Long voucherId) {
        return seckillVoucherService.querySeckillVoucher(voucherId);
    }

    /**
     * 更新秒杀券。
     *
     * <p>这里仅负责改库，缓存失效依赖 Canal 订阅 binlog 后再发 RocketMQ 消息。</p>
     *
     * @param voucherId 秒杀券 ID
     * @param request 更新请求
     * @return 更新结果
     */
    @PutMapping("/seckill/{id}")
    public Result updateSeckillVoucher(@PathVariable("id") Long voucherId,
                                       @RequestBody SeckillVoucherUpdateRequest request) {
        request.setVoucherId(voucherId);
        seckillVoucherService.updateSeckillVoucher(request);
        return Result.ok();
    }

    /**
     * 查看秒杀券缓存失效消费者的死信消息。
     *
     * @param limit 最多返回多少条
     * @return 死信消息列表
     */
    @GetMapping("/cache/dlq")
    public Result queryCacheInvalidationDeadLetters(
            @RequestParam(value = "limit", required = false) Integer limit
    ) {
        return seckillVoucherService.queryCacheInvalidationDeadLetters(limit);
    }
}
