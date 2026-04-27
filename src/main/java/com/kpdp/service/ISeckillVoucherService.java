package com.kpdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.kpdp.dto.Result;
import com.kpdp.dto.SeckillVoucherRequest;
import com.kpdp.dto.SeckillVoucherUpdateRequest;
import com.kpdp.entity.SeckillVoucher;

/**
 * 秒杀券扩展信息服务。
 */
public interface ISeckillVoucherService extends IService<SeckillVoucher> {

    /**
     * 创建秒杀券。
     *
     * @param request 秒杀券请求
     * @return 秒杀券 ID
     */
    Long addSeckillVoucher(SeckillVoucherRequest request);

    /**
     * 查询秒杀券列表。
     *
     * @return 查询结果
     */
    Result querySeckillVouchers();

    /**
     * 查询单个秒杀券。
     *
     * @param voucherId 秒杀券 ID
     * @return 查询结果
     */
    Result querySeckillVoucher(Long voucherId);

    /**
     * 按统一缓存链路查询单张秒杀券详情。
     *
     * @param voucherId 秒杀券 ID
     * @return 秒杀券详情，不存在时返回 null
     */
    SeckillVoucher getSeckillVoucherWithCache(Long voucherId);

    /**
     * 仅走 Redis 和 MySQL 查询单张秒杀券，供压测对比本地缓存效果时使用。
     *
     * @param voucherId 秒杀券 ID
     * @return 秒杀券详情，不存在时返回 null
     */
    SeckillVoucher getSeckillVoucherByRedisAndDbOnly(Long voucherId);

    /**
     * 更新秒杀券。
     *
     * @param request 更新请求
     */
    void updateSeckillVoucher(SeckillVoucherUpdateRequest request);

    /**
     * 查看秒杀券缓存失效消费者的死信消息。
     *
     * @param limit 最多返回多少条
     * @return 死信消息
     */
    Result queryCacheInvalidationDeadLetters(Integer limit);

    /**
     * 按消息内容删除相关缓存，并同步 Redis 库存。
     *
     * @param voucherId 秒杀券 ID
     * @param deleteOnly 是否仅删除缓存不回写库存
     */
    void invalidateVoucherCache(Long voucherId, boolean deleteOnly);
}
