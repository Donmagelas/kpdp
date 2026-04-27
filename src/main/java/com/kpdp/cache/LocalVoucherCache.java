package com.kpdp.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kpdp.entity.SeckillVoucher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 秒杀券本地缓存，优先承接热点详情查询。
 */
@Component
public class LocalVoucherCache {

    /**
     * Caffeine 本地缓存实例。
     */
    private final Cache<Long, SeckillVoucher> voucherCache;

    public LocalVoucherCache(
            @Value("${kpdp.cache.local.voucher.max-size:10000}") long maxSize,
            @Value("${kpdp.cache.local.voucher.ttl-minutes:5}") long ttlMinutes
    ) {
        this.voucherCache = Caffeine.newBuilder()
                // 控制本地缓存体量，避免热点积压挤占过多堆内存。
                .maximumSize(maxSize)
                // 本地缓存只兜热点短期访问，TTL 不宜设置过长。
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .build();
    }

    /**
     * 按秒杀券 ID 读取本地缓存。
     *
     * @param voucherId 秒杀券 ID
     * @return 命中的秒杀券详情，未命中时返回 null
     */
    public SeckillVoucher get(Long voucherId) {
        return voucherCache.getIfPresent(voucherId);
    }

    /**
     * 写入本地缓存。
     *
     * @param voucher 秒杀券详情
     */
    public void put(SeckillVoucher voucher) {
        if (voucher == null || voucher.getVoucherId() == null) {
            return;
        }
        voucherCache.put(voucher.getVoucherId(), voucher);
    }

    /**
     * 删除指定秒杀券的本地缓存。
     *
     * @param voucherId 秒杀券 ID
     */
    public void evict(Long voucherId) {
        if (voucherId == null) {
            return;
        }
        voucherCache.invalidate(voucherId);
    }
}
