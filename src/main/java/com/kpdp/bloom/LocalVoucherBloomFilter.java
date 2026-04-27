package com.kpdp.bloom;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.kpdp.entity.SeckillVoucher;
import com.kpdp.mapper.SeckillVoucherMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.util.List;
import java.util.Objects;

/**
 * 秒杀券本地布隆过滤器，用于在进入 Redis 前先挡掉明显不存在的券 ID。
 */
@Slf4j
@Component
public class LocalVoucherBloomFilter {

    /**
     * Long 类型漏斗，把秒杀券 ID 稳定写入布隆过滤器。
     */
    private static final Funnel<Long> LONG_FUNNEL = new Funnel<>() {
        @Override
        public void funnel(Long from, PrimitiveSink into) {
            into.putLong(from);
        }
    };

    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;

    /**
     * 预估插入量，用于初始化布隆过滤器。
     */
    private final long expectedInsertions;

    /**
     * 允许的误判率。
     */
    private final double falsePositiveRate;

    /**
     * 当前生效的本地布隆过滤器。
     */
    private volatile BloomFilter<Long> bloomFilter;

    public LocalVoucherBloomFilter(
            @Value("${kpdp.bloom.voucher.expected-insertions:10000}") long expectedInsertions,
            @Value("${kpdp.bloom.voucher.false-positive-rate:0.01}") double falsePositiveRate
    ) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
    }

    /**
     * 启动时全量加载已有秒杀券 ID，初始化本地布隆过滤器。
     */
    @PostConstruct
    public void init() {
        try {
            rebuild();
        } catch (Exception e) {
            // 布隆初始化失败时保守放行，不阻断应用启动，后续查询仍可退化到 Redis/DB。
            bloomFilter = null;
            log.warn("初始化本地布隆过滤器失败，查询链路将暂时跳过 Bloom 判断", e);
        }
    }

    /**
     * 全量重建本地布隆过滤器。
     */
    public synchronized void rebuild() {
        List<Object> voucherIdObjects = seckillVoucherMapper.selectObjs(
                new QueryWrapper<SeckillVoucher>().select("voucher_id")
        );
        int insertions = (int) Math.max(expectedInsertions, voucherIdObjects.size() + 1L);
        BloomFilter<Long> newBloomFilter = BloomFilter.create(LONG_FUNNEL, insertions, falsePositiveRate);
        voucherIdObjects.stream()
                .filter(Objects::nonNull)
                .map(value -> ((Number) value).longValue())
                .forEach(newBloomFilter::put);
        bloomFilter = newBloomFilter;
        log.info(
                "秒杀券本地布隆过滤器初始化完成，voucherCount={}, expectedInsertions={}, falsePositiveRate={}",
                voucherIdObjects.size(),
                insertions,
                falsePositiveRate
        );
    }

    /**
     * 判断某个秒杀券 ID 是否“可能存在”。
     *
     * <p>如果布隆过滤器尚未初始化成功，则保守放行到后续 Redis/DB 链路，避免误伤真实请求。</p>
     *
     * @param voucherId 秒杀券 ID
     * @return true 表示可能存在，false 表示一定不存在
     */
    public boolean mightContain(Long voucherId) {
        if (voucherId == null) {
            return false;
        }
        BloomFilter<Long> currentFilter = bloomFilter;
        if (currentFilter == null) {
            log.warn("本地布隆过滤器尚未准备完成，跳过布隆判断，voucherId={}", voucherId);
            return true;
        }
        return currentFilter.mightContain(voucherId);
    }

    /**
     * 新增秒杀券后，把新券 ID 增量写入本地布隆过滤器。
     *
     * @param voucherId 新增秒杀券 ID
     */
    public synchronized void put(Long voucherId) {
        if (voucherId == null) {
            return;
        }
        if (bloomFilter == null) {
            rebuild();
            return;
        }
        bloomFilter.put(voucherId);
    }
}
