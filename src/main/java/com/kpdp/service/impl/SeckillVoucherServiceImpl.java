package com.kpdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpdp.bloom.LocalVoucherBloomFilter;
import com.kpdp.cache.LocalVoucherCache;
import com.kpdp.dto.Result;
import com.kpdp.dto.SeckillVoucherRequest;
import com.kpdp.dto.SeckillVoucherUpdateRequest;
import com.kpdp.entity.SeckillVoucher;
import com.kpdp.entity.VoucherOrder;
import com.kpdp.mapper.SeckillVoucherMapper;
import com.kpdp.mapper.VoucherOrderMapper;
import com.kpdp.mq.SeckillOrderDeadLetterInspector;
import com.kpdp.service.ISeckillVoucherService;
import com.kpdp.utils.SnowflakeIdWorker;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.kpdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.kpdp.utils.RedisConstants.SECKILL_STOCK_KEY;
import static com.kpdp.utils.RedisConstants.SECKILL_VOUCHER_KEY;
import static com.kpdp.utils.RedisConstants.SECKILL_VOUCHER_TTL;

/**
 * \u79d2\u6740\u5238\u670d\u52a1\u5b9e\u73b0\u3002
 */
@Slf4j
@Service
public class SeckillVoucherServiceImpl extends ServiceImpl<SeckillVoucherMapper, SeckillVoucher>
        implements ISeckillVoucherService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private LocalVoucherCache localVoucherCache;

    @Resource
    private LocalVoucherBloomFilter localVoucherBloomFilter;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private SeckillOrderDeadLetterInspector seckillOrderDeadLetterInspector;

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Value("${kpdp.rocketmq.voucher-cache-invalidate-consumer-group}")
    private String voucherCacheInvalidateConsumerGroup;

    @Value("${kpdp.stock-check.initial-delay-ms:600000}")
    private long stockCheckInitialDelayMs;

    @Value("${kpdp.stock-check.scan-delay-ms:600000}")
    private long stockCheckScanDelayMs;

    @Value("${kpdp.cache.local.voucher.enabled:true}")
    private boolean localVoucherCacheEnabled;

    @Value("${kpdp.bloom.voucher.enabled:true}")
    private boolean localVoucherBloomEnabled;

    /**
     * \u542f\u52a8\u540e\u9884\u70ed\u5df2\u6709\u79d2\u6740\u5e93\u5b58\u5230 Redis\u3002
     */
    @PostConstruct
    public void warmUpExistingVoucherStock() {
        try {
            List<SeckillVoucher> voucherList = list();
            for (SeckillVoucher voucher : voucherList) {
                writeStockToRedis(voucher);
            }
            log.info("Voucher stock warm-up completed, voucherCount={}", voucherList.size());
        } catch (Exception e) {
            log.warn("Voucher stock warm-up failed, fallback to scheduled reconciliation", e);
        }
    }

    /**
     * \u5b9a\u65f6\u5bf9\u8d26 MySQL \u548c Redis \u4e2d\u7684\u8fd0\u884c\u6001\u5e93\u5b58\u3002
     */
    @Scheduled(
            initialDelayString = "${kpdp.stock-check.initial-delay-ms:600000}",
            fixedDelayString = "${kpdp.stock-check.scan-delay-ms:600000}"
    )
    public void reconcileVoucherStockCache() {
        try {
            List<SeckillVoucher> voucherList = list();
            if (voucherList == null || voucherList.isEmpty()) {
                return;
            }

            LocalDateTime now = LocalDateTime.now();
            int repairedCount = 0;
            int skippedCount = 0;
            for (SeckillVoucher voucher : voucherList) {
                if (!canReconcileVoucherStock(voucher, now)) {
                    skippedCount++;
                    continue;
                }
                if (repairVoucherStockIfNecessary(voucher)) {
                    repairedCount++;
                }
            }
            if (repairedCount > 0) {
                log.warn("Stock reconciliation repaired Redis from MySQL, repairedCount={}, skippedCount={}, intervalMs={}",
                        repairedCount, skippedCount, stockCheckScanDelayMs);
            } else {
                log.debug("Stock reconciliation completed without changes, voucherCount={}, skippedCount={}, intervalMs={}",
                        voucherList.size(), skippedCount, stockCheckScanDelayMs);
            }
        } catch (Exception e) {
            log.error("Stock reconciliation failed, initialDelayMs={}, intervalMs={}",
                    stockCheckInitialDelayMs, stockCheckScanDelayMs, e);
        }
    }

    @Override
    @Transactional
    public Long addSeckillVoucher(SeckillVoucherRequest request) {
        validateRequest(request);

        SeckillVoucher voucher = new SeckillVoucher();
        // \u5e7f\u64ad\u8868\u4e0d\u80fd\u4f9d\u8d56\u6570\u636e\u5e93\u81ea\u589e\u4e3b\u952e\uff0c\u7edf\u4e00\u7531\u5e94\u7528\u4fa7\u96ea\u82b1\u7b97\u6cd5\u751f\u6210 voucherId\u3002
        voucher.setVoucherId(snowflakeIdWorker.nextId());
        voucher.setTitle(request.getTitle());
        voucher.setStock(request.getStock());
        voucher.setBeginTime(request.getBeginTime());
        voucher.setEndTime(request.getEndTime());
        save(voucher);

        // \u65b0\u589e\u540e\u540c\u6b65\u9884\u70ed\u8be6\u60c5\u548c\u8fd0\u884c\u6001\u5e93\u5b58\u3002
        writeStockToRedis(voucher);
        writeVoucherToCache(voucher);
        if (localVoucherCacheEnabled) {
            localVoucherCache.put(voucher);
        }
        if (localVoucherBloomEnabled) {
            localVoucherBloomFilter.put(voucher.getVoucherId());
        }
        return voucher.getVoucherId();
    }

    @Override
    public Result querySeckillVouchers() {
        List<SeckillVoucher> voucherList = lambdaQuery()
                .orderByDesc(SeckillVoucher::getVoucherId)
                .list();
        return Result.ok(voucherList);
    }

    @Override
    public Result querySeckillVoucher(Long voucherId) {
        if (voucherId == null) {
            return Result.fail("\u79d2\u6740\u5238ID\u4e0d\u80fd\u4e3a\u7a7a");
        }
        try {
            SeckillVoucher voucher = getSeckillVoucherWithCache(voucherId);
            if (voucher == null) {
                return Result.fail("\u79d2\u6740\u5238\u4e0d\u5b58\u5728");
            }
            return Result.ok(voucher);
        } catch (IllegalStateException e) {
            log.error("Failed to query voucher through cache chain, voucherId={}", voucherId, e);
            return Result.fail("\u7cfb\u7edf\u7e41\u5fd9\uff0c\u8bf7\u7a0d\u540e\u518d\u8bd5");
        }
    }

    @Override
    public SeckillVoucher getSeckillVoucherWithCache(Long voucherId) {
        if (voucherId == null) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238ID\u4e0d\u80fd\u4e3a\u7a7a");
        }

        if (localVoucherCacheEnabled) {
            SeckillVoucher localVoucher = readVoucherFromLocalCache(voucherId);
            if (localVoucher != null) {
                return localVoucher;
            }
        }

        if (localVoucherBloomEnabled && !mightContainVoucherSafely(voucherId)) {
            return null;
        }

        String cacheKey = SECKILL_VOUCHER_KEY + voucherId;
        String cacheJson = readVoucherCacheFromRedis(cacheKey);
        if (cacheJson != null) {
            if (cacheJson.isBlank()) {
                return null;
            }
            SeckillVoucher voucher = readVoucherFromCache(cacheJson);
            if (voucher != null) {
                if (localVoucherCacheEnabled) {
                    cacheVoucherToLocal(voucher);
                }
                return voucher;
            }
        }

        SeckillVoucher voucher;
        try {
            voucher = getById(voucherId);
        } catch (Exception e) {
            throw new IllegalStateException("Query voucher from database failed, voucherId=" + voucherId, e);
        }
        if (voucher == null) {
            cacheNullVoucherSafely(cacheKey, voucherId);
            return null;
        }

        writeVoucherToCache(voucher);
        if (localVoucherCacheEnabled) {
            cacheVoucherToLocal(voucher);
        }
        return voucher;
    }

    @Override
    public SeckillVoucher getSeckillVoucherByRedisAndDbOnly(Long voucherId) {
        if (voucherId == null) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238ID\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (localVoucherCacheEnabled || localVoucherBloomEnabled) {
            return getSeckillVoucherWithCache(voucherId);
        }

        String cacheKey = SECKILL_VOUCHER_KEY + voucherId;
        String cacheJson = readVoucherCacheFromRedis(cacheKey);
        if (cacheJson != null) {
            if (cacheJson.isBlank()) {
                return null;
            }
            SeckillVoucher voucher = readVoucherFromCache(cacheJson);
            if (voucher != null) {
                return voucher;
            }
        }

        SeckillVoucher voucher;
        try {
            voucher = getById(voucherId);
        } catch (Exception e) {
            throw new IllegalStateException("Query voucher from database failed, voucherId=" + voucherId, e);
        }
        if (voucher == null) {
            cacheNullVoucherSafely(cacheKey, voucherId);
            return null;
        }

        writeVoucherToCache(voucher);
        return voucher;
    }

    @Override
    @Transactional
    public void updateSeckillVoucher(SeckillVoucherUpdateRequest request) {
        validateUpdateRequest(request);

        SeckillVoucher voucher = getById(request.getVoucherId());
        if (voucher == null) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238\u4e0d\u5b58\u5728");
        }

        // \u79d2\u6740\u5df2\u5f00\u59cb\u540e\uff0c\u4e0d\u518d\u5141\u8bb8\u901a\u8fc7\u6539\u5238\u63a5\u53e3\u76f4\u63a5\u4fee\u6539\u5e93\u5b58\u3002
        boolean seckillStarted = voucher.getBeginTime() != null && !LocalDateTime.now().isBefore(voucher.getBeginTime());
        boolean stockChanged = request.getStock() != null && !request.getStock().equals(voucher.getStock());
        if (seckillStarted && stockChanged) {
            throw new IllegalArgumentException("\u79d2\u6740\u5df2\u5f00\u59cb\uff0c\u4e0d\u80fd\u518d\u901a\u8fc7\u5238\u4fe1\u606f\u63a5\u53e3\u4fee\u6539\u5e93\u5b58");
        }

        voucher.setTitle(request.getTitle());
        voucher.setStock(request.getStock());
        voucher.setBeginTime(request.getBeginTime());
        voucher.setEndTime(request.getEndTime());
        updateById(voucher);
        log.info("Voucher updated, voucherId={}", request.getVoucherId());
    }

    @Override
    public Result queryCacheInvalidationDeadLetters(Integer limit) {
        return Result.ok(seckillOrderDeadLetterInspector.queryDeadLetters(voucherCacheInvalidateConsumerGroup, limit));
    }

    @Override
    public void invalidateVoucherCache(Long voucherId, boolean deleteOnly) {
        if (voucherId == null) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238ID\u4e0d\u80fd\u4e3a\u7a7a");
        }
        try {
            localVoucherCache.evict(voucherId);
            stringRedisTemplate.delete(SECKILL_VOUCHER_KEY + voucherId);
        } catch (Exception e) {
            throw new IllegalStateException("Delete voucher cache failed, voucherId=" + voucherId, e);
        }
        if (deleteOnly) {
            return;
        }
    }

    /**
     * \u6821\u9a8c\u65b0\u589e\u79d2\u6740\u5238\u8bf7\u6c42\u3002
     */
    private void validateRequest(SeckillVoucherRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("\u8bf7\u6c42\u4f53\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (request.getTitle() == null || request.getTitle().trim().isEmpty()) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238\u6807\u9898\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (request.getStock() == null || request.getStock() <= 0) {
            throw new IllegalArgumentException("\u5e93\u5b58\u5fc5\u987b\u5927\u4e8e 0");
        }
        LocalDateTime beginTime = request.getBeginTime();
        LocalDateTime endTime = request.getEndTime();
        if (beginTime == null || endTime == null) {
            throw new IllegalArgumentException("\u5f00\u59cb\u65f6\u95f4\u548c\u7ed3\u675f\u65f6\u95f4\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (!endTime.isAfter(beginTime)) {
            throw new IllegalArgumentException("\u7ed3\u675f\u65f6\u95f4\u5fc5\u987b\u665a\u4e8e\u5f00\u59cb\u65f6\u95f4");
        }
    }

    /**
     * \u6821\u9a8c\u4fee\u6539\u79d2\u6740\u5238\u8bf7\u6c42\u3002
     */
    private void validateUpdateRequest(SeckillVoucherUpdateRequest request) {
        if (request == null || request.getVoucherId() == null) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238ID\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (request.getTitle() == null || request.getTitle().trim().isEmpty()) {
            throw new IllegalArgumentException("\u79d2\u6740\u5238\u6807\u9898\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (request.getStock() == null || request.getStock() < 0) {
            throw new IllegalArgumentException("\u5e93\u5b58\u4e0d\u80fd\u5c0f\u4e8e 0");
        }
        LocalDateTime beginTime = request.getBeginTime();
        LocalDateTime endTime = request.getEndTime();
        if (beginTime == null || endTime == null) {
            throw new IllegalArgumentException("\u5f00\u59cb\u65f6\u95f4\u548c\u7ed3\u675f\u65f6\u95f4\u4e0d\u80fd\u4e3a\u7a7a");
        }
        if (!endTime.isAfter(beginTime)) {
            throw new IllegalArgumentException("\u7ed3\u675f\u65f6\u95f4\u5fc5\u987b\u665a\u4e8e\u5f00\u59cb\u65f6\u95f4");
        }
    }

    /**
     * \u628a Redis \u4e2d\u7684 JSON \u53cd\u5e8f\u5217\u5316\u6210\u79d2\u6740\u5238\u5bf9\u8c61\u3002
     */
    private SeckillVoucher readVoucherFromCache(String cacheJson) {
        try {
            return objectMapper.readValue(cacheJson, SeckillVoucher.class);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize voucher cache from Redis", e);
            return null;
        }
    }

    private void writeVoucherToCache(SeckillVoucher voucher) {
        try {
            stringRedisTemplate.opsForValue().set(
                    SECKILL_VOUCHER_KEY + voucher.getVoucherId(),
                    objectMapper.writeValueAsString(voucher),
                    SECKILL_VOUCHER_TTL,
                    TimeUnit.MINUTES
            );
        } catch (Exception e) {
            log.warn("Failed to write voucher cache, voucherId={}", voucher.getVoucherId(), e);
        }
    }

    private void writeStockToRedis(SeckillVoucher voucher) {
        try {
            stringRedisTemplate.opsForValue().set(
                    SECKILL_STOCK_KEY + voucher.getVoucherId(),
                    String.valueOf(voucher.getStock())
            );
        } catch (Exception e) {
            log.warn("Failed to write stock to Redis, voucherId={}", voucher.getVoucherId(), e);
        }
    }

    private void writeStockToRedisStrict(SeckillVoucher voucher) {
        try {
            stringRedisTemplate.opsForValue().set(
                    SECKILL_STOCK_KEY + voucher.getVoucherId(),
                    String.valueOf(voucher.getStock())
            );
        } catch (Exception e) {
            throw new IllegalStateException("Write stock to Redis failed, voucherId=" + voucher.getVoucherId(), e);
        }
    }

    /**
     * \u53ea\u6709\u79d2\u6740\u7ed3\u675f\u4e14\u6ca1\u6709\u5904\u7406\u4e2d\u8ba2\u5355\u65f6\uff0c\u624d\u5141\u8bb8\u5bf9\u8d26\u8be5\u5238\u5e93\u5b58\u3002
     */
    private boolean canReconcileVoucherStock(SeckillVoucher voucher, LocalDateTime now) {
        if (voucher == null || voucher.getVoucherId() == null) {
            return false;
        }
        if (voucher.getEndTime() == null || now == null) {
            return false;
        }
        if (!voucher.getEndTime().isBefore(now)) {
            return false;
        }

        Long processingOrderCount = voucherOrderMapper.selectCount(
                new LambdaQueryWrapper<VoucherOrder>()
                        .eq(VoucherOrder::getVoucherId, voucher.getVoucherId())
                        .eq(VoucherOrder::getStatus, VoucherOrder.STATUS_PROCESSING)
        );
        if (processingOrderCount != null && processingOrderCount > 0) {
            log.debug("Skip stock reconciliation because processing orders still exist, voucherId={}, processingOrderCount={}",
                    voucher.getVoucherId(), processingOrderCount);
            return false;
        }
        return true;
    }

    /**
     * \u5bf9\u8d26\u65f6\u53d1\u73b0\u5e93\u5b58\u4e0d\u4e00\u81f4\uff0c\u6309 MySQL \u771f\u503c\u8986\u76d6 Redis\u3002
     */
    private boolean repairVoucherStockIfNecessary(SeckillVoucher voucher) {
        if (voucher == null || voucher.getVoucherId() == null || voucher.getStock() == null) {
            return false;
        }

        String stockKey = SECKILL_STOCK_KEY + voucher.getVoucherId();
        String redisStock = stringRedisTemplate.opsForValue().get(stockKey);
        String mysqlStock = String.valueOf(voucher.getStock());
        if (mysqlStock.equals(redisStock)) {
            return false;
        }

        log.warn("Detected stock mismatch, repairing Redis from MySQL, voucherId={}, mysqlStock={}, redisStock={}",
                voucher.getVoucherId(), mysqlStock, redisStock);
        writeStockToRedisStrict(voucher);
        return true;
    }

    private SeckillVoucher readVoucherFromLocalCache(Long voucherId) {
        try {
            return localVoucherCache.get(voucherId);
        } catch (Exception e) {
            log.warn("Read local Caffeine cache failed, fallback to next layer, voucherId={}", voucherId, e);
            return null;
        }
    }

    private boolean mightContainVoucherSafely(Long voucherId) {
        try {
            return localVoucherBloomFilter.mightContain(voucherId);
        } catch (Exception e) {
            log.warn("Read local Bloom failed, fallback to Redis/DB, voucherId={}", voucherId, e);
            return true;
        }
    }

    /**
     * \u8bfb\u53d6 Redis \u5355\u5238\u7f13\u5b58\uff0c\u5f02\u5e38\u65f6\u5141\u8bb8\u56de\u6e90\u6570\u636e\u5e93\u3002
     */
    private String readVoucherCacheFromRedis(String cacheKey) {
        try {
            return stringRedisTemplate.opsForValue().get(cacheKey);
        } catch (Exception e) {
            log.warn("Read Redis voucher cache failed, fallback to database, cacheKey={}", cacheKey, e);
            return null;
        }
    }

    private void cacheVoucherToLocal(SeckillVoucher voucher) {
        try {
            localVoucherCache.put(voucher);
        } catch (Exception e) {
            log.warn("Write local Caffeine cache failed, voucherId={}", voucher.getVoucherId(), e);
        }
    }

    /**
     * \u67e5\u65e0\u6b64\u5238\u65f6\u5199\u5165\u77ed TTL \u7a7a\u503c\u7f13\u5b58\uff0c\u9632\u6b62\u7a7f\u900f\u3002
     */
    private void cacheNullVoucherSafely(String cacheKey, Long voucherId) {
        try {
            stringRedisTemplate.opsForValue().set(cacheKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.warn("Write null voucher cache failed, voucherId={}", voucherId, e);
        }
    }
}
