package com.kpdp.ratelimit;

import com.kpdp.annotation.RateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀入口 P95 监控与全局限流降级任务。
 *
 * <p>当前版本只监控 {@code POST /voucher-order/seckill/{id}}，并且只动态调整全局限流，
 * 用户维度限流仍然保持注解里的固定值。</p>
 */
@Slf4j
@Component
public class SeckillRateDegradeTask {

    /**
     * 评估任务的分布式锁，避免多实例重复改写同一份动态限流配置。
     */
    private static final String RATE_DEGRADE_LOCK_KEY = "lock:rate:degrade:seckill:global";

    /**
     * 降级状态记录 key，用于跨轮次保留连续命中次数。
     */
    private static final String RATE_DEGRADE_STATE_KEY = "rate:degrade:state:seckill:global";

    @Resource
    private MeterRegistry meterRegistry;

    @Resource
    private RateLimitConfigService rateLimitConfigService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Value("${kpdp.rate-degrade.p95-threshold-ms}")
    private Double degradeP95ThresholdMs;

    @Value("${kpdp.rate-degrade.recover-threshold-ms}")
    private Double recoverP95ThresholdMs;

    @Value("${kpdp.rate-degrade.degrade-consecutive-threshold}")
    private Integer degradeConsecutiveThreshold;

    @Value("${kpdp.rate-degrade.recover-consecutive-threshold}")
    private Integer recoverConsecutiveThreshold;

    @Value("${kpdp.rate-degrade.default-global-limit}")
    private Long defaultGlobalLimit;

    @Value("${kpdp.rate-degrade.degraded-global-limit}")
    private Long degradedGlobalLimit;

    @Value("${kpdp.rate-degrade.global-window-seconds}")
    private Long globalWindowSeconds;

    /**
     * 每 10 秒评估一次最近 1 分钟的单实例 P95，并决定是否收紧全局限流。
     */
    @Scheduled(
            initialDelayString = "${kpdp.rate-degrade.initial-delay-ms}",
            fixedDelayString = "${kpdp.rate-degrade.scan-delay-ms}"
    )
    public void evaluateSeckillP95AndAdjustGlobalRateLimit() {
        Timer timer = meterRegistry.find("http.server.requests")
                .tags("uri", RateLimitConstants.SECKILL_URI_PATTERN, "method", "POST")
                .timer();
        if (timer == null) {
            return;
        }

        Double p95Millis = readP95Millis(timer);
        if (p95Millis == null || p95Millis <= 0) {
            return;
        }

        RLock lock = redissonClient.getLock(RATE_DEGRADE_LOCK_KEY);
        boolean locked = lock.tryLock();
        if (!locked) {
            return;
        }
        try {
            processDegradeState(p95Millis);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据当前 P95 更新连续命中次数，并在阈值达标时切换全局限流档位。
     *
     * @param p95Millis 当前秒杀接口 P95，单位毫秒
     */
    private void processDegradeState(double p95Millis) {
        DegradeState state = loadState();
        if (p95Millis > degradeP95ThresholdMs) {
            state.highStreak++;
            state.lowStreak = 0;
        } else if (p95Millis < recoverP95ThresholdMs) {
            state.lowStreak++;
            state.highStreak = 0;
        } else {
            state.highStreak = 0;
            state.lowStreak = 0;
        }

        long currentLimit = rateLimitConfigService.getDynamicRuleOptional(
                        RateLimitConstants.SECKILL_GLOBAL_KEY,
                        RateLimiter.LimitType.GLOBAL
                )
                .map(DynamicRateLimitRule::limit)
                .orElse(defaultGlobalLimit);

        if (p95Millis > degradeP95ThresholdMs
                && state.highStreak >= Math.max(degradeConsecutiveThreshold, 1)
                && currentLimit > degradedGlobalLimit) {
            rateLimitConfigService.saveDynamicRule(
                    RateLimitConstants.SECKILL_GLOBAL_KEY,
                    RateLimiter.LimitType.GLOBAL,
                    globalWindowSeconds,
                    degradedGlobalLimit
            );
            state.highStreak = 0;
            state.lowStreak = 0;
            currentLimit = degradedGlobalLimit;
            log.warn("秒杀接口 P95 持续超过阈值，已收紧全局限流，p95={}ms, newLimit={}", roundToLong(p95Millis), degradedGlobalLimit);
        } else if (p95Millis < recoverP95ThresholdMs
                && state.lowStreak >= Math.max(recoverConsecutiveThreshold, 1)
                && currentLimit < defaultGlobalLimit) {
            rateLimitConfigService.clearDynamicRule(
                    RateLimitConstants.SECKILL_GLOBAL_KEY,
                    RateLimiter.LimitType.GLOBAL
            );
            state.highStreak = 0;
            state.lowStreak = 0;
            currentLimit = defaultGlobalLimit;
            log.info("秒杀接口 P95 已恢复到安全区间，已放开全局限流，p95={}ms, newLimit={}", roundToLong(p95Millis), defaultGlobalLimit);
        }

        saveState(state, p95Millis, currentLimit);
    }

    /**
     * 从 Timer 快照中提取 P95。
     *
     * @param timer 秒杀接口计时器
     * @return P95 毫秒值；当前窗口无有效分位信息时返回 null
     */
    private Double readP95Millis(Timer timer) {
        HistogramSnapshot snapshot = timer.takeSnapshot();
        for (ValueAtPercentile percentile : snapshot.percentileValues()) {
            if (Math.abs(percentile.percentile() - 0.95d) < 0.0001d) {
                return percentile.value(TimeUnit.MILLISECONDS);
            }
        }
        return null;
    }

    /**
     * 读取上一轮降级状态。
     *
     * @return 降级状态
     */
    private DegradeState loadState() {
        Map<Object, Object> stateMap = stringRedisTemplate.opsForHash().entries(RATE_DEGRADE_STATE_KEY);
        DegradeState state = new DegradeState();
        state.highStreak = parseIntValue(stateMap.get("highStreak"));
        state.lowStreak = parseIntValue(stateMap.get("lowStreak"));
        return state;
    }

    /**
     * 持久化本轮降级状态，便于多轮次连续判断和人工查看。
     *
     * @param state 连续命中状态
     * @param p95Millis 本轮 P95
     * @param currentLimit 当前生效全局限流值
     */
    private void saveState(DegradeState state, double p95Millis, long currentLimit) {
        stringRedisTemplate.opsForHash().put(RATE_DEGRADE_STATE_KEY, "highStreak", String.valueOf(state.highStreak));
        stringRedisTemplate.opsForHash().put(RATE_DEGRADE_STATE_KEY, "lowStreak", String.valueOf(state.lowStreak));
        stringRedisTemplate.opsForHash().put(RATE_DEGRADE_STATE_KEY, "lastP95Millis", String.valueOf(roundToLong(p95Millis)));
        stringRedisTemplate.opsForHash().put(RATE_DEGRADE_STATE_KEY, "currentLimit", String.valueOf(currentLimit));
        stringRedisTemplate.opsForHash().put(RATE_DEGRADE_STATE_KEY, "updatedAt", String.valueOf(System.currentTimeMillis()));
        stringRedisTemplate.expire(RATE_DEGRADE_STATE_KEY, 1, TimeUnit.DAYS);
    }

    /**
     * 解析 Redis 中的整数状态值。
     *
     * @param value Redis 字段值
     * @return 解析后的整数；为空时返回 0
     */
    private int parseIntValue(Object value) {
        return Optional.ofNullable(value)
                .map(Object::toString)
                .filter(each -> !each.isBlank())
                .map(Integer::parseInt)
                .orElse(0);
    }

    /**
     * 把双精度毫秒值转成便于日志输出的长整型。
     *
     * @param value 双精度值
     * @return 四舍五入后的长整型
     */
    private long roundToLong(double value) {
        return Math.round(value);
    }

    /**
     * 降级连续命中状态。
     */
    private static final class DegradeState {

        /**
         * 连续高于降级阈值的次数。
         */
        private int highStreak;

        /**
         * 连续低于恢复阈值的次数。
         */
        private int lowStreak;
    }
}
