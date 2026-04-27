package com.kpdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpdp.dto.Result;
import com.kpdp.dto.SeckillOrderDeadLetterVO;
import com.kpdp.dto.SeckillOrderMessage;
import com.kpdp.dto.UserDTO;
import com.kpdp.dto.VoucherOrderStatusVO;
import com.kpdp.entity.OrderOutbox;
import com.kpdp.entity.SeckillVoucher;
import com.kpdp.entity.VoucherOrder;
import com.kpdp.mapper.VoucherOrderMapper;
import com.kpdp.mq.SeckillOrderDeadLetterInspector;
import com.kpdp.service.IOrderOutboxService;
import com.kpdp.service.ISeckillVoucherService;
import com.kpdp.service.IVoucherOrderService;
import com.kpdp.utils.RedisConstants;
import com.kpdp.utils.SnowflakeIdWorker;
import com.kpdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import jakarta.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 秒杀订单服务实现。
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    /**
     * Outbox 最终死亡时的失败码。
     */
    private static final String FAIL_CODE_OUTBOX_DEAD = "OUTBOX_DEAD";

    /**
     * 消费重试阶段记录的失败码。
     */
    private static final String FAIL_CODE_MQ_CONSUME_RETRY = "MQ_CONSUME_RETRY";

    /**
     * 消费最终进入死信队列时的失败码。
     */
    private static final String FAIL_CODE_MQ_CONSUME_DLQ = "MQ_CONSUME_DLQ";

    /**
     * 处理中订单缺失时的失败码。
     */
    private static final String FAIL_CODE_PROCESSING_ORDER_MISSING = "PROCESSING_ORDER_MISSING";

    /**
     * 处理中订单超过半小时仍未终结时的取消原因码。
     */
    private static final String FAIL_CODE_ORDER_TIMEOUT_CANCELLED = "ORDER_TIMEOUT_CANCELLED";

    /**
     * 超时关单时写入订单与 Outbox 的统一原因文案。
     */
    private static final String ORDER_TIMEOUT_CANCEL_REASON = "超过半小时自动取消";

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        SECKILL_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("seckill_rollback.lua"));
        SECKILL_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private TransactionTemplate transactionTemplate;

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private IOrderOutboxService orderOutboxService;

    @Resource
    private SeckillOrderDeadLetterInspector seckillOrderDeadLetterInspector;

    @Value("${kpdp.rocketmq.seckill-order-topic}")
    private String seckillOrderTopic;

    @Value("${kpdp.outbox.batch-size}")
    private Integer outboxBatchSize;

    @Value("${kpdp.outbox.retry-interval-seconds}")
    private Integer outboxRetryIntervalSeconds;

    @Value("${kpdp.outbox.sending-timeout-seconds}")
    private Integer outboxSendingTimeoutSeconds;

    @Value("${kpdp.outbox.max-retry-count}")
    private Integer outboxMaxRetryCount;

    @Value("${kpdp.order-timeout.batch-size}")
    private Integer orderTimeoutBatchSize;

    @Value("${kpdp.order-timeout.timeout-minutes}")
    private Long orderTimeoutMinutes;

    @Override
    public Result seckillVoucher(Long voucherId) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }

        SeckillVoucher voucher;
        try {
            // 秒杀入口和查券接口复用同一条缓存链路，避免每次先打数据库。
            voucher = seckillVoucherService.getSeckillVoucherWithCache(voucherId);
        } catch (IllegalStateException e) {
            log.error("查询秒杀券缓存链路失败，voucherId={}", voucherId, e);
            return Result.fail("系统繁忙，请稍后再试");
        } catch (Exception e) {
            log.error("查询秒杀券信息失败，voucherId={}", voucherId, e);
            return Result.fail("系统繁忙，请稍后再试");
        }
        if (voucher == null) {
            return Result.fail("秒杀券不存在");
        }
        LocalDateTime now = LocalDateTime.now();
        if (voucher.getBeginTime().isAfter(now)) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime().isBefore(now)) {
            return Result.fail("秒杀已经结束");
        }

        long orderId;
        try {
            orderId = snowflakeIdWorker.nextId();
        } catch (Exception e) {
            log.error("生成雪花订单号失败，voucherId={}, userId={}", voucherId, user.getId(), e);
            return Result.fail("系统繁忙，请稍后再试");
        }

        Long result;
        try {
            result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    user.getId().toString()
            );
        } catch (Exception e) {
            // Redis 或 Lua 执行异常时直接快速失败，避免降级成数据库直写导致打爆数据库。
            log.error("执行秒杀资格预占脚本失败，voucherId={}, userId={}", voucherId, user.getId(), e);
            return Result.fail("秒杀服务繁忙，请稍后再试");
        }
        if (result == null) {
            return Result.fail("秒杀请求提交失败");
        }
        int code = result.intValue();
        if (code != 0) {
            if (code == 1) {
                return Result.fail("库存不足");
            }
            if (code == 2) {
                return Result.fail("不能重复下单");
            }
            if (code == 3) {
                return Result.fail("秒杀券库存未预热，请稍后再试");
            }
            return Result.fail("秒杀请求提交失败");
        }

        SeckillOrderMessage orderMessage = buildSeckillOrderMessage(orderId, user.getId(), voucherId);
        boolean persisted = saveOrderAndOutbox(orderMessage);
        if (!persisted) {
            log.error("保存订单与Outbox失败，开始回滚资格，orderId={}, userId={}, voucherId={}",
                    orderId, user.getId(), voucherId);
            rollbackSeckillReservation(voucherId, user.getId(), orderId);
            return Result.fail("订单创建失败，请稍后再试");
        }
        return Result.ok(orderId);
    }

    @Override
    public Result queryOrderStatus(Long orderId) {
        if (orderId == null) {
            return Result.fail("订单ID不能为空");
        }

        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }

        VoucherOrder order = lambdaQuery()
                .eq(VoucherOrder::getId, orderId)
                .eq(VoucherOrder::getUserId, user.getId())
                .one();
        if (order == null) {
            return Result.fail("订单不存在");
        }

        OrderOutbox outbox = orderOutboxService.lambdaQuery()
                .eq(OrderOutbox::getId, orderId)
                .eq(OrderOutbox::getUserId, user.getId())
                .one();

        VoucherOrderStatusVO statusVO = new VoucherOrderStatusVO();
        statusVO.setOrderId(order.getId());
        statusVO.setVoucherId(order.getVoucherId());
        statusVO.setOrderStatus(order.getStatus());
        statusVO.setOrderStatusDesc(buildOrderStatusDesc(order.getStatus()));
        statusVO.setFinishTime(order.getFinishTime());
        statusVO.setFailCode(order.getFailCode());
        statusVO.setFailReason(order.getFailReason());
        if (outbox != null) {
            statusVO.setOutboxStatus(outbox.getStatus());
            statusVO.setOutboxStatusDesc(buildOutboxStatusDesc(outbox.getStatus()));
        }
        return Result.ok(statusVO);
    }

    @Override
    public Result queryDeadLetters(Integer limit) {
        List<SeckillOrderDeadLetterVO> deadLetters = seckillOrderDeadLetterInspector.queryDeadLetters(limit);
        return Result.ok(deadLetters);
    }

    /**
     * 供 RocketMQ 消费者调用的异步落单入口。
     *
     * @param message 秒杀订单消息
     */
    public void handleSeckillOrderMessage(SeckillOrderMessage message) {
        if (message == null
                || message.getOrderId() == null
                || message.getUserId() == null
                || message.getVoucherId() == null) {
            // 非法消息不能当成功消费，否则会静默丢失，交给 RocketMQ 重试并最终进入死信。
            throw new IllegalArgumentException("收到非法秒杀订单消息: " + message);
        }
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(message.getOrderId());
        voucherOrder.setUserId(message.getUserId());
        voucherOrder.setVoucherId(message.getVoucherId());
        // 消费消息时默认视为“处理中”，最终由消费成功逻辑改成“已完成”。
        voucherOrder.setStatus(VoucherOrder.STATUS_PROCESSING);
        handleVoucherOrder(voucherOrder);
    }

    /**
     * 记录订单消费重试阶段的最近一次失败原因。
     * <p>这里不改变订单终态，只更新失败码和失败原因，便于最终进入死信时保留最后一次错误。</p>
     *
     * @param message 秒杀订单消息
     * @param throwable 本次消费失败异常
     */
    public void recordConsumeRetryFailure(SeckillOrderMessage message, Throwable throwable) {
        if (message == null || message.getOrderId() == null || message.getUserId() == null) {
            return;
        }
        try {
            VoucherOrder existingOrder = findOrderByIdAndUserId(message.getOrderId(), message.getUserId());
            if (existingOrder == null || !VoucherOrder.STATUS_PROCESSING.equals(existingOrder.getStatus())) {
                return;
            }
            VoucherOrder updateOrder = new VoucherOrder();
            updateOrder.setId(message.getOrderId());
            updateOrder.setFailCode(FAIL_CODE_MQ_CONSUME_RETRY);
            updateOrder.setFailReason(trimErrorMessage(throwable == null ? null : throwable.getMessage()));
            updateOrderByIdAndUserId(updateOrder, message.getUserId());
        } catch (Exception e) {
            log.warn("记录订单消费重试失败原因时出现异常，orderId={}", message.getOrderId(), e);
        }
    }

    /**
     * 处理已进入死信队列的订单消息。
     * <p>这里会把仍处于处理中状态的订单收敛为失败态，并回滚 Redis 中预占的秒杀资格。</p>
     *
     * @param message 死信订单消息
     */
    public void handleSeckillOrderDeadLetter(SeckillOrderMessage message) {
        if (message == null
                || message.getOrderId() == null
                || message.getUserId() == null
                || message.getVoucherId() == null) {
            throw new IllegalArgumentException("收到非法秒杀订单死信消息: " + message);
        }
        markOrderFailedSafely(
                message.getOrderId(),
                message.getUserId(),
                message.getVoucherId(),
                FAIL_CODE_MQ_CONSUME_DLQ,
                null
        );
    }

    /**
     * 定时扫描已经超过超时时间的处理中订单，并尝试把它们收敛为“已取消”。
     *
     * <p>这里的关键点不是单纯扫表，而是取消动作和消费完成动作都依赖
     * {@code where status = 处理中} 的 CAS 语义，避免并发下相互覆盖。</p>
     */
    @Scheduled(
            initialDelayString = "${kpdp.order-timeout.initial-delay-ms}",
            fixedDelayString = "${kpdp.order-timeout.scan-delay-ms}"
    )
    public void cancelExpiredProcessingOrders() {
        try {
            LocalDateTime deadline = LocalDateTime.now().minusMinutes(Math.max(orderTimeoutMinutes, 1L));
            List<VoucherOrder> expiredOrders = lambdaQuery()
                    .select(VoucherOrder::getId, VoucherOrder::getUserId, VoucherOrder::getVoucherId, VoucherOrder::getCreateTime)
                    .eq(VoucherOrder::getStatus, VoucherOrder.STATUS_PROCESSING)
                    .le(VoucherOrder::getCreateTime, deadline)
                    .orderByAsc(VoucherOrder::getCreateTime)
                    .last("limit " + Math.max(orderTimeoutBatchSize, 1))
                    .list();
            if (expiredOrders == null || expiredOrders.isEmpty()) {
                return;
            }

            for (VoucherOrder expiredOrder : expiredOrders) {
                cancelOrderByTimeout(expiredOrder);
            }
        } catch (Exception e) {
            log.error("扫描超时处理中订单失败", e);
        }
    }

    /**
     * 定时扫描 Outbox，并把待发送消息异步投递到 RocketMQ。
     *
     * <p>高频任务只扫 Redis 就绪索引，不再每 50ms 广播扫描所有分片表。</p>
     */
    @Scheduled(fixedDelayString = "${kpdp.outbox.scan-delay-ms}")
    public void publishOrderOutboxMessages() {
        try {
            processDueSendingOutboxMessages();
        } catch (Exception e) {
            log.error("扫描并发布Outbox消息失败", e);
        }
    }

    /**
     * 低频补偿回填 Outbox 分发索引。
     *
     * <p>这一步仍然会扫数据库，但只作为补偿路径低频执行，
     * 用来兜底“落库成功但 Redis 索引尚未来得及写入”这类极端窗口。</p>
     */
    @Scheduled(
            initialDelayString = "${kpdp.outbox.compensation-initial-delay-ms}",
            fixedDelayString = "${kpdp.outbox.compensation-scan-delay-ms}"
    )
    public void rebuildOutboxDispatchIndex() {
        try {
            LocalDateTime now = LocalDateTime.now();
            List<OrderOutbox> pendingOutboxList = orderOutboxService.lambdaQuery()
                    .select(OrderOutbox::getId, OrderOutbox::getUserId, OrderOutbox::getNextRetryTime)
                    .eq(OrderOutbox::getStatus, OrderOutbox.STATUS_PENDING)
                    .le(OrderOutbox::getNextRetryTime, now)
                    .orderByAsc(OrderOutbox::getCreateTime)
                    .last("limit " + Math.max(outboxBatchSize, 1))
                    .list();
            if (pendingOutboxList == null || pendingOutboxList.isEmpty()) {
                return;
            }
            for (OrderOutbox outbox : pendingOutboxList) {
                // 中文注释：低频补偿只兜底“待发送”记录，直接补一次立即发送。
                tryPublishOutboxImmediately(outbox.getId(), outbox.getUserId());
            }
        } catch (Exception e) {
            log.error("补偿扫描待发送 Outbox 失败", e);
        }
    }

    /**
     * 尝试把单笔超时订单收敛为“已取消”，并停止后续不必要的 Outbox 投递。
     *
     * <p>如果订单已经被消费者推进为其他终态，这里的 CAS 更新会直接失败，
     * 不会把已完成或已失败订单覆盖成已取消。</p>
     *
     * @param expiredOrder 命中的超时处理中订单
     */
    private void cancelOrderByTimeout(VoucherOrder expiredOrder) {
        if (expiredOrder == null
                || expiredOrder.getId() == null
                || expiredOrder.getUserId() == null
                || expiredOrder.getVoucherId() == null) {
            return;
        }
        Boolean cancelled = transactionTemplate.execute(status -> {
            LocalDateTime now = LocalDateTime.now();
            boolean orderCancelled = lambdaUpdate()
                    .eq(VoucherOrder::getId, expiredOrder.getId())
                    .eq(VoucherOrder::getUserId, expiredOrder.getUserId())
                    .eq(VoucherOrder::getStatus, VoucherOrder.STATUS_PROCESSING)
                    .set(VoucherOrder::getStatus, VoucherOrder.STATUS_CANCELLED)
                    .set(VoucherOrder::getFinishTime, now)
                    .set(VoucherOrder::getFailCode, FAIL_CODE_ORDER_TIMEOUT_CANCELLED)
                    .set(VoucherOrder::getFailReason, ORDER_TIMEOUT_CANCEL_REASON)
                    .update();
            if (!orderCancelled) {
                return Boolean.FALSE;
            }
            // 只收敛尚未稳定投递成功的 Outbox；已经 SENT 的消息允许消费者读到取消态后自然跳过。
            orderOutboxService.lambdaUpdate()
                    .set(OrderOutbox::getStatus, OrderOutbox.STATUS_DEAD)
                    .set(OrderOutbox::getLastError, ORDER_TIMEOUT_CANCEL_REASON)
                    .eq(OrderOutbox::getId, expiredOrder.getId())
                    .eq(OrderOutbox::getUserId, expiredOrder.getUserId())
                    .in(OrderOutbox::getStatus, OrderOutbox.STATUS_PENDING, OrderOutbox.STATUS_SENDING, OrderOutbox.STATUS_RETRYING)
                    .update();
            return Boolean.TRUE;
        });
        if (!Boolean.TRUE.equals(cancelled)) {
            return;
        }
        clearOutboxDispatchIndex(expiredOrder.getId(), expiredOrder.getUserId());
        log.warn("订单超过半小时仍处于处理中，已自动取消，orderId={}, userId={}, voucherId={}",
                expiredOrder.getId(), expiredOrder.getUserId(), expiredOrder.getVoucherId());
        rollbackSeckillReservation(expiredOrder.getVoucherId(), expiredOrder.getUserId(), expiredOrder.getId());
    }

    /**
     * 从 Redis 的“发送中租约索引”里恢复已经超时的 Outbox。
     *
     * <p>一旦 asyncSend 之后进程崩溃，消息可能永久停留在“发送中”。
     * 这里按租约到期时间恢复成“重试中”，并重新放回就绪索引。</p>
     */
    /**
     * 中文注释：高频任务只扫描 sending 这一份 ZSet。
     *
     * <p>同一个 ZSet 里可能有两类成员：</p>
     * <p>1. 状态仍为发送中的租约，到了 score 就表示发送超时。</p>
     * <p>2. 状态已经转成待发送/重试中的任务，到了 score 就表示可以再次尝试投递。</p>
     */
    private void processDueSendingOutboxMessages() {
    long nowMillis = System.currentTimeMillis();
    Set<String> dueMembers = stringRedisTemplate.opsForZSet().rangeByScore(
            RedisConstants.OUTBOX_SENDING_KEY,
            0,
            nowMillis,
            0,
            Math.max(outboxBatchSize, 1)
    );
    if (dueMembers == null || dueMembers.isEmpty()) {
        return;
    }
    for (String member : dueMembers) {
        if (!claimDispatchMember(RedisConstants.OUTBOX_SENDING_KEY, member)) {
            continue;
        }
        OutboxDispatchKey dispatchKey = parseOutboxDispatchKey(member);
        if (dispatchKey == null) {
            continue;
        }
        OrderOutbox outbox = findOutboxByIdAndUserId(dispatchKey.orderId(), dispatchKey.userId());
        if (outbox == null) {
            clearOutboxDispatchIndex(dispatchKey.orderId(), dispatchKey.userId());
            continue;
        }
        if (OrderOutbox.STATUS_SENDING.equals(outbox.getStatus())) {
            recoverSendingOutboxAndRetry(dispatchKey);
            continue;
        }
        if (OrderOutbox.STATUS_RETRYING.equals(outbox.getStatus())) {
            tryPublishOutboxImmediately(dispatchKey.orderId(), dispatchKey.userId());
            continue;
        }
        if (OrderOutbox.STATUS_PENDING.equals(outbox.getStatus())) {
            // sending 索引里理论上不应该出现“待发送”记录，这里做一次兜底纠偏并立刻重试。
            log.warn("发现异常的待发送 Outbox 仍停留在 sending 索引中，准备纠偏重试，outboxId={}, userId={}",
                    dispatchKey.orderId(), dispatchKey.userId());
            clearOutboxDispatchIndex(dispatchKey.orderId(), dispatchKey.userId());
            tryPublishOutboxImmediately(dispatchKey.orderId(), dispatchKey.userId());
            continue;
        }
        clearOutboxDispatchIndex(dispatchKey.orderId(), dispatchKey.userId());
    }
}

    /**
     * 中文注释：发送中的消息超过租约仍未收到回调时，先恢复为重试中，再立即重走一次精确投递。
     */
    private void recoverSendingOutboxAndRetry(OutboxDispatchKey dispatchKey) {
        LocalDateTime nextRetryTime = currentSecond().plusSeconds(Math.max(outboxRetryIntervalSeconds, 1));
        boolean recovered = orderOutboxService.lambdaUpdate()
                .set(OrderOutbox::getStatus, OrderOutbox.STATUS_RETRYING)
                .set(OrderOutbox::getNextRetryTime, nextRetryTime)
                .set(OrderOutbox::getLastError, "发送超时，已自动转为重试中")
                .eq(OrderOutbox::getId, dispatchKey.orderId())
                .eq(OrderOutbox::getUserId, dispatchKey.userId())
                .eq(OrderOutbox::getStatus, OrderOutbox.STATUS_SENDING)
                .update();
        if (!recovered) {
            clearOutboxDispatchIndex(dispatchKey.orderId(), dispatchKey.userId());
            return;
        }
        // 发送超时后继续保留在 sending 索引中，等待下一个重试时间到达后再次发送。
        registerOutboxSendingIndex(dispatchKey.orderId(), dispatchKey.userId(), toFutureScore(nextRetryTime));
        log.warn("发送中的 Outbox 已转为重试中，outboxId={}, userId={}, nextRetryTime={}",
                dispatchKey.orderId(), dispatchKey.userId(), nextRetryTime);
    }

    /**
     * 异步发送 Outbox 消息。
     *
     * <p>这里真正使用 RocketMQ 的 asyncSend，发送结果通过回调更新 Outbox 状态。</p>
     *
     * @param outbox Outbox 记录
     */
    private void sendOutboxMessageAsync(OrderOutbox outbox) {
        try {
            SeckillOrderMessage orderMessage = parseOutboxPayload(outbox);
            rocketMQTemplate.asyncSend(
                    outbox.getTopic(),
                    orderMessage,
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                handleOutboxSendFailure(outbox, "RocketMQ 返回了非成功发送状态");
                                return;
                            }
                            boolean updated = orderOutboxService.lambdaUpdate()
                                    .set(OrderOutbox::getStatus, OrderOutbox.STATUS_SENT)
                                    .set(OrderOutbox::getMqMsgId, sendResult.getMsgId())
                                    .set(OrderOutbox::getLastError, null)
                                    .eq(OrderOutbox::getId, outbox.getId())
                                    .eq(OrderOutbox::getUserId, outbox.getUserId())
                                    .eq(OrderOutbox::getStatus, OrderOutbox.STATUS_SENDING)
                                    .update();
                            if (!updated) {
                                log.warn("更新 Outbox 已发送状态失败，outboxId={}, userId={}", outbox.getId(), outbox.getUserId());
                                return;
                            }
                            clearOutboxDispatchIndex(outbox.getId(), outbox.getUserId());
                            log.info("Outbox 消息发送成功，outboxId={}, userId={}, msgId={}",
                                    outbox.getId(), outbox.getUserId(), sendResult.getMsgId());
                        }

                        @Override
                        public void onException(Throwable e) {
                            handleOutboxSendFailure(outbox, e == null ? "未知发送异常" : e.getMessage());
                        }
                    },
                    3000
            );
        } catch (Exception e) {
            handleOutboxSendFailure(outbox, e.getMessage());
        }
    }

    /**
     * 处理 Outbox 发送失败。
     *
     * <p>未达到最大重试次数时转为“重试中”，达到次数后直接转为“死亡”。</p>
     *
     * @param outbox Outbox 记录
     * @param errorMessage 错误信息
     */
    private void handleOutboxSendFailure(OrderOutbox outbox, String errorMessage) {
        int nextRetryCount = (outbox.getRetryCount() == null ? 0 : outbox.getRetryCount()) + 1;
        if (nextRetryCount >= Math.max(outboxMaxRetryCount, 1)) {
            boolean dead = orderOutboxService.lambdaUpdate()
                    .set(OrderOutbox::getStatus, OrderOutbox.STATUS_DEAD)
                    .set(OrderOutbox::getRetryCount, nextRetryCount)
                    .set(OrderOutbox::getLastError, trimErrorMessage(errorMessage))
                    .eq(OrderOutbox::getId, outbox.getId())
                    .eq(OrderOutbox::getUserId, outbox.getUserId())
                    .eq(OrderOutbox::getStatus, OrderOutbox.STATUS_SENDING)
                    .update();
            if (dead) {
                clearOutboxDispatchIndex(outbox.getId(), outbox.getUserId());
                log.error("Outbox 发送失败次数达到上限，已转为死亡，outboxId={}, userId={}, retryCount={}, error={}",
                        outbox.getId(), outbox.getUserId(), nextRetryCount, errorMessage);
                markOrderFailedSafely(
                        outbox.getId(),
                        outbox.getUserId(),
                        outbox.getVoucherId(),
                        FAIL_CODE_OUTBOX_DEAD,
                        errorMessage
                );
            }
            return;
        }

        LocalDateTime nextRetryTime = currentSecond().plusSeconds(Math.max(outboxRetryIntervalSeconds, 1));
        boolean retrying = orderOutboxService.lambdaUpdate()
                .set(OrderOutbox::getStatus, OrderOutbox.STATUS_RETRYING)
                .set(OrderOutbox::getRetryCount, nextRetryCount)
                .set(OrderOutbox::getNextRetryTime, nextRetryTime)
                .set(OrderOutbox::getLastError, trimErrorMessage(errorMessage))
                .eq(OrderOutbox::getId, outbox.getId())
                .eq(OrderOutbox::getUserId, outbox.getUserId())
                .eq(OrderOutbox::getStatus, OrderOutbox.STATUS_SENDING)
                .update();
        if (retrying) {
            registerOutboxSendingIndex(outbox.getId(), outbox.getUserId(), toFutureScore(nextRetryTime));
            log.warn("Outbox 发送失败，已进入重试中，outboxId={}, userId={}, retryCount={}, nextRetryTime={}, error={}",
                    outbox.getId(), outbox.getUserId(), nextRetryCount, nextRetryTime, errorMessage);
        }
    }

    /**
     * 发送失败时回滚 Redis 中已经预占的库存和一人一单标记。
     *
     * @param voucherId 秒杀券 ID
     * @param userId 用户 ID
     * @param orderId 订单 ID，仅用于日志定位
     */
    private void rollbackSeckillReservation(Long voucherId, Long userId, Long orderId) {
        try {
            Long rollbackResult = stringRedisTemplate.execute(
                    SECKILL_ROLLBACK_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString()
            );
            log.warn("秒杀资格回滚完成，orderId={}, userId={}, voucherId={}, rollbackResult={}",
                    orderId, userId, voucherId, rollbackResult);
        } catch (Exception e) {
            log.error("秒杀资格回滚失败，可能需要人工排查，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId, e);
        }
    }

    /**
     * 构造统一的秒杀订单消息体，避免发送端和消费端字段漂移。
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     * @param voucherId 秒杀券 ID
     * @return 秒杀订单消息
     */
    private SeckillOrderMessage buildSeckillOrderMessage(Long orderId, Long userId, Long voucherId) {
        SeckillOrderMessage orderMessage = new SeckillOrderMessage();
        orderMessage.setOrderId(orderId);
        orderMessage.setUserId(userId);
        orderMessage.setVoucherId(voucherId);
        return orderMessage;
    }

    /**
     * RocketMQ 消费者拿到订单消息后，按用户粒度加锁并落库。
     *
     * @param voucherOrder 订单信息
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        boolean locked = redisLock.tryLock();
        if (!locked) {
            // 同一用户已存在正在处理中的消息时，交给 MQ 重试，避免直接丢单。
            throw new IllegalStateException("当前用户订单正在处理中，请稍后重试消费");
        }
        try {
            transactionTemplate.executeWithoutResult(status -> completeVoucherOrder(voucherOrder));
        } finally {
            redisLock.unlock();
        }
    }

    private void completeVoucherOrder(VoucherOrder voucherOrder) {
        Long orderId = voucherOrder.getId();
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        VoucherOrder existingOrder = findOrderByIdAndUserId(orderId, userId);
        if (existingOrder != null && VoucherOrder.STATUS_COMPLETED.equals(existingOrder.getStatus())) {
            log.info("订单已完成，直接跳过重复消费，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId);
            return;
        }
        if (existingOrder != null && VoucherOrder.STATUS_CANCELLED.equals(existingOrder.getStatus())) {
            log.warn("订单已取消，直接跳过后续消费，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId);
            return;
        }
        if (existingOrder != null && VoucherOrder.STATUS_FAILED.equals(existingOrder.getStatus())) {
            log.warn("订单已失败，直接跳过后续消费，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId);
            return;
        }
        if (existingOrder == null) {
            // “处理中”订单缺失说明订单状态已经不可靠，直接按失败处理。
            log.warn("处理中订单不存在，直接按失败处理，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId);
            markOrderFailedSafely(
                    orderId,
                    userId,
                    voucherId,
                    FAIL_CODE_PROCESSING_ORDER_MISSING,
                    "处理中订单不存在"
            );
            return;
        }

        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            // Redis 已经预扣成功但数据库扣减失败时，必须抛异常给 RocketMQ 重试并最终落入死信。
            throw new IllegalStateException("数据库扣减库存失败，voucherId=" + voucherId);
        }

        VoucherOrder updateOrder = new VoucherOrder();
        updateOrder.setId(orderId);
        updateOrder.setStatus(VoucherOrder.STATUS_COMPLETED);
        updateOrder.setFinishTime(LocalDateTime.now());
        updateOrder.setFailCode(null);
        updateOrder.setFailReason(null);
        boolean updated = updateOrderByIdAndUserId(updateOrder, userId);
        if (!updated) {
            throw new IllegalStateException("更新秒杀订单状态失败");
        }
    }

    /**
     * 把“处理中订单”和“待发送 Outbox”一起写入数据库。
     *
     * <p>这里才是 Outbox 的核心：先把消息落库，再交给后台发布器异步推送 RocketMQ。</p>
     *
     * @param orderMessage 秒杀订单消息
     * @return 是否保存成功
     */
    private boolean saveOrderAndOutbox(SeckillOrderMessage orderMessage) {
        try {
            Boolean saved = transactionTemplate.execute(status -> {
                VoucherOrder processingOrder = new VoucherOrder();
                processingOrder.setId(orderMessage.getOrderId());
                processingOrder.setUserId(orderMessage.getUserId());
                processingOrder.setVoucherId(orderMessage.getVoucherId());
                processingOrder.setStatus(VoucherOrder.STATUS_PROCESSING);
                boolean orderSaved = save(processingOrder);
                if (!orderSaved) {
                    status.setRollbackOnly();
                    return Boolean.FALSE;
                }

                OrderOutbox outbox = buildOrderOutbox(orderMessage);
                boolean outboxSaved = orderOutboxService.save(outbox);
                if (!outboxSaved) {
                    status.setRollbackOnly();
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            });
            if (!Boolean.TRUE.equals(saved)) {
                return false;
            }
            // 新单落库后立即写入就绪索引，并同步触发一次精准投递，主链路不再依赖高频扫表。
            try {
                tryPublishOutboxImmediately(orderMessage.getOrderId(), orderMessage.getUserId());
            } catch (Exception e) {
                // 这里不能把已经落库成功的订单再回滚成接口失败，后续交给低频补偿任务回填索引继续投递。
                log.error("初始化Outbox分发索引失败，后续将依赖补偿任务恢复，orderId={}, userId={}, voucherId={}",
                        orderMessage.getOrderId(), orderMessage.getUserId(), orderMessage.getVoucherId(), e);
            }
            return true;
        } catch (Exception e) {
            log.error("保存处理中订单和Outbox失败，orderId={}, userId={}, voucherId={}",
                    orderMessage.getOrderId(), orderMessage.getUserId(), orderMessage.getVoucherId(), e);
            return false;
        }
    }

    /**
     * 构造 Outbox 记录。
     *
     * @param orderMessage 秒杀订单消息
     * @return Outbox 记录
     */
    private OrderOutbox buildOrderOutbox(SeckillOrderMessage orderMessage) {
        OrderOutbox outbox = new OrderOutbox();
        outbox.setId(orderMessage.getOrderId());
        outbox.setUserId(orderMessage.getUserId());
        outbox.setVoucherId(orderMessage.getVoucherId());
        outbox.setTopic(seckillOrderTopic);
        outbox.setPayload(writeOutboxPayload(orderMessage));
        outbox.setStatus(OrderOutbox.STATUS_PENDING);
        outbox.setRetryCount(0);
        outbox.setNextRetryTime(currentSecond());
        return outbox;
    }

    /**
     * 新单落库后立刻按主键 + 分片键精准触发一次投递。
     *
     * <p>这样正常链路不再需要等待定时任务扫到这条 Outbox 才开始发 MQ。</p>
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     */
    private void tryPublishOutboxImmediately(Long orderId, Long userId) {
        if (orderId == null || userId == null) {
            return;
        }
        OrderOutbox outbox = findOutboxByIdAndUserId(orderId, userId);
        if (outbox == null) {
            clearOutboxDispatchIndex(orderId, userId);
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        if (!OrderOutbox.STATUS_PENDING.equals(outbox.getStatus())
                && !OrderOutbox.STATUS_RETRYING.equals(outbox.getStatus())) {
            refreshDispatchIndexForCurrentOutboxState(outbox, now);
            return;
        }

        boolean claimed = claimOutboxForSending(outbox, now);
        if (!claimed) {
            OrderOutbox latestOutbox = findOutboxByIdAndUserId(orderId, userId);
            if (latestOutbox != null) {
                refreshDispatchIndexForCurrentOutboxState(latestOutbox, now);
            }
            return;
        }

        long leaseDeadlineMillis = System.currentTimeMillis() + Math.max(outboxSendingTimeoutSeconds, 1) * 1000L;
        registerOutboxSendingIndex(outbox.getId(), outbox.getUserId(), leaseDeadlineMillis);
        sendOutboxMessageAsync(outbox);
    }

    /**
     * 根据当前 Outbox 状态刷新 Redis 的 sending 调度索引。
     *
     * <p>这里只有两类状态允许继续留在 sending 索引中：</p>
     * <p>1. 发送中：score 表示发送租约到期时间，用于高频任务检测是否超时。</p>
     * <p>2. 重试中：score 表示下次允许再次发送的时间，用于高频任务到点重试。</p>
     *
     * @param outbox 当前 Outbox 记录
     * @param now 当前时间
     */
    private void refreshDispatchIndexForCurrentOutboxState(OrderOutbox outbox, LocalDateTime now) {
        if (outbox == null || outbox.getId() == null || outbox.getUserId() == null) {
            return;
        }
        if (OrderOutbox.STATUS_RETRYING.equals(outbox.getStatus())) {
            LocalDateTime nextRetryTime = outbox.getNextRetryTime() == null ? now : outbox.getNextRetryTime();
            registerOutboxSendingIndex(outbox.getId(), outbox.getUserId(), toFutureScore(nextRetryTime));
            return;
        }
        if (OrderOutbox.STATUS_SENDING.equals(outbox.getStatus())) {
            LocalDateTime updateTime = outbox.getUpdateTime() == null ? now : outbox.getUpdateTime();
            registerOutboxSendingIndex(outbox.getId(), outbox.getUserId(), toSendingDeadlineScore(updateTime));
            return;
        }
        clearOutboxDispatchIndex(outbox.getId(), outbox.getUserId());
    }

    /**
     * 统一往 sending ZSet 写入调度时间。
     *
     * <p>这里的 score 既可能是发送中的租约到期时间，也可能是重试任务的下次可发送时间。</p>
     */
    private void registerOutboxSendingIndex(Long orderId, Long userId, long dueAtMillis) {
        registerOutboxSendingLease(orderId, userId, dueAtMillis);
    }

    private void registerOutboxSendingLease(Long orderId, Long userId, long leaseDeadlineMillis) {
        if (orderId == null || userId == null) {
            return;
        }
        String member = buildOutboxDispatchMember(userId, orderId);
        stringRedisTemplate.opsForZSet().add(
                RedisConstants.OUTBOX_SENDING_KEY,
                member,
                leaseDeadlineMillis
        );
    }

    /**
     * 清理一条 Outbox 在 Redis 中的所有分发索引。
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     */
    private void clearOutboxDispatchIndex(Long orderId, Long userId) {
        if (orderId == null || userId == null) {
            return;
        }
        String member = buildOutboxDispatchMember(userId, orderId);
        stringRedisTemplate.opsForZSet().remove(RedisConstants.OUTBOX_SENDING_KEY, member);
    }

    /**
     * 从指定 Redis 索引中领取一条成员。
     *
     * <p>领取成功后才允许继续处理，避免多实例同时消费同一条就绪任务。</p>
     *
     * @param dispatchKey Redis 索引 key
     * @param member 索引成员
     * @return 是否成功领取
     */
    private boolean claimDispatchMember(String dispatchKey, String member) {
        Long removed = stringRedisTemplate.opsForZSet().remove(dispatchKey, member);
        return removed != null && removed > 0;
    }

    /**
     * 把 Outbox 从“待发送/重试中”抢占成“发送中”。
     *
     * @param outbox Outbox 记录
     * @param now 当前时间
     * @return 是否抢占成功
     */
    private boolean claimOutboxForSending(OrderOutbox outbox, LocalDateTime now) {
        return orderOutboxService.lambdaUpdate()
                .set(OrderOutbox::getStatus, OrderOutbox.STATUS_SENDING)
                .set(OrderOutbox::getLastError, null)
                .eq(OrderOutbox::getId, outbox.getId())
                .eq(OrderOutbox::getUserId, outbox.getUserId())
                .in(OrderOutbox::getStatus, OrderOutbox.STATUS_PENDING, OrderOutbox.STATUS_RETRYING)
                .le(OrderOutbox::getNextRetryTime, now)
                .update();
    }

    /**
     * 按 Outbox 主键和用户 ID 精准查询 Outbox，避免分片表只按主键触发广播。
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     * @return 命中的 Outbox；未命中时返回 null
     */
    private OrderOutbox findOutboxByIdAndUserId(Long orderId, Long userId) {
        if (orderId == null || userId == null) {
            return null;
        }
        return orderOutboxService.lambdaQuery()
                .eq(OrderOutbox::getId, orderId)
                .eq(OrderOutbox::getUserId, userId)
                .one();
    }

    /**
     * 构造 Redis 分发索引成员。
     *
     * @param userId 用户 ID
     * @param orderId 订单 ID
     * @return 索引成员字符串
     */
    private String buildOutboxDispatchMember(Long userId, Long orderId) {
        return userId + ":" + orderId;
    }

    /**
     * 解析 Redis 分发索引成员。
     *
     * @param member 索引成员字符串
     * @return 解析结果；格式非法时返回 null
     */
    private OutboxDispatchKey parseOutboxDispatchKey(String member) {
        String[] parts = member.split(":");
        if (parts.length != 2) {
            log.warn("解析Outbox分发索引成员失败，member={}", member);
            return null;
        }
        try {
            return new OutboxDispatchKey(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        } catch (NumberFormatException e) {
            log.warn("解析Outbox分发索引成员失败，member={}", member, e);
            return null;
        }
    }

    /**
     * 把本地时间转换成毫秒时间戳，用作 Redis ZSet 的分数。
     *
     * @param time 本地时间
     * @return 毫秒时间戳
     */
    private long toFutureScore(LocalDateTime time) {
        long delayMillis = Math.max(0L, Duration.between(LocalDateTime.now(), time).toMillis());
        return System.currentTimeMillis() + delayMillis;
    }

    /**
     * 计算“发送中”状态写入 sending 索引时使用的租约到期时间。
     *
     * @param updateTime 最近一次进入发送中的时间
     * @return 租约到期时间戳
     */
    private long toSendingDeadlineScore(LocalDateTime updateTime) {
        return toFutureScore(updateTime.plusSeconds(Math.max(outboxSendingTimeoutSeconds, 1)));
    }

    /**
     * 把订单消息序列化成 Outbox JSON。
     *
     * @param orderMessage 秒杀订单消息
     * @return JSON 字符串
     */
    /**
     * 中文注释：把“立即可执行”的时间统一压成秒级，避免 MySQL 秒级时间列把毫秒部分进位到下一秒。
     */
    private LocalDateTime currentSecond() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
    }

    private String writeOutboxPayload(SeckillOrderMessage orderMessage) {
        try {
            return objectMapper.writeValueAsString(orderMessage);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("序列化Outbox消息失败", e);
        }
    }

    /**
     * 把 Outbox JSON 反序列化回订单消息。
     *
     * @param outbox Outbox 记录
     * @return 订单消息
     */
    private SeckillOrderMessage parseOutboxPayload(OrderOutbox outbox) {
        try {
            return objectMapper.readValue(outbox.getPayload(), SeckillOrderMessage.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("反序列化Outbox消息失败，outboxId=" + outbox.getId(), e);
        }
    }

    /**
     * 截断过长错误信息，避免把一整段异常栈塞进数据库字段。
     *
     * @param errorMessage 原始错误
     * @return 截断后的错误
     */
    private String trimErrorMessage(String errorMessage) {
        if (errorMessage == null || errorMessage.isBlank()) {
            return null;
        }
        return errorMessage.length() <= 512 ? errorMessage : errorMessage.substring(0, 512);
    }

    /**
     * 统一收敛订单失败原因。
     * <p>优先使用本次传入的失败原因；如果为空，则尝试复用历史失败原因；都没有时再按失败码生成默认文案。</p>
     *
     * @param failCode 失败码
     * @param currentReason 本次失败原因
     * @param existingReason 历史失败原因
     * @return 最终写入订单表的失败原因
     */
    private String resolveFailReason(String failCode, String currentReason, String existingReason) {
        String candidateReason = trimErrorMessage(currentReason);
        if (candidateReason != null) {
            return candidateReason;
        }
        candidateReason = trimErrorMessage(existingReason);
        if (candidateReason != null) {
            return candidateReason;
        }
        if (FAIL_CODE_MQ_CONSUME_DLQ.equals(failCode)) {
            return "订单消费重试耗尽，已进入死信队列";
        }
        if (FAIL_CODE_OUTBOX_DEAD.equals(failCode)) {
            return "Outbox发送失败且达到最大重试次数";
        }
        if (FAIL_CODE_PROCESSING_ORDER_MISSING.equals(failCode)) {
            return "处理中订单不存在";
        }
        return null;
    }

    /**
     * 把订单状态值转换成前端更容易直接展示的中文描述。
     *
     * @param status 订单状态
     * @return 状态描述
     */
    private String buildOrderStatusDesc(Integer status) {
        if (VoucherOrder.STATUS_PROCESSING.equals(status)) {
            return "处理中";
        }
        if (VoucherOrder.STATUS_COMPLETED.equals(status)) {
            return "已完成";
        }
        if (VoucherOrder.STATUS_CANCELLED.equals(status)) {
            return "已取消";
        }
        if (VoucherOrder.STATUS_FAILED.equals(status)) {
            return "失败";
        }
        return "未知状态";
    }

    /**
     * 把 Outbox 状态值转换成中文描述。
     *
     * @param status Outbox 状态
     * @return 状态描述
     */
    private String buildOutboxStatusDesc(Integer status) {
        if (status == null) {
            return null;
        }
        if (OrderOutbox.STATUS_PENDING.equals(status)) {
            return "待发送";
        }
        if (OrderOutbox.STATUS_SENDING.equals(status)) {
            return "发送中";
        }
        if (OrderOutbox.STATUS_SENT.equals(status)) {
            return "已发送";
        }
        if (OrderOutbox.STATUS_RETRYING.equals(status)) {
            return "重试中";
        }
        if (OrderOutbox.STATUS_DEAD.equals(status)) {
            return "死亡";
        }
        return "未知状态";
    }

    /**
     * 把订单安全收敛为“失败”，并回滚 Redis 中已经预占的秒杀资格。
     *
     * <p>这里只处理仍处于处理中态的订单；如果订单已经完成、取消或失败，则直接跳过。</p>
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     * @param voucherId 秒杀券 ID
     * @param failCode 失败码
     * @param failReason 失败原因
     */
    private void markOrderFailedSafely(Long orderId,
                                       Long userId,
                                       Long voucherId,
                                       String failCode,
                                       String failReason) {
        boolean failedMarked = false;
        try {
            VoucherOrder existingOrder = findOrderByIdAndUserId(orderId, userId);
            if (existingOrder == null) {
                VoucherOrder failedOrder = new VoucherOrder();
                failedOrder.setId(orderId);
                failedOrder.setUserId(userId);
                failedOrder.setVoucherId(voucherId);
                failedOrder.setStatus(VoucherOrder.STATUS_FAILED);
                failedOrder.setFinishTime(LocalDateTime.now());
                failedOrder.setFailCode(failCode);
                failedOrder.setFailReason(resolveFailReason(failCode, failReason, null));
                boolean saved = save(failedOrder);
                failedMarked = saved;
                if (!saved) {
                    log.warn("保存失败订单失败，orderId={}, userId={}, voucherId={}, failCode={}, failReason={}",
                            orderId, userId, voucherId, failCode, failReason);
                }
            } else if (VoucherOrder.STATUS_PROCESSING.equals(existingOrder.getStatus())) {
                VoucherOrder updateOrder = new VoucherOrder();
                updateOrder.setId(orderId);
                updateOrder.setStatus(VoucherOrder.STATUS_FAILED);
                updateOrder.setFinishTime(LocalDateTime.now());
                updateOrder.setFailCode(failCode);
                updateOrder.setFailReason(resolveFailReason(failCode, failReason, existingOrder.getFailReason()));
                boolean updated = updateOrderByIdAndUserId(updateOrder, userId);
                failedMarked = updated;
                if (!updated) {
                    log.warn("更新失败订单失败，orderId={}, userId={}, voucherId={}, failCode={}, failReason={}",
                            orderId, userId, voucherId, failCode, failReason);
                }
            }
        } catch (Exception e) {
            log.error("标记订单为失败状态失败，orderId={}, userId={}, voucherId={}, failCode={}, failReason={}",
                    orderId, userId, voucherId, failCode, failReason, e);
        } finally {
            // 只有订单已经可靠落成失败态时，才允许释放 Redis 中的资格占用。
            if (failedMarked) {
                rollbackSeckillReservation(voucherId, userId, orderId);
            }
        }
    }

    /**
     * 按订单 ID 和用户 ID 精准查询订单，避免分片表只按主键触发广播路由。
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     * @return 命中的订单记录；未命中时返回 null
     */
    private VoucherOrder findOrderByIdAndUserId(Long orderId, Long userId) {
        if (orderId == null || userId == null) {
            return null;
        }
        return lambdaQuery()
                .eq(VoucherOrder::getId, orderId)
                .eq(VoucherOrder::getUserId, userId)
                .one();
    }

    /**
     * 按订单 ID 和用户 ID 精准更新订单，避免按主键更新分片表时触发广播。
     *
     * @param updateOrder 待更新的订单字段
     * @param userId 用户 ID
     * @return 是否更新成功
     */
    private boolean updateOrderByIdAndUserId(VoucherOrder updateOrder, Long userId) {
        if (updateOrder == null || updateOrder.getId() == null || userId == null) {
            return false;
        }
        return lambdaUpdate()
                .eq(VoucherOrder::getId, updateOrder.getId())
                .eq(VoucherOrder::getUserId, userId)
                // 订单终态推进统一要求当前仍是“处理中”，避免被超时取消或其他终态并发覆盖。
                .eq(VoucherOrder::getStatus, VoucherOrder.STATUS_PROCESSING)
                .set(updateOrder.getStatus() != null, VoucherOrder::getStatus, updateOrder.getStatus())
                .set(updateOrder.getFinishTime() != null, VoucherOrder::getFinishTime, updateOrder.getFinishTime())
                .set(updateOrder.getFailCode() != null, VoucherOrder::getFailCode, updateOrder.getFailCode())
                .set(updateOrder.getFailReason() != null, VoucherOrder::getFailReason, updateOrder.getFailReason())
                // 订单成功收敛时需要清空旧失败信息，这里显式允许把失败字段置空。
                .set(updateOrder.getFailCode() == null, VoucherOrder::getFailCode, null)
                .set(updateOrder.getFailReason() == null, VoucherOrder::getFailReason, null)
                .update();
    }

    /**
     * Redis Outbox 分发索引里使用的轻量键对象。
     *
     * @param userId 用户 ID
     * @param orderId 订单 ID
     */
    private record OutboxDispatchKey(Long userId, Long orderId) {
    }
}
