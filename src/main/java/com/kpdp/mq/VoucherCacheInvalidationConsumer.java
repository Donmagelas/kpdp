package com.kpdp.mq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.util.Iterator;

/**
 * 秒杀券缓存失效消息消费者。
 *
 * <p>这里约定消费 Canal flatMessage，并按券 ID 删除 Redis/Caffeine 缓存。</p>
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = "${kpdp.rocketmq.voucher-cache-invalidate-topic}",
        consumerGroup = "${kpdp.rocketmq.voucher-cache-invalidate-consumer-group}",
        consumeMode = ConsumeMode.CONCURRENTLY,
        // 每个实例使用不同消费组名，因此这里仍然可以保留 CLUSTERING，并同时拥有重试和 DLQ。
        messageModel = MessageModel.CLUSTERING,
        maxReconsumeTimes = 3
)
public class VoucherCacheInvalidationConsumer implements RocketMQListener<String> {

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Override
    public void onMessage(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            if (root.path("isDdl").asBoolean(false)) {
                log.info("收到 DDL 类型缓存失效消息，直接跳过，message={}", message);
                return;
            }
            if (!"tb_seckill_voucher".equals(root.path("table").asText())) {
                log.debug("收到非秒杀券表消息，直接跳过，table={}", root.path("table").asText());
                return;
            }

            boolean deleteOnly = "DELETE".equalsIgnoreCase(root.path("type").asText());
            Iterator<JsonNode> dataIterator = root.path("data").elements();
            while (dataIterator.hasNext()) {
                JsonNode row = dataIterator.next();
                long voucherId = row.path("voucher_id").asLong(-1L);
                if (voucherId <= 0) {
                    throw new IllegalArgumentException("缓存失效消息缺少合法 voucher_id: " + message);
                }
                seckillVoucherService.invalidateVoucherCache(voucherId, deleteOnly);
                log.info("秒杀券缓存失效完成，voucherId={}, deleteOnly={}", voucherId, deleteOnly);
            }
        } catch (Exception e) {
            log.error("处理秒杀券缓存失效消息失败，将进入 RocketMQ 重试或死信流程，message={}", message, e);
            throw new RuntimeException(e);
        }
    }
}
