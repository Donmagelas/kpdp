package com.kpdp.mq;

import com.kpdp.dto.SeckillOrderMessage;
import com.kpdp.service.impl.VoucherOrderServiceImpl;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单死信消费者。
 *
 * <p>这里专门订阅订单消费组对应的 DLQ，
 * 用于把最终进入死信的订单收敛成“失败”状态。</p>
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = "${kpdp.rocketmq.seckill-order-dlq-topic}",
        consumerGroup = "${kpdp.rocketmq.seckill-order-dlq-sync-consumer-group}",
        consumeMode = ConsumeMode.CONCURRENTLY,
        messageModel = MessageModel.CLUSTERING,
        maxReconsumeTimes = 3
)
public class SeckillOrderDeadLetterConsumer implements RocketMQListener<SeckillOrderMessage> {

    @Resource
    private VoucherOrderServiceImpl voucherOrderService;

    @Override
    public void onMessage(SeckillOrderMessage message) {
        try {
            voucherOrderService.handleSeckillOrderDeadLetter(message);
            log.warn("秒杀订单死信消息处理完成，orderId={}, userId={}, voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId());
        } catch (Exception e) {
            log.error("秒杀订单死信消息处理失败，message={}", message, e);
            throw e;
        }
    }
}
