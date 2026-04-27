package com.kpdp.mq;

import com.kpdp.dto.SeckillOrderMessage;
import com.kpdp.service.impl.VoucherOrderServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀订单消息消费者。
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = "${kpdp.rocketmq.seckill-order-topic}",
        consumerGroup = "${kpdp.rocketmq.seckill-order-consumer-group}",
        consumeMode = ConsumeMode.CONCURRENTLY,
        messageModel = MessageModel.CLUSTERING,
        maxReconsumeTimes = 3
)
public class SeckillOrderConsumer implements RocketMQListener<SeckillOrderMessage> {

    @Resource
    private VoucherOrderServiceImpl voucherOrderService;

    @Override
    public void onMessage(SeckillOrderMessage message) {
        try {
            // 消费失败时抛异常交给 RocketMQ 触发重试，超过次数后会进入死信主题。
            voucherOrderService.handleSeckillOrderMessage(message);
            log.debug("秒杀订单消息消费完成，orderId={}, userId={}, voucherId={}",
                    message.getOrderId(), message.getUserId(), message.getVoucherId());
        } catch (Exception e) {
            // 先把最后一次消费失败原因落到订单表，便于最终进入死信后保留可读原因。
            voucherOrderService.recordConsumeRetryFailure(message, e);
            log.error("秒杀订单消息消费失败，将进入 RocketMQ 重试或死信流程，message={}", message, e);
            throw e;
        }
    }
}
