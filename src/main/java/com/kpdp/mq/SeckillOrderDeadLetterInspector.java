package com.kpdp.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpdp.dto.SeckillOrderDeadLetterVO;
import com.kpdp.dto.SeckillOrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * 秒杀订单死信查看器。
 */
@Slf4j
@Component
public class SeckillOrderDeadLetterInspector {

    /**
     * RocketMQ 默认死信主题前缀。
     */
    private static final String DLQ_TOPIC_PREFIX = "%DLQ%";

    /**
     * 单次最多拉取多少条消息，避免查看接口一次性打太多数据。
     */
    private static final int MAX_LIMIT = 50;

    /**
     * 单次从 Broker 拉取的批量大小。
     */
    private static final int PULL_BATCH_SIZE = 16;

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${kpdp.rocketmq.seckill-order-consumer-group}")
    private String seckillOrderConsumerGroup;

    @Resource
    private ObjectMapper objectMapper;

    /**
     * 查询秒杀订单消费者对应的死信消息。
     *
     * @param limit 期望返回的条数
     * @return 死信消息列表
     */
    public List<SeckillOrderDeadLetterVO> queryDeadLetters(Integer limit) {
        return queryDeadLetters(seckillOrderConsumerGroup, limit);
    }

    /**
     * 按指定消费组查询死信消息。
     *
     * @param consumerGroup 消费组
     * @param limit 期望返回条数
     * @return 死信消息列表
     */
    public List<SeckillOrderDeadLetterVO> queryDeadLetters(String consumerGroup, Integer limit) {
        int safeLimit = normalizeLimit(limit);
        String targetConsumerGroup = normalizeConsumerGroup(consumerGroup);
        String dlqTopic = DLQ_TOPIC_PREFIX + targetConsumerGroup;
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(buildInspectorGroupName());
        pullConsumer.setNamesrvAddr(nameServer);
        // 每次查看都用独立实例名，避免复用同一客户端缓存导致结果不稳定。
        pullConsumer.setInstanceName("kpdp-dlq-inspector-" + System.nanoTime());

        try {
            pullConsumer.start();
            Set<MessageQueue> messageQueues = fetchDeadLetterQueuesSafely(pullConsumer, dlqTopic);
            if (messageQueues == null || messageQueues.isEmpty()) {
                return new ArrayList<>();
            }

            List<MessageQueue> orderedQueues = new ArrayList<>(messageQueues);
            orderedQueues.sort(Comparator
                    .comparing(MessageQueue::getBrokerName)
                    .thenComparing(MessageQueue::getQueueId));

            List<SeckillOrderDeadLetterVO> deadLetters = new ArrayList<>();
            for (MessageQueue queue : orderedQueues) {
                if (deadLetters.size() >= safeLimit) {
                    break;
                }
                pullQueueMessages(pullConsumer, queue, safeLimit, deadLetters);
            }
            return deadLetters;
        } catch (Exception e) {
            throw new IllegalStateException("查询 RocketMQ 死信消息失败", e);
        } finally {
            try {
                pullConsumer.shutdown();
            } catch (Exception e) {
                log.warn("关闭死信查看器失败", e);
            }
        }
    }

    /**
     * 安全获取死信主题队列。
     *
     * <p>当消费组还没有产生过真正的死信时，RocketMQ 不会提前创建对应 DLQ 主题，
     * 这时查询接口应该返回空列表，而不是把“主题不存在”当成系统异常。</p>
     *
     * @param pullConsumer 拉取消费者
     * @param dlqTopic 死信主题
     * @return 死信队列集合
     */
    private Set<MessageQueue> fetchDeadLetterQueuesSafely(DefaultMQPullConsumer pullConsumer,
                                                          String dlqTopic) throws Exception {
        try {
            return pullConsumer.fetchSubscribeMessageQueues(dlqTopic);
        } catch (MQClientException e) {
            String errorMessage = e.getErrorMessage();
            if ((errorMessage != null && errorMessage.contains("No topic route info"))
                    || e.getMessage().contains("Can not find Message Queue")) {
                log.info("死信主题尚未创建，当前视为空列表，topic={}", dlqTopic);
                return null;
            }
            throw e;
        }
    }

    /**
     * 从指定死信队列中拉取消息。
     *
     * <p>这里优先从靠近尾部的位置开始拉，尽量让查看接口返回较新的死信。</p>
     *
     * @param pullConsumer 拉取消费者
     * @param queue 死信队列
     * @param safeLimit 目标条数
     * @param deadLetters 已收集结果
     */
    private void pullQueueMessages(DefaultMQPullConsumer pullConsumer,
                                   MessageQueue queue,
                                   int safeLimit,
                                   List<SeckillOrderDeadLetterVO> deadLetters) throws Exception {
        long minOffset = pullConsumer.minOffset(queue);
        long maxOffset = pullConsumer.maxOffset(queue);
        long nextOffset = Math.max(minOffset, maxOffset - safeLimit * (long) PULL_BATCH_SIZE);

        while (nextOffset < maxOffset && deadLetters.size() < safeLimit) {
            PullResult pullResult = pullConsumer.pull(
                    queue,
                    "*",
                    nextOffset,
                    Math.min(PULL_BATCH_SIZE, safeLimit - deadLetters.size())
            );
            if (pullResult == null) {
                break;
            }

            PullStatus pullStatus = pullResult.getPullStatus();
            nextOffset = pullResult.getNextBeginOffset();
            if (pullStatus == PullStatus.FOUND && pullResult.getMsgFoundList() != null) {
                for (MessageExt messageExt : pullResult.getMsgFoundList()) {
                    deadLetters.add(convertMessage(queue, messageExt));
                    if (deadLetters.size() >= safeLimit) {
                        break;
                    }
                }
                continue;
            }
            if (pullStatus == PullStatus.NO_NEW_MSG
                    || pullStatus == PullStatus.NO_MATCHED_MSG
                    || pullStatus == PullStatus.OFFSET_ILLEGAL) {
                break;
            }
        }
    }

    /**
     * 把 RocketMQ 死信消息转换成接口返回对象。
     *
     * @param queue 死信队列
     * @param messageExt 原始 RocketMQ 消息
     * @return 死信消息视图
     */
    private SeckillOrderDeadLetterVO convertMessage(MessageQueue queue, MessageExt messageExt) {
        SeckillOrderDeadLetterVO deadLetter = new SeckillOrderDeadLetterVO();
        deadLetter.setTopic(messageExt.getTopic());
        deadLetter.setMsgId(messageExt.getMsgId());
        deadLetter.setBrokerName(queue.getBrokerName());
        deadLetter.setQueueId(queue.getQueueId());
        deadLetter.setQueueOffset(messageExt.getQueueOffset());
        deadLetter.setReconsumeTimes(messageExt.getReconsumeTimes());
        deadLetter.setKeys(messageExt.getKeys());
        deadLetter.setTags(messageExt.getTags());
        deadLetter.setBornTimestamp(messageExt.getBornTimestamp());
        deadLetter.setStoreTimestamp(messageExt.getStoreTimestamp());

        String rawBody = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        deadLetter.setRawBody(rawBody);
        try {
            deadLetter.setJsonBody(objectMapper.readValue(rawBody, Object.class));
        } catch (Exception e) {
            log.warn("解析死信 JSON 消息体失败，msgId={}", messageExt.getMsgId(), e);
        }
        try {
            deadLetter.setOrderMessage(objectMapper.readValue(rawBody, SeckillOrderMessage.class));
        } catch (Exception e) {
            // 死信查看的核心是把原始消息展示出来，JSON 解析失败时不影响主结果返回。
            log.warn("解析死信消息体失败，msgId={}", messageExt.getMsgId(), e);
        }
        return deadLetter;
    }

    /**
     * 归一化查看条数，避免查询接口一次拉太多消息。
     *
     * @param limit 原始条数
     * @return 安全条数
     */
    private int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) {
            return 20;
        }
        return Math.min(limit, MAX_LIMIT);
    }

    /**
     * 归一化消费组，空值时回退到正式秒杀订单消费组。
     *
     * @param consumerGroup 原始消费组
     * @return 生效消费组
     */
    private String normalizeConsumerGroup(String consumerGroup) {
        if (consumerGroup == null || consumerGroup.isBlank()) {
            return seckillOrderConsumerGroup;
        }
        return consumerGroup.trim();
    }

    /**
     * 为死信查看器生成独立消费组，避免与正式消费者的消费位点互相影响。
     *
     * @return 查看器消费组名
     */
    private String buildInspectorGroupName() {
        return "kpdp-seckill-dlq-inspector-" + System.nanoTime();
    }
}
