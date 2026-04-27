package com.kpdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.kpdp.entity.OrderOutbox;
import com.kpdp.mapper.OrderOutboxMapper;
import com.kpdp.service.IOrderOutboxService;
import org.springframework.stereotype.Service;

/**
 * 订单消息 Outbox 服务实现。
 */
@Service
public class OrderOutboxServiceImpl extends ServiceImpl<OrderOutboxMapper, OrderOutbox>
        implements IOrderOutboxService {
}
