package com.kpdp.service.impl;

import cn.hutool.core.lang.UUID;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.kpdp.dto.LoginFormDTO;
import com.kpdp.dto.Result;
import com.kpdp.entity.User;
import com.kpdp.mapper.UserMapper;
import com.kpdp.service.IUserService;
import com.kpdp.utils.SnowflakeIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.kpdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.kpdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * 用户服务实现。
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    /**
     * 中国大陆手机号正则。
     */
    private static final String PHONE_REGEX =
            "^1([38][0-9]|4[579]|5[0-3,5-9]|6[6]|7[0135678]|9[89])\\d{8}$";

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result login(LoginFormDTO loginForm) {
        if (loginForm == null || !isPhoneValid(loginForm.getPhone())) {
            return Result.fail("手机号格式错误");
        }

        String phone = loginForm.getPhone();
        User user = query().eq("phone", phone).one();
        if (user == null) {
            user = getOrCreateUser(phone);
        }

        String token = UUID.randomUUID().toString(true);
        Map<String, Object> userMap = new HashMap<>(1);
        // 登录态中只保存用户 ID，避免把无关资料带入秒杀链路。
        userMap.put("id", user.getId().toString());

        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        log.info("用户登录成功，userId={}", user.getId());
        return Result.ok(token);
    }

    /**
     * 手机号首次出现时自动创建最小用户记录。
     *
     * @param phone 手机号
     * @return 新用户
     */
    private User createUser(String phone) {
        User user = new User();
        // 分片后的用户表不能再依赖数据库自增主键，统一使用雪花算法生成用户 ID。
        user.setId(snowflakeIdWorker.nextId());
        user.setPhone(phone);
        save(user);
        return user;
    }

    /**
     * 用户表按用户 ID 分片后，手机号查询会走广播路由。
     * 创建新用户前额外加一层手机号锁，避免并发自动注册时产生重复手机号。
     *
     * @param phone 手机号
     * @return 已存在或新创建的用户
     */
    private User getOrCreateUser(String phone) {
        RLock phoneLock = redissonClient.getLock("lock:user:phone:" + phone);
        phoneLock.lock();
        try {
            User existingUser = query().eq("phone", phone).one();
            if (existingUser != null) {
                return existingUser;
            }
            return createUser(phone);
        } finally {
            phoneLock.unlock();
        }
    }

    /**
     * 只保留一个基础手机号校验，避免继续保留额外工具类。
     *
     * @param phone 手机号
     * @return 是否合法
     */
    private boolean isPhoneValid(String phone) {
        return phone != null && phone.matches(PHONE_REGEX);
    }
}
