-- 1. 限流 key
local limitKey = KEYS[1]

-- 2. 滑动窗口大小，单位毫秒
local windowMillis = tonumber(ARGV[1])

-- 3. 窗口内允许的最大请求数
local limit = tonumber(ARGV[2])

-- 4. 当前请求时间戳，单位毫秒
local nowMillis = tonumber(ARGV[3])

-- 5. 当前请求的唯一成员值，避免同毫秒请求发生覆盖
local requestMember = ARGV[4]

-- 6. 移除滑动窗口之外的旧请求记录
redis.call('zremrangebyscore', limitKey, 0, nowMillis - windowMillis)

-- 7. 获取当前滑动窗口内的请求数
local currentCount = redis.call('zcard', limitKey)

-- 8. 如果窗口内请求数已经达到阈值，则本次请求直接限流
if currentCount >= limit then
    return 0
end

-- 9. 把本次请求加入滑动窗口，score 使用当前时间戳
local member = tostring(nowMillis) .. '-' .. requestMember
redis.call('zadd', limitKey, nowMillis, member)

-- 10. 更新过期时间，避免冷数据长期残留
redis.call('pexpire', limitKey, windowMillis)

-- 11. 返回 1，表示本次请求放行
return 1
