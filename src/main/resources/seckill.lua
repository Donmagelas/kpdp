-- 1. 参数列表
-- 1.1 秒杀券 ID
local voucherId = ARGV[1]
-- 1.2 用户 ID
local userId = ARGV[2]

-- 2. 业务 Key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 3. 先取库存值，避免库存 key 未预热时直接做数字比较导致脚本报错。
local stock = redis.call('get', stockKey)
if (not stock) then
    return 3
end

-- 4. 校验库存
if (tonumber(stock) <= 0) then
    return 1
end

-- 5. 校验一人一单
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 6. 仅在 Redis 中完成资格预占，不再直接写 Redis Stream。
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
return 0
