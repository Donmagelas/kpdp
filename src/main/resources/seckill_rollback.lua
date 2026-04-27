-- 1. 参数列表
-- 1.1 秒杀券 ID
local voucherId = ARGV[1]
-- 1.2 用户 ID
local userId = ARGV[2]

-- 2. 业务 Key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 3. 仅在当前用户确实占过资格时执行回滚，保证脚本具备幂等性。
if (redis.call('sismember', orderKey, userId) == 1) then
    redis.call('srem', orderKey, userId)
    redis.call('incrby', stockKey, 1)
    return 1
end

return 0
