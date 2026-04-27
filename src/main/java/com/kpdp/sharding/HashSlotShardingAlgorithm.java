package com.kpdp.sharding;

import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;

/**
 * 基于“哈希取模 -> 总槽位 -> 库表映射”的标准分片算法。
 *
 * <p>这个算法专门用于 4 库 8 表这类“先总分片，再映射到库表”的场景，
 * 避免单独使用 dbCount/tableCount 取模时只命中部分物理节点。</p>
 */
public class HashSlotShardingAlgorithm implements StandardShardingAlgorithm<Comparable<?>> {

    /**
     * 总分片槽位数，例如 4 库 8 表时传 8。
     */
    private static final String TOTAL_SHARDING_COUNT_KEY = "total-sharding-count";

    /**
     * 每个数据库内的分表数量，例如每库 2 张表时传 2。
     */
    private static final String TABLE_SHARDING_COUNT_PER_DATABASE_KEY = "table-sharding-count-per-database";

    /**
     * 当前算法是用于路由数据库还是路由数据表。
     */
    private static final String ROUTE_TYPE_KEY = "route-type";

    private int totalShardingCount;

    private int tableShardingCountPerDatabase;

    private RouteType routeType;

    @Override
    public void init(Properties props) {
        totalShardingCount = parsePositiveInt(props, TOTAL_SHARDING_COUNT_KEY);
        tableShardingCountPerDatabase = parsePositiveInt(props, TABLE_SHARDING_COUNT_PER_DATABASE_KEY);
        if (totalShardingCount % tableShardingCountPerDatabase != 0) {
            throw new IllegalArgumentException("total-sharding-count 必须能被 table-sharding-count-per-database 整除");
        }
        String routeTypeValue = props.getProperty(ROUTE_TYPE_KEY);
        if (routeTypeValue == null || routeTypeValue.isBlank()) {
            throw new IllegalArgumentException("route-type 不能为空");
        }
        routeType = RouteType.valueOf(routeTypeValue.trim().toUpperCase());
    }

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Comparable<?>> shardingValue) {
        Comparable<?> shardingColumnValue = Objects.requireNonNull(shardingValue.getValue(), "分片值不能为空");
        int totalSlot = Math.floorMod(shardingColumnValue.hashCode(), totalShardingCount);
        int suffix = RouteType.DATABASE == routeType
                ? totalSlot / tableShardingCountPerDatabase
                : totalSlot % tableShardingCountPerDatabase;
        return matchTargetName(availableTargetNames, suffix);
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, RangeShardingValue<Comparable<?>> shardingValue) {
        // 范围查询无法精准命中单一分片时，保守返回全部目标节点。
        return availableTargetNames;
    }

    @Override
    public String getType() {
        return "HASH_SLOT";
    }

    /**
     * 从候选节点中匹配带指定后缀的真实数据源或真实表名。
     *
     * @param availableTargetNames 当前可选的目标节点
     * @param suffix 目标后缀
     * @return 精准匹配到的目标节点
     */
    private String matchTargetName(Collection<String> availableTargetNames, int suffix) {
        String expectedSuffix = "_" + suffix;
        return availableTargetNames.stream()
                .filter(each -> each.endsWith(expectedSuffix))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("未找到匹配的分片目标，suffix=" + suffix));
    }

    /**
     * 读取并校验正整数配置。
     *
     * @param props 配置集合
     * @param key 配置键
     * @return 正整数配置值
     */
    private int parsePositiveInt(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " 不能为空");
        }
        int result = Integer.parseInt(value.trim());
        if (result <= 0) {
            throw new IllegalArgumentException(key + " 必须大于 0");
        }
        return result;
    }

    /**
     * 算法当前路由目标类型。
     */
    private enum RouteType {
        DATABASE,
        TABLE
    }
}
