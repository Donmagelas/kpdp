package com.kpdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Instant;
import java.util.Enumeration;

/**
 * 基于雪花算法的全局唯一 ID 生成器。
 */
@Slf4j
@Component
public class SnowflakeIdWorker {

    /**
     * 自定义起始时间戳，使用 2024-01-01 00:00:00 UTC。
     */
    private static final long START_TIMESTAMP = 1704067200000L;

    /**
     * 机器标识位数。
     */
    private static final long WORKER_ID_BITS = 5L;

    /**
     * 机房标识位数。
     */
    private static final long DATACENTER_ID_BITS = 5L;

    /**
     * 同毫秒内序列号位数。
     */
    private static final long SEQUENCE_BITS = 12L;

    /**
     * 机器标识最大值。
     */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /**
     * 机房标识最大值。
     */
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);

    /**
     * 序列号掩码，用于同毫秒内循环递增。
     */
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    /**
     * 机器标识左移位数。
     */
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;

    /**
     * 机房标识左移位数。
     */
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    /**
     * 时间戳左移位数。
     */
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    /**
     * 当前实例的机器标识。
     */
    private final long workerId;

    /**
     * 当前实例的机房标识。
     */
    private final long datacenterId;

    /**
     * 同一毫秒内的序列号。
     */
    private long sequence = 0L;

    /**
     * 上一次发号的时间戳。
     */
    private long lastTimestamp = -1L;

    /**
     * 参与自动计算 workerId 的本机 IP。
     */
    private final String localIp;

    public SnowflakeIdWorker(
            @Value("${kpdp.snowflake.worker-id:-1}") long configuredWorkerId,
            @Value("${kpdp.snowflake.datacenter-id:1}") long datacenterId,
            @Value("${server.port:8081}") int serverPort
    ) {
        if (datacenterId < 0 || datacenterId > MAX_DATACENTER_ID) {
            throw new IllegalArgumentException("datacenterId 超出允许范围，必须在 0 到 31 之间");
        }
        this.localIp = resolveLocalIp();
        this.workerId = resolveWorkerId(configuredWorkerId, serverPort);
        this.datacenterId = datacenterId;
        log.info(
                "雪花算法发号器初始化完成，ip={}, port={}, workerId={}, datacenterId={}",
                localIp,
                serverPort,
                this.workerId,
                this.datacenterId
        );
    }

    /**
     * 基于雪花算法生成全局唯一 ID。
     * @return 全局唯一 ID
     */
    public synchronized long nextId() {
        long timestamp = currentTimestamp();
        if (timestamp < lastTimestamp) {
            throw new IllegalStateException("系统时钟回拨，拒绝生成订单号");
        }

        if (timestamp == lastTimestamp) {
            // 同一毫秒内通过递增序列号保证唯一性。
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0L) {
                // 当前毫秒的序列号耗尽后，阻塞到下一毫秒再继续发号。
                timestamp = waitUntilNextMillis(lastTimestamp);
            }
        } else {
            // 新毫秒重新从 0 开始计数，减少序列号无意义增长。
            sequence = 0L;
        }

        lastTimestamp = timestamp;
        long timestampDelta = timestamp - START_TIMESTAMP;
        return (timestampDelta << TIMESTAMP_LEFT_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /**
     * 兼容当前业务层调用习惯，保留业务前缀参数但不再参与发号。
     *
     * @param keyPrefix 业务前缀，当前仅用于保留方法兼容性
     * @return 全局唯一 ID
     */
    public long nextId(String keyPrefix) {
        return nextId();
    }

    /**
     * 获取当前时间戳，单位毫秒。
     *
     * @return 当前时间戳
     */
    private long currentTimestamp() {
        return Instant.now().toEpochMilli();
    }

    /**
     * 解析本机 IP，用于在未显式配置 workerId 时稳定计算机器码。
     *
     * @return 本机 IPv4 地址，解析失败时返回回环地址
     */
    private String resolveLocalIp() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces != null && networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                if (!networkInterface.isUp() || networkInterface.isLoopback() || networkInterface.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (inetAddress instanceof Inet4Address && !inetAddress.isLoopbackAddress()) {
                        return inetAddress.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            log.warn("遍历网卡获取本机 IP 失败，将尝试使用 InetAddress 回退", e);
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            log.warn("获取本机 IP 失败，回退到 127.0.0.1", e);
            return "127.0.0.1";
        }
    }

    /**
     * 解析最终使用的 workerId。
     *
     * <p>如果显式配置了 workerId，则直接使用配置值；否则使用“IP + 端口”的哈希结果稳定计算。</p>
     *
     * @param configuredWorkerId 显式配置的 workerId，负数表示未配置
     * @param serverPort 当前应用端口
     * @return 最终生效的 workerId
     */
    private long resolveWorkerId(long configuredWorkerId, int serverPort) {
        if (configuredWorkerId >= 0) {
            if (configuredWorkerId > MAX_WORKER_ID) {
                throw new IllegalArgumentException("workerId 超出允许范围，必须在 0 到 31 之间");
            }
            return configuredWorkerId;
        }

        // 使用“IP + 端口”计算稳定机器码，保证同一实例重启后生成结果不变。
        String workerSource = localIp + ":" + serverPort;
        return Math.floorMod(workerSource.hashCode(), (int) MAX_WORKER_ID + 1);
    }

    /**
     * 等待直到进入下一毫秒，避免同毫秒序列号溢出。
     *
     * @param lastTimestamp 上一次发号时间戳
     * @return 下一毫秒时间戳
     */
    private long waitUntilNextMillis(long lastTimestamp) {
        long timestamp = currentTimestamp();
        while (timestamp <= lastTimestamp) {
            timestamp = currentTimestamp();
        }
        return timestamp;
    }
}
