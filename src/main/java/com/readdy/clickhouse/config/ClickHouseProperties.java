package com.readdy.clickhouse.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author readdy
 * @Description: 配置类
 * @date 2025/11/13
 */
@ConfigurationProperties(prefix = "spring.clickhouse")
public class ClickHouseProperties {
    private String url = "jdbc:clickhouse:http://localhost:8123/default";
    private String username = "default";
    private String password = "";
    private int maxPoolSize = 10;
    private boolean useCompression = true;

    /** 异步插入开关（URL 参数） */
    private boolean asyncInsert = true;

    /** TTL 配置 */
    private boolean enableTtl = true;
    private String ttlInterval = "1 MONTH";

    /** 批量插入建议大小 */
    private int batchSize = 1000;
    private Integer waitForAsyncInsert = 1;

    // ---------- getters & setters ----------
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public int getMaxPoolSize() { return maxPoolSize; }
    public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }

    public boolean isUseCompression() { return useCompression; }
    public void setUseCompression(boolean useCompression) { this.useCompression = useCompression; }

    public boolean isAsyncInsert() { return asyncInsert; }
    public void setAsyncInsert(boolean asyncInsert) { this.asyncInsert = asyncInsert; }

    public boolean isEnableTtl() { return enableTtl; }
    public void setEnableTtl(boolean enableTtl) { this.enableTtl = enableTtl; }

    public String getTtlInterval() { return ttlInterval; }
    public void setTtlInterval(String ttlInterval) { this.ttlInterval = ttlInterval; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public Integer getWaitForAsyncInsert() {
        return waitForAsyncInsert;
    }

    public void setWaitForAsyncInsert(Integer waitForAsyncInsert) {
        this.waitForAsyncInsert = waitForAsyncInsert;
    }
}