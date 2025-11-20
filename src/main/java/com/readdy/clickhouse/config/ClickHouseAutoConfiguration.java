package com.readdy.clickhouse.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.readdy.clickhouse.template.ClickHouseTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author readdy
 * @description: 自动配置类
 * @date 2025/11/13
 */
@Configuration
@ConditionalOnProperty(prefix = "spring.clickhouse", name = "url")
@ConditionalOnClass(ClickHouseDataSource.class)
@EnableConfigurationProperties(ClickHouseProperties.class)
public class ClickHouseAutoConfiguration {
    private final ClickHouseProperties properties;

    public ClickHouseAutoConfiguration(ClickHouseProperties properties) {
        this.properties = properties;
    }

    @Bean(name = "clickHouseDataSource")
    @ConditionalOnMissingBean(name = "clickHouseDataSource")
    public DataSource clickHouseDataSource() throws SQLException {
        String finalUrl = buildUrlWithAsync();
        Properties props = new Properties();
        props.setProperty("user", properties.getUsername());
        props.setProperty("password", properties.getPassword());
        if (properties.isUseCompression()) {
            props.setProperty("compress", "0");
        }
        return new ClickHouseDataSource(finalUrl, props);
    }

    @Bean(name = "clickHouseJdbcTemplate")
    public JdbcTemplate clickHouseJdbcTemplate(@Qualifier("clickHouseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public ClickHouseTemplate clickHouseTemplate(@Qualifier("clickHouseJdbcTemplate") JdbcTemplate jdbcTemplate) {
        return new ClickHouseTemplate(jdbcTemplate, properties);
    }

    // 动态构建 URL，包含 async_insert 和 wait_for_async_insert
    private String buildUrlWithAsync() {
        String base = properties.getUrl();

        StringBuilder params = new StringBuilder();
        if (properties.isAsyncInsert()) {
            params.append("async_insert=1");
            // 读取 wait-for-async-insert 配置
            if (properties.getWaitForAsyncInsert() != null) {
                params.append("&wait_for_async_insert=").append(properties.getWaitForAsyncInsert());
            } else {
                params.append("&wait_for_async_insert=1"); // 默认等待
            }
        } else {
            params.append("async_insert=0");
        }

        // 拼接参数
        String paramStr = params.toString();
        return base.contains("?") ? base + "&" + paramStr : base + "?" + paramStr;
    }
}