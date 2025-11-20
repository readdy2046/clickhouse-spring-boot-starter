package com.readdy.clickhouse.template;

import com.readdy.clickhouse.annotation.TableName;
import com.readdy.clickhouse.config.ClickHouseProperties;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author readdy
 * @description: TODO
 * @date 2025/11/14
 */
public class ClickHouseTemplate extends JdbcTemplate {
    private final ClickHouseProperties properties;

    public ClickHouseTemplate(JdbcTemplate jdbcTemplate, ClickHouseProperties properties) {
        super(jdbcTemplate.getDataSource());  // 复用 DataSource
        this.properties = properties;
    }

    // 一行插入实体
    public  <T> void insert(T entity) {
        InsertStatement stmt = buildInsertStatement(entity);
        execute(stmt.sql, (PreparedStatementCallback<Integer>) ps -> {
            stmt.setter.setValues(ps, entity);
            return ps.executeUpdate();
        });
    }

    // 一行批量插入
    public <T> void insertBatch(List<T> entities) {
        if (entities.isEmpty()) {return;}
        InsertStatement stmt = buildInsertStatement(entities.get(0));
        batchUpdate(stmt.sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                stmt.setter.setValues(ps, entities.get(i));
            }
            @Override
            public int getBatchSize() {
                return entities.size();
            }
        });
    }

    // 自动构建 INSERT SQL 和 Setter
    private <T> InsertStatement buildInsertStatement(T entity) {
        Class<?> clazz = entity.getClass();
        TableName annotation = clazz.getAnnotation(TableName.class);
        String tableName = annotation.value();
        if (tableName == null) {
            tableName = "t_" + toSnakeCase(clazz.getSimpleName().replace("Data", "")) + "_data"; // GunRealData → t_gun_real
        }

        List<String> columns = new ArrayList<>();
        List<PropertySetter> setters = new ArrayList<>();

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
            for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
                if ("class".equals(pd.getName())) {continue;}
                String column = toSnakeCase(pd.getName());
                columns.add(column);
                setters.add(new PropertySetter(pd, entity));
            }
        } catch (Exception e) {
            throw new RuntimeException("Build insert statement failed", e);
        }

        String sql = "INSERT INTO " + tableName + " (" +
                String.join(", ", columns) + ") VALUES (" +
                columns.stream().map(c -> "?").collect(Collectors.joining(", ")) + ")";

        return new InsertStatement(sql, new EntitySetter(setters));
    }

    // 工具：驼峰 → 下划线
    private String toSnakeCase(String name) {
//        return name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
        // 先处理 “前面是小写或数字，后面是大写字母开头的单词”
        String s1 = name.replaceAll("([a-z0-9])([A-Z])", "$1_$2");
        // 再处理 “前面是大写字母，后面是大写字母+小写字母的情况” (连续大写)
        String s2 = s1.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2");
        return s2.toLowerCase();
    }

    // 内部类：SQL + Setter
    private static class InsertStatement {
        String sql;
        EntitySetter setter;
        InsertStatement(String sql, EntitySetter setter) {
            this.sql = sql;
            this.setter = setter;
        }
    }

    // 内部类：属性 setter
    private static class PropertySetter {
        PropertyDescriptor pd;
        Object entity;
        PropertySetter(PropertyDescriptor pd, Object entity) {
            this.pd = pd;
            this.entity = entity;
        }
        Object getValue() throws Exception {
            Object value = pd.getReadMethod().invoke(entity);
            if (value instanceof LocalDateTime) {
                return Timestamp.valueOf((LocalDateTime) value);
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? 1 : 0;
            }
            return value;
        }
    }

    // 内部类：批量 setter
    private static class EntitySetter {
        List<PropertySetter> setters;
        EntitySetter(List<PropertySetter> setters) { this.setters = setters; }
        void setValues(PreparedStatement ps, Object entity) throws SQLException {
            try {
                for (int i = 0; i < setters.size(); i++) {
                    PropertySetter setter = setters.get(i);
                    setter.entity = entity; // 切换实体
                    ps.setObject(i + 1, setter.getValue());
                }
            } catch (Exception e) {
                throw new SQLException("Set values failed", e);
            }
        }
    }
}
