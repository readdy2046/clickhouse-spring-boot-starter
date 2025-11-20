package com.readdy.clickhouse.annotation;

import java.lang.annotation.*;

/**
 * @author readdy
 * @description: 表名
 * @date 2025/11/14
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableName {
    String value() default "";
}
