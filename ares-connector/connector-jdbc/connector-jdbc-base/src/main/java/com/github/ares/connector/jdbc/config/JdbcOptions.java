package com.github.ares.connector.jdbc.config;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import java.math.BigDecimal;
import java.util.Map;

@SuppressWarnings("checkstyle:MagicNumber")
public interface JdbcOptions {

    Option<String> URL = Options.key("url").stringType().noDefaultValue().withDescription("url");

    Option<String> DRIVER =
            Options.key("driver").stringType().noDefaultValue().withDescription("driver");

    Option<Integer> CONNECTION_CHECK_TIMEOUT_SEC =
            Options.key("connection_check_timeout_sec")
                    .intType()
                    .defaultValue(30)
                    .withDescription("connection check time second");

    Option<Boolean> CONNECTION_POOL_ENABLED =
            Options.key("connection_pool_enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("whether to use HikariCP connection pool");

    Option<Integer> POOL_SIZE =
            Options.key("pool_size")
                    .intType()
                    .defaultValue(8)
                    .withDescription("maximum JDBC connection pool size per datasource");

    Option<Integer> QUERY_TIMEOUT_SEC =
            Options.key("query_timeout_sec")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Query timeout in seconds for MySQL-compatible analytics databases "
                                    + "(StarRocks/Doris/SelectDB). Applied via SET query_timeout after connection. "
                                    + "Use -1 to disable.");
    Option<String> COMPATIBLE_MODE =
            Options.key("compatible_mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The compatible mode of database, required when the database supports multiple compatible modes. For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.");

    Option<Integer> MAX_RETRIES =
            Options.key("max_retries").intType().defaultValue(0).withDescription("max_retired");

    Option<String> USER = Options.key("user").stringType().noDefaultValue().withDescription("user");

    Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("query");

    Option<Boolean> AUTO_COMMIT =
            Options.key("auto_commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("auto commit");

    Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(1000).withDescription("batch size");

    Option<Integer> FETCH_SIZE =
            Options.key("fetch_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "For queries that return a large number of objects, "
                                    + "you can configure the row fetch size used in the query to improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.");

    Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    Option<String> TABLE_NAME =
            Options.key("table_name").stringType().noDefaultValue().withDescription("target table");

    /** source config */
    Option<String> PARTITION_COLUMN =
            Options.key("partition_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("partition column");

    Option<BigDecimal> PARTITION_UPPER_BOUND =
            Options.key("partition_upper_bound")
                    .bigDecimalType()
                    .noDefaultValue()
                    .withDescription("partition upper bound");
    Option<BigDecimal> PARTITION_LOWER_BOUND =
            Options.key("partition_lower_bound")
                    .bigDecimalType()
                    .noDefaultValue()
                    .withDescription("partition lower bound");
    Option<Integer> PARTITION_NUM =
            Options.key("partition_num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("partition num");

    Option<FieldIdeEnum> FIELD_IDE =
            Options.key("field_ide")
                    .enumType(FieldIdeEnum.class)
                    .noDefaultValue()
                    .withDescription("Whether case conversion is required");

    Option<Map<String, String>> PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("additional connection configuration parameters");
}
