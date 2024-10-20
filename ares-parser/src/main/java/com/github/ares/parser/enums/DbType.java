package com.github.ares.parser.enums;

import java.util.HashMap;
import java.util.Map;

public enum DbType {
    /**
     * MYSQL
     */
    MYSQL("mysql"),
    /**
     * ORACLE
     */
    ORACLE("oracle"),
    /**
     * PGSQL
     */
    PGSQL("postgresql"),
    /**
     * DAMENG
     */
    DAMENG("dm"),
    /**
     * OCEANBASE
     */
    OCEANBASE("oceanbase");

    private static final String JDBC_PREFIX = "jdbc:";

    private static final Map<String, DbType> TYPES = new HashMap<>();

    static {
        TYPES.put(MYSQL.type, MYSQL);
        TYPES.put(ORACLE.type, ORACLE);
        TYPES.put(PGSQL.type, PGSQL);
        TYPES.put(DAMENG.type, DAMENG);
        TYPES.put(OCEANBASE.type, OCEANBASE);
    }

    private final String type;

    DbType(String type) {
        this.type = type;
    }

    public static DbType getDbType(String type) {
        return TYPES.get(type);
    }

    public String getType() {
        return type;
    }

    public static DbType getDbTypeFromUrl(String jdbcUrl) {
        if (jdbcUrl.startsWith(JDBC_PREFIX + MYSQL.type)) {
            return MYSQL;
        }
        if (jdbcUrl.startsWith(JDBC_PREFIX + ORACLE.type)) {
            return ORACLE;
        }
        if (jdbcUrl.startsWith(JDBC_PREFIX + PGSQL.type)) {
            return PGSQL;
        }
        if (jdbcUrl.startsWith(JDBC_PREFIX + DAMENG.type)) {
            return DAMENG;
        }
        if (jdbcUrl.startsWith(JDBC_PREFIX + OCEANBASE.type)) {
            return OCEANBASE;
        }
        throw new IllegalArgumentException("Unsupported jdbc type: " + jdbcUrl);
    }
}
