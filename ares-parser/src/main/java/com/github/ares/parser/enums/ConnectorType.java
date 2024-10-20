package com.github.ares.parser.enums;

import java.util.HashMap;
import java.util.Map;

public enum ConnectorType {
    /**
     * JDBC
     */
    JDBC("jdbc"),
    /**
     * T3
     */
    T3("t3"),
    /**
     * FTP
     */
    FTP("ftp"),
    /**
     * SFTP
     */
    SFTP("sftp"),
    /**
     * HIVE
     */
    HIVE("hive"),
    ;

    private static final Map<String, ConnectorType> TYPES = new HashMap<>();

    static {
        TYPES.put(JDBC.type, JDBC);
        TYPES.put(T3.type, T3);
        TYPES.put(FTP.type, FTP);
        TYPES.put(SFTP.type, SFTP);
        TYPES.put(HIVE.type, HIVE);
    }

    private final String type;

    ConnectorType(String type) {
        this.type = type;
    }

    public static ConnectorType getType(String type) {
        return TYPES.get(type);
    }

    public String getType() {
        return type;
    }
}
