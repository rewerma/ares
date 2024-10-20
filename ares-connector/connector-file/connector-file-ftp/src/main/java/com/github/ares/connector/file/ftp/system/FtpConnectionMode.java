package com.github.ares.connector.file.ftp.system;

/** Ftp connection mode enum. href="http://commons.apache.org/net/">Apache Commons Net</a>. */
public enum FtpConnectionMode {

    /** ACTIVE_LOCAL_DATA_CONNECTION_MODE */
    ACTIVE_LOCAL_DATA_CONNECTION_MODE("active_local"),

    /** PASSIVE_LOCAL_DATA_CONNECTION_MODE */
    PASSIVE_LOCAL_DATA_CONNECTION_MODE("passive_local");

    private final String mode;

    FtpConnectionMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static FtpConnectionMode fromMode(String mode) {
        for (FtpConnectionMode ftpConnectionModeEnum : FtpConnectionMode.values()) {
            if (ftpConnectionModeEnum.getMode().equals(mode)) {
                return ftpConnectionModeEnum;
            }
        }
        throw new IllegalArgumentException("Unknown ftp connection mode: " + mode);
    }
}
