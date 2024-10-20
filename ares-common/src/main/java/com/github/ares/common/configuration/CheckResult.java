package com.github.ares.common.configuration;

import lombok.Data;

@Data
public class CheckResult {

    private static final CheckResult SUCCESS = new CheckResult(true, "");

    private boolean success;

    private String msg;

    private CheckResult(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }

    /** @return a successful instance of CheckResult */
    public static CheckResult success() {
        return SUCCESS;
    }

    /**
     * @param msg the error message
     * @return an error instance of CheckResult
     */
    public static CheckResult error(String msg) {
        return new CheckResult(false, msg);
    }
}
