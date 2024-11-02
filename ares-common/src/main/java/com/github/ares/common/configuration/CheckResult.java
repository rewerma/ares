package com.github.ares.common.configuration;

import lombok.Data;

@Data
public class CheckResult {

    private static final CheckResult SUCCESS = new CheckResult(true, "");

    private boolean isSuccess;

    private String msg;

    private CheckResult(boolean isSuccess, String msg) {
        this.isSuccess = isSuccess;
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
