package com.github.ares.engine.core;

import java.io.Serializable;

public abstract class ExceptionMessageHandler implements Serializable {
    private static final long serialVersionUID = 1L;

    public String getMessage(Exception e) {
        if (e == null) {
            return null;
        }
        return e.getMessage();
    }
}
